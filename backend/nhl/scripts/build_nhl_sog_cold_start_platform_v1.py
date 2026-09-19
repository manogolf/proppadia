#!/usr/bin/env python3
"""Build the deterministic, no-market NHL SOG cold-start evidence package."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg
from sklearn.metrics import roc_auc_score

from backend.nhl.sog_cold_start.core import CONTRACT_PATH, LINES, poisson_tail, sha256_file

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/nhl_sog_cold_start_prediction_platform_v1/2026-09-19"
VARIANTS = [
    "A_PRIOR_SEASON_CARRY_FORWARD",
    "B_PRIOR_SEASON_RECENCY_WEIGHTED",
    "C_MULTISEASON_SHRUNK_PLAYER",
    "D_PLAYER_ROLE_HIERARCHICAL",
    "E_TEAM_CHANGE_AWARE",
    "F_CURRENT_PRESEASON_UPDATE",
    "G_COLD_START_TO_CURRENT_SEASON_BLEND",
    "CONTROL_FROZEN_ZERO_DEFAULT",
    "CONTROL_POSITION_PRIOR",
]


def stable_json(value: Any) -> str:
    def clean(x: Any) -> Any:
        if isinstance(x, dict): return {str(k): clean(v) for k, v in x.items()}
        if isinstance(x, (list, tuple)): return [clean(v) for v in x]
        if isinstance(x, (np.integer,)): return int(x)
        if isinstance(x, (np.floating, float)): return None if not math.isfinite(float(x)) else float(x)
        if isinstance(x, (np.bool_,)): return bool(x)
        if x is pd.NA or (not isinstance(x, (str, bytes)) and pd.isna(x)): return None
        return x
    return json.dumps(clean(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(json.loads(stable_json(value)), indent=2, sort_keys=True) + "\n")


def fetch_snapshot(dsn: str, out: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not dsn: raise RuntimeError("SUPABASE_DB_URL_REQUIRED_FOR_READ_ONLY_SNAPSHOT")
    logs_sql = """
      SELECT g.season,g.game_date,g.start_time_utc,g.game_id,g.game_type,g.status,
             l.player_id,l.team_id,l.opponent_id,l.is_home,l.shots_on_goal,
             l.shot_attempts,l.toi_minutes,l.pp_toi_minutes,
             p.full_name AS player_name,p.position,p.current_team_id,p.active,
             l.created_at AS log_created_at
      FROM nhl.skater_game_logs_raw l
      JOIN nhl.games g USING(game_id)
      LEFT JOIN nhl.players p USING(player_id)
      WHERE g.season BETWEEN 2023 AND 2025 AND g.game_type IN (2,3)
      ORDER BY g.season,g.game_date,g.start_time_utc,g.game_id,l.player_id
    """
    games_sql = """
      SELECT season,game_date,start_time_utc,game_id,game_type,status,
             home_team_id,away_team_id,home_team_code,away_team_code
      FROM nhl.games WHERE season BETWEEN 2023 AND 2026
      ORDER BY season,start_time_utc,game_id
    """
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            cur.execute("BEGIN READ ONLY")
            logs = pd.read_sql_query(logs_sql, con)
            games = pd.read_sql_query(games_sql, con)
            con.rollback()
    logs.to_parquet(out / "retained_skater_history_snapshot.parquet", index=False)
    games.to_parquet(out / "retained_game_schedule_snapshot.parquet", index=False)
    return logs, games


def aggregate_profile(frame: pd.DataFrame) -> pd.DataFrame:
    grouped = frame.groupby(["season", "player_id"], as_index=False).agg(
        games=("game_id", "nunique"), shots=("shots_on_goal", "sum"),
        minutes=("toi_minutes", "sum"), toi_per_game=("toi_minutes", "mean"),
        last_game_date=("game_date", "max"), last_team_id=("team_id", "last"),
    )
    grouped["sog_per60"] = grouped.shots * 60 / grouped.minutes.replace(0, np.nan)
    return grouped


def position_priors(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.copy()
    x["position_group"] = np.where(x.position.astype(str).str.upper().eq("D"), "D", "F")
    g = x.groupby(["season", "position_group"], as_index=False).agg(
        shots=("shots_on_goal", "sum"), minutes=("toi_minutes", "sum"),
        games=("game_id", "size"), toi_per_game=("toi_minutes", "mean"),
    )
    g["sog_per60"] = g.shots * 60 / g.minutes.replace(0, np.nan)
    return g


def team_profiles(logs: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    team_for = logs.groupby(["season", "team_id", "game_id"], as_index=False).shots_on_goal.sum()
    for_pg = team_for.groupby(["season", "team_id"], as_index=False).shots_on_goal.mean().rename(columns={"shots_on_goal":"shots_for_pg"})
    against = team_for.rename(columns={"team_id":"opponent_id", "shots_on_goal":"shots_against"}).merge(
        logs[["season","game_id","team_id","opponent_id"]].drop_duplicates(),
        on=["season","game_id","opponent_id"], how="inner")
    against = against.groupby(["season","team_id"], as_index=False).shots_against.mean().rename(columns={"shots_against":"shots_allowed_pg"})
    return for_pg.merge(against, on=["season","team_id"], how="outer")


def team_prior_game_counts(games: pd.DataFrame) -> pd.DataFrame:
    g = games[games.game_type.eq(2)].copy()
    h = g[["season","game_id","start_time_utc","home_team_id"]].rename(columns={"home_team_id":"team_id"})
    a = g[["season","game_id","start_time_utc","away_team_id"]].rename(columns={"away_team_id":"team_id"})
    both = pd.concat([h,a], ignore_index=True).sort_values(["season","team_id","start_time_utc","game_id"])
    both["team_games_prior"] = both.groupby(["season","team_id"]).cumcount()
    return both[["season","game_id","team_id","team_games_prior"]]


def recency_profile(logs: pd.DataFrame) -> pd.DataFrame:
    x = logs.sort_values(["season","player_id","game_date","start_time_utc","game_id"]).groupby(["season","player_id"], group_keys=False).tail(20).copy()
    x["rank_newest"] = x.groupby(["season","player_id"]).cumcount(ascending=False)
    x["weight"] = np.power(0.9, x.rank_newest)
    x["wshots"] = x.shots_on_goal * x.weight
    x["wtoi"] = x.toi_minutes * x.weight
    g = x.groupby(["season","player_id"], as_index=False).agg(games=("game_id","size"), wshots=("wshots","sum"), wtoi=("wtoi","sum"), weight=("weight","sum"))
    g["sog_per60"] = g.wshots * 60 / g.wtoi.replace(0,np.nan)
    g["toi_per_game"] = g.wtoi / g.weight.replace(0,np.nan)
    return g


def player_class(row: pd.Series) -> str:
    if row.prior_minutes >= 300: base = "RETURNING_ADEQUATE_HISTORY"
    elif row.prior_minutes > 0: base = "RETURNING_SPARSE_HISTORY"
    elif row.older_minutes > 0: base = "RETURNING_AFTER_ABSENCE_OR_OLD_HISTORY"
    else: base = "ROOKIE_NO_NHL_HISTORY"
    if bool(row.team_changed): base += "_TEAM_CHANGE"
    return base


def build_replay(logs: pd.DataFrame, games: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logs = logs.copy()
    game_identity=games[["season","game_id","home_team_id","away_team_id"]].drop_duplicates(["season","game_id"])
    logs=logs.merge(game_identity,on=["season","game_id"],how="left",validate="many_to_one")
    logs["stored_team_id"]=logs.team_id
    logs["team_id"]=np.where(logs.is_home.astype(bool),logs.home_team_id,logs.away_team_id)
    logs["opponent_id"]=np.where(logs.is_home.astype(bool),logs.away_team_id,logs.home_team_id)
    for c in ["shots_on_goal","toi_minutes"]: logs[c] = pd.to_numeric(logs[c], errors="coerce")
    logs = logs.dropna(subset=["shots_on_goal","toi_minutes","player_id","team_id"]).copy()
    logs["game_date"] = pd.to_datetime(logs.game_date).dt.date.astype(str)
    logs["position_group"] = np.where(logs.position.astype(str).str.upper().eq("D"), "D", "F")
    profiles = aggregate_profile(logs)
    recency = recency_profile(logs)
    positions = position_priors(logs)
    teams = team_profiles(logs, games)
    counts = team_prior_game_counts(games)
    league_rate = {
        int(season): float(group.shots_on_goal.sum() * 60 / group.toi_minutes.sum())
        for season, group in logs.groupby("season")
    }
    league_toi = {int(season): float(group.toi_minutes.mean()) for season, group in logs.groupby("season")}

    target = logs[logs.season.isin([2024,2025]) & logs.game_type.eq(2)].copy()
    target = target.merge(counts, on=["season","game_id","team_id"], how="left", validate="many_to_one")
    target = target[target.team_games_prior.le(10)].copy()
    target = target.sort_values(["season","player_id","game_date","start_time_utc","game_id"])
    target["current_regular_games"] = target.groupby(["season","player_id"]).cumcount()
    target["current_shots_prior"] = target.groupby(["season","player_id"]).shots_on_goal.cumsum() - target.shots_on_goal
    target["current_minutes_prior"] = target.groupby(["season","player_id"]).toi_minutes.cumsum() - target.toi_minutes
    target["current_sog_per60"] = target.current_shots_prior * 60 / target.current_minutes_prior.replace(0,np.nan)
    target["current_toi_per_game"] = target.current_minutes_prior / target.current_regular_games.replace(0,np.nan)
    target["prior_season"] = target.season - 1
    target["older_season"] = target.season - 2

    p = profiles.rename(columns={"season":"prior_season","games":"prior_games","shots":"prior_shots","minutes":"prior_minutes","toi_per_game":"prior_toi","last_game_date":"prior_last_date","last_team_id":"prior_last_team","sog_per60":"prior_rate"})
    o = profiles.rename(columns={"season":"older_season","games":"older_games","shots":"older_shots","minutes":"older_minutes","toi_per_game":"older_toi","last_game_date":"older_last_date","last_team_id":"older_last_team","sog_per60":"older_rate"})
    r = recency.rename(columns={"season":"prior_season","games":"recent_games","sog_per60":"recent_rate","toi_per_game":"recent_toi"})[["prior_season","player_id","recent_games","recent_rate","recent_toi"]]
    pp = positions.rename(columns={"season":"prior_season","sog_per60":"position_rate","toi_per_game":"position_toi"})[["prior_season","position_group","position_rate","position_toi"]]
    target = target.merge(p, on=["prior_season","player_id"], how="left").merge(o,on=["older_season","player_id"],how="left").merge(r,on=["prior_season","player_id"],how="left").merge(pp,on=["prior_season","position_group"],how="left")
    for c in ["prior_minutes","older_minutes","prior_games","older_games"]: target[c] = target[c].fillna(0)
    target["team_changed"] = target.prior_last_team.notna() & target.team_id.ne(target.prior_last_team)
    target["player_class"] = target.apply(player_class, axis=1)
    tf = teams.rename(columns={"season":"prior_season","team_id":"team_id","shots_for_pg":"team_prior_for","shots_allowed_pg":"team_prior_allowed"})
    ta = teams.rename(columns={"season":"prior_season","team_id":"opponent_id","shots_for_pg":"opp_prior_for","shots_allowed_pg":"opp_prior_allowed"})
    target = target.merge(tf[["prior_season","team_id","team_prior_for","team_prior_allowed"]],on=["prior_season","team_id"],how="left").merge(ta[["prior_season","opponent_id","opp_prior_for","opp_prior_allowed"]],on=["prior_season","opponent_id"],how="left")
    league = teams.groupby("season").shots_for_pg.mean().to_dict()

    replay: list[dict[str,Any]] = []
    coverage: list[dict[str,Any]] = []
    for row in target.itertuples(index=False):
        league_shots = float(league.get(int(row.prior_season), np.nan))
        variants: dict[str,float|None] = {}
        variants["A_PRIOR_SEASON_CARRY_FORWARD"] = (row.prior_rate * row.prior_toi / 60) if pd.notna(row.prior_rate) and pd.notna(row.prior_toi) else None
        variants["B_PRIOR_SEASON_RECENCY_WEIGHTED"] = (row.recent_rate * row.recent_toi / 60) if pd.notna(row.recent_rate) and pd.notna(row.recent_toi) else None
        total_m = row.prior_minutes + .5 * row.older_minutes
        if total_m > 0:
            base_rate = ((0 if pd.isna(row.prior_rate) else row.prior_rate*row.prior_minutes) + (0 if pd.isna(row.older_rate) else .5*row.older_rate*row.older_minutes))/total_m
            base_toi = ((0 if pd.isna(row.prior_toi) else row.prior_toi*row.prior_minutes) + (0 if pd.isna(row.older_toi) else .5*row.older_toi*row.older_minutes))/total_m
            prior_league_rate = league_rate[int(row.prior_season)]
            prior_league_toi = league_toi[int(row.prior_season)]
            c_rate = (base_rate*total_m+prior_league_rate*300)/(total_m+300); c_toi=(base_toi*total_m+prior_league_toi*300)/(total_m+300)
            variants["C_MULTISEASON_SHRUNK_PLAYER"] = c_rate*c_toi/60
        else: variants["C_MULTISEASON_SHRUNK_PLAYER"] = None
        if total_m > 0 and pd.notna(row.position_rate) and pd.notna(row.position_toi):
            d_rate=(base_rate*total_m+row.position_rate*300)/(total_m+300); d_toi=(base_toi*total_m+row.position_toi*300)/(total_m+300)
            variants["D_PLAYER_ROLE_HIERARCHICAL"] = d_rate*d_toi/60
        elif pd.notna(row.position_rate) and pd.notna(row.position_toi):
            variants["D_PLAYER_ROLE_HIERARCHICAL"] = row.position_rate*row.position_toi/60
        else: variants["D_PLAYER_ROLE_HIERARCHICAL"] = None
        context = None
        if pd.notna(row.team_prior_for) and pd.notna(row.opp_prior_allowed) and league_shots > 0:
            context = math.sqrt(max(.5,min(1.5,row.team_prior_for/league_shots))*max(.5,min(1.5,row.opp_prior_allowed/league_shots)))
        variants["E_TEAM_CHANGE_AWARE"] = None if variants["D_PLAYER_ROLE_HIERARCHICAL"] is None else variants["D_PLAYER_ROLE_HIERARCHICAL"]*(context if context is not None else 1.0)
        variants["F_CURRENT_PRESEASON_UPDATE"] = None
        base = variants["D_PLAYER_ROLE_HIERARCHICAL"]
        if base is not None and row.current_regular_games > 0 and pd.notna(row.current_sog_per60) and pd.notna(row.current_toi_per_game):
            w=min(float(row.current_regular_games)/10,1); current=row.current_sog_per60*row.current_toi_per_game/60
            variants["G_COLD_START_TO_CURRENT_SEASON_BLEND"]=(1-w)*base+w*current
        else: variants["G_COLD_START_TO_CURRENT_SEASON_BLEND"]=base
        variants["CONTROL_FROZEN_ZERO_DEFAULT"]=0.0
        variants["CONTROL_POSITION_PRIOR"]=(row.position_rate*row.position_toi/60) if pd.notna(row.position_rate) and pd.notna(row.position_toi) else None
        identity={"season":int(row.season),"game_date":row.game_date,"game_id":int(row.game_id),"player_id":int(row.player_id),"team_id":int(row.team_id),"team_games_prior":int(row.team_games_prior),"current_player_games":int(row.current_regular_games),"player_class":row.player_class,"official_sog":float(row.shots_on_goal)}
        for variant, lam in variants.items():
            if lam is not None: exclusion = ""
            elif variant.startswith("F_"): exclusion = "NO_RETAINED_PRESEASON_HISTORY"
            else: exclusion = "INSUFFICIENT_STRICT_PRIOR_HISTORY"
            coverage.append({**identity,"variant":variant,"scoreable":lam is not None,"exclusion_reason":exclusion})
            if lam is None: continue
            for line in LINES:
                p=poisson_tail(max(0,float(lam)),line); y=float(row.shots_on_goal)>line
                replay.append({**identity,"variant":variant,"line":line,"expected_sog":float(lam),"p_over":p,"actual_over":int(y),"selected_side":"OVER" if p>=.5 else "UNDER","correct":int((p>=.5)==y)})
    return pd.DataFrame(replay),pd.DataFrame(coverage),target


def metric_row(group: pd.DataFrame, label: dict[str,Any]) -> dict[str,Any]:
    if group.empty: return {**label,"rows":0}
    p=group.p_over.clip(1e-12,1-1e-12); y=group.actual_over.astype(float)
    brier=float(np.mean((p-y)**2)); logloss=float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))
    bins=pd.cut(p,bins=np.linspace(0,1,11),include_lowest=True)
    ece=0.0
    for _,x in group.assign(_p=p,_y=y,_b=bins).groupby("_b",observed=True): ece += len(x)/len(group)*abs(x._p.mean()-x._y.mean())
    auc=float(roc_auc_score(y,p)) if y.nunique()>1 else None
    daily=group.assign(_loss=(p-y)**2).groupby("game_date").agg(acc=("correct","mean"),brier=("_loss","mean"))
    acc_se=float(daily.acc.std(ddof=1)/math.sqrt(len(daily))) if len(daily)>1 else None
    return {**label,"rows":len(group),"player_games":group[["game_id","player_id"]].drop_duplicates().shape[0],"dates":group.game_date.nunique(),"accuracy":group.correct.mean(),"brier":brier,"log_loss":logloss,"ece_10":ece,"roc_auc":auc,"date_clustered_accuracy_se":acc_se,"accuracy_ci_low":None if acc_se is None else max(0,group.correct.mean()-1.96*acc_se),"accuracy_ci_high":None if acc_se is None else min(1,group.correct.mean()+1.96*acc_se)}


def metrics(replay: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for variant,g in replay.groupby("variant"): rows.append(metric_row(g,{"variant":variant,"scope":"ALL","scope_value":"ALL"}))
    for (variant,season),g in replay.groupby(["variant","season"]): rows.append(metric_row(g,{"variant":variant,"scope":"SEASON","scope_value":str(season)}))
    for (variant,line),g in replay.groupby(["variant","line"]): rows.append(metric_row(g,{"variant":variant,"scope":"LINE","scope_value":str(line)}))
    for (variant,depth),g in replay.groupby(["variant","team_games_prior"]): rows.append(metric_row(g,{"variant":variant,"scope":"TEAM_GAMES_PRIOR","scope_value":str(depth)}))
    for (variant,klass),g in replay.groupby(["variant","player_class"]): rows.append(metric_row(g,{"variant":variant,"scope":"PLAYER_CLASS","scope_value":str(klass)}))
    for (variant,team),g in replay.groupby(["variant","team_id"]): rows.append(metric_row(g,{"variant":variant,"scope":"TEAM","scope_value":str(team)}))
    common_variants={"A_PRIOR_SEASON_CARRY_FORWARD","B_PRIOR_SEASON_RECENCY_WEIGHTED","C_MULTISEASON_SHRUNK_PLAYER","D_PLAYER_ROLE_HIERARCHICAL","G_COLD_START_TO_CURRENT_SEASON_BLEND"}
    z=replay[replay.variant.isin(common_variants)].copy(); keys=["season","game_id","player_id","line"]
    common=z.groupby(keys).variant.nunique();common=common[common.eq(len(common_variants))].reset_index()[keys]
    z=z.merge(common,on=keys,how="inner")
    for variant,g in z.groupby("variant"): rows.append(metric_row(g,{"variant":variant,"scope":"COMMON_ABCDG","scope_value":"IDENTICAL_ROWS"}))
    return pd.DataFrame(rows)


def source_inventory(logs: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([
      {"source":"nhl.skater_game_logs_raw snapshot","coverage":f"{len(logs):,} rows; seasons {logs.season.min()}-{logs.season.max()}","timing":"game-final retained","authority":"retained canonical NHL game-log store","identity_keys":"season,game_id,player_id,team_id","leakage_risk":"target outcome excluded from every feature; used only as replay label","status":"CERTIFIED_WITH_STRICT_PRIOR_GATE"},
      {"source":"prior-season all-situations TOI","coverage":f"{logs.toi_minutes.notna().sum():,}/{len(logs):,}","timing":"completed prior games","authority":"skater_game_logs_raw.toi_minutes","identity_keys":"game_id,player_id","leakage_risk":"LOW_STRICT_PRIOR","status":"AVAILABLE"},
      {"source":"prior-season situation TOI","coverage":"shiftcharts retained only for season 2025; unavailable for 2023/2024 replay sources","timing":"completed games","authority":"shiftcharts/manpower tables","identity_keys":"game_id,player_id","leakage_risk":"LOW when available","status":"PARTIAL_NOT_USED_FOR_CROSS_SEASON_REPLAY"},
      {"source":"shots/SOG rate/attempts","coverage":f"SOG {logs.shots_on_goal.notna().sum():,}; attempts {logs.shot_attempts.notna().sum():,}","timing":"completed prior games","authority":"skater_game_logs_raw","identity_keys":"game_id,player_id","leakage_risk":"LOW_STRICT_PRIOR","status":"AVAILABLE"},
      {"source":"position","coverage":f"{logs.position.notna().sum():,}/{len(logs):,}","timing":"current retained player reference; historical as-of timestamp absent","authority":"nhl.players","identity_keys":"player_id","leakage_risk":"BOUNDED_TEMPORAL_PROVENANCE","status":"AVAILABLE_WITH_QUALIFICATION"},
      {"source":"team/opponent pace context","coverage":"canonical team/opponent reconstructed deterministically from official game home/away IDs plus skater is_home; raw pre-2025 skater team_id not trusted","timing":"completed prior season only","authority":"nhl.games + skater_game_logs_raw.is_home","identity_keys":"season,game_id,player_id","leakage_risk":"LOW_STRICT_PRIOR_AFTER_CANONICAL_RECONSTRUCTION","status":"AVAILABLE_WITH_EXPLICIT_IDENTITY_REPAIR"},
      {"source":"preseason history","coverage":f"{int(games.game_type.eq(1).sum())} retained schedule games; no historical 2023-2025 preseason game logs","timing":"strictly prior only","authority":"nhl.games / game logs","identity_keys":"game_id,player_id","leakage_risk":"LOW once completed","status":"PROSPECTIVE_ONLY_NOT_HISTORICALLY_EVALUABLE"},
      {"source":"line assignment / confirmed lineup / injury / depth","coverage":"no certified historical timestamped source","timing":"unavailable","authority":"none","identity_keys":"none","leakage_risk":"HIGH_IF_INFERRED_FROM_OUTCOME","status":"UNAVAILABLE_FAIL_CLOSED_OR_ROSTER_UNCONFIRMED"},
      {"source":"minor/prior-league translation","coverage":"no licensed certified historically testable source","timing":"unavailable","authority":"none","identity_keys":"none","leakage_risk":"UNACCEPTABLE_IF_INVENTED","status":"UNAVAILABLE_NOT_USED"},
      {"source":"sportsbook odds","coverage":"intentionally absent","timing":"not applicable","authority":"none","identity_keys":"none","leakage_risk":"not applicable","status":"NOT_REQUIRED"},
    ])

def feature_semantics() -> pd.DataFrame:
    training=ROOT/"backend/nhl/exports/train_nhl_sog_denali_pairings_v1__no_shiftcounts.csv"
    use=["szn_toi_per_game_5on5","szn_toi_per_game_pp","szn_toi_per_game_pk","season_5on5_icetime_per_game","season_5on4_icetime_per_game","season_4on5_icetime_per_game","d5_toi_min_avg","d10_toi_min_avg","d20_toi_min_avg"]
    d=pd.read_csv(training,usecols=lambda c:c in use,low_memory=False)
    semantics={
      "szn_toi_per_game_5on5":("strictly-prior season-to-date average even-strength shift overlap","minutes/game"),
      "szn_toi_per_game_pp":("strictly-prior season-to-date average power-play shift overlap","minutes/game"),
      "szn_toi_per_game_pk":("strictly-prior season-to-date average penalty-kill shift overlap","minutes/game"),
      "season_5on5_icetime_per_game":("duplicate-family strictly-prior season 5v5 average before scorer conversion","seconds/game"),
      "season_5on4_icetime_per_game":("duplicate-family strictly-prior season 5v4 average before scorer conversion","seconds/game"),
      "season_4on5_icetime_per_game":("duplicate-family strictly-prior season 4v5 average before scorer conversion","seconds/game"),
      "d5_toi_min_avg":("all-situations average TOI over prior five appearances","minutes/game"),
      "d10_toi_min_avg":("all-situations average TOI over prior ten appearances","minutes/game"),
      "d20_toi_min_avg":("all-situations average TOI over prior twenty appearances","minutes/game"),
    }
    rows=[]
    for field,(meaning,unit) in semantics.items():
        x=pd.to_numeric(d[field],errors="coerce")
        rows.append({"field":field,"exact_semantics":meaning,"unit":unit,"training_rows":len(x),"nonmissing":x.notna().sum(),"missing":x.isna().sum(),"zero":x.eq(0).sum(),"mean":x.mean(),"p10":x.quantile(.1),"median":x.median(),"p90":x.quantile(.9),"training_imputation":"GLOBAL_NUMERIC_NULL_TO_0.0_IN_LEGACY_DENALI_TRAINER","poisson_selection_order":"rolling d10,d20,d5 then szn 5v5+PP then season seconds/60","cold_start_v1_behavior":"NO_ZERO_FILL_FAIL_CLOSED_OR_CERTIFIED_PRIOR"})
    return pd.DataFrame(rows)


def manifest(directory: Path) -> None:
    files=sorted(p for p in directory.iterdir() if p.is_file() and p.name!="SHA256SUMS")
    (directory/"SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def main() -> int:
    ap=argparse.ArgumentParser();ap.add_argument("--output",type=Path,default=DEFAULT_OUT);ap.add_argument("--refresh-snapshot",action="store_true");ap.add_argument("--dsn",default=os.environ.get("SUPABASE_DB_URL",""));args=ap.parse_args()
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    lp=out/"retained_skater_history_snapshot.parquet";gp=out/"retained_game_schedule_snapshot.parquet"
    if args.refresh_snapshot: logs,games=fetch_snapshot(args.dsn,out)
    else:
        if not lp.is_file() or not gp.is_file(): raise RuntimeError("SNAPSHOT_MISSING_USE_REFRESH_SNAPSHOT")
        logs=pd.read_parquet(lp);games=pd.read_parquet(gp)
    replay,coverage,target=build_replay(logs,games);m=metrics(replay);sources=source_inventory(logs,games)
    replay.to_parquet(out/"historical_cold_start_replay_ledger.parquet",index=False)
    coverage.to_parquet(out/"coverage_by_player_game_variant.parquet",index=False)
    m.to_csv(out/"variant_comparison.csv",index=False);sources.to_csv(out/"cold_start_feature_source_inventory.csv",index=False);feature_semantics().to_csv(out/"feature_semantics_and_training_distribution.csv",index=False)
    cov=coverage.groupby(["variant","player_class"],as_index=False).agg(rows=("game_id","size"),scoreable=("scoreable","sum"));cov["coverage_rate"]=cov.scoreable/cov.rows;cov.to_csv(out/"coverage_by_player_class.csv",index=False)
    transition=m[m.scope.eq("TEAM_GAMES_PRIOR")].copy();transition.to_csv(out/"transition_analysis.csv",index=False)
    pd.DataFrame(columns=["run_id","prediction_identity","canonical_season","slate_date","phase","contract_arm","arm_status","game_id","player_id","line","expected_sog","p_over","p_under","selected_side","cold_start_class","transition_state","market_attachment_status"]).to_csv(out/"immutable_prediction_ledger.csv",index=False)
    pd.DataFrame(columns=["prediction_identity","game_id","player_id","official_sog","participation_status","grading_state","outcome_source","outcome_source_timestamp_utc","grading_timestamp_utc"]).to_csv(out/"canonical_outcome_ledger.csv",index=False)
    allm=m[(m.scope.eq("ALL"))].set_index("variant")
    replayable=[v for v in VARIANTS if v in allm.index and int(allm.loc[v,"rows"])>0]
    summary={"task":"NHL_SOG_COLD_START_PREDICTION_PLATFORM_V1","as_of":"2026-09-19","retained_log_rows":len(logs),"replay_target_player_games":target[["game_id","player_id"]].drop_duplicates().shape[0],"replay_seasons":sorted(target.season.unique().astype(int).tolist()),"variants_replayable":replayable,"variant_not_historically_evaluable":{"F_CURRENT_PRESEASON_UPDATE":"no retained 2023-2025 preseason game logs"},"selected_contract":"D_PLAYER_ROLE_HIERARCHICAL","challengers_retained":["A_PRIOR_SEASON_CARRY_FORWARD","B_PRIOR_SEASON_RECENCY_WEIGHTED","C_MULTISEASON_SHRUNK_PLAYER","E_TEAM_CHANGE_AWARE","G_COLD_START_TO_CURRENT_SEASON_BLEND"],"selection_reason":"A is strongest on returning-player common rows, but D supplies explicit sparse/old-history/rookie coverage with similar aggregate proper scores. E's aggregate proper-score change is negligible and G is mixed, so both remain shadow-only; F is implemented but unqualified. No single construction established stable superiority across both replay seasons, lines and depths.","first_eligible_prospective_date":"2026-09-20_IF_CANONICAL_NONEMPTY_PREGAME_SLATE","september_19":"OPERATIONAL_OUTCOME_ONLY_RETROSPECTIVE_PREDICTIONS_FORBIDDEN","network_market_requests":0,"paid_credits":0,"database_writes":0,"scheduler_changes":0,"production_model_promoted":False}
    write_json(out/"machine_readable_summary.json",summary)
    contract=json.loads(CONTRACT_PATH.read_text());write_json(out/"frozen_contract.json",contract)
    write_json(out/"model_identity.json",{"selected_contract_sha256":sha256_file(CONTRACT_PATH),"cold_start_core_sha256":sha256_file(ROOT/"backend/nhl/sog_cold_start/core.py"),"cold_start_cli_sha256":sha256_file(ROOT/"backend/nhl/sog_cold_start/cli.py"),"operational_runner_sha256":sha256_file(ROOT/"backend/nhl/scripts/run_nhl_sog_prediction_only_warn_only.py"),"frozen_poisson_scorer":"backend/nhl/scripts/score_sog_poisson_baseline.py","frozen_poisson_scorer_sha256":sha256_file(ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py"),"legacy_denali_training_source":"backend/nhl/exports/train_nhl_sog_denali_pairings_v1__no_shiftcounts.csv","legacy_denali_training_source_sha256":sha256_file(ROOT/"backend/nhl/exports/train_nhl_sog_denali_pairings_v1__no_shiftcounts.csv"),"selected_model_is_promotion":False})
    table_rows=allm.reset_index()[["variant","rows","player_games","accuracy","brier","log_loss","ece_10","roc_auc"]]
    table="| Variant | Rows | Player-games | Accuracy | Brier | Log loss | ECE | AUC |\n|---|---:|---:|---:|---:|---:|---:|---:|\n"+"\n".join(
        f"| {r.variant} | {int(r.rows)} | {int(r.player_games)} | {r.accuracy:.6f} | {r.brier:.6f} | {r.log_loss:.6f} | {r.ece_10:.6f} | {r.roc_auc:.6f} |"
        for r in table_rows.itertuples(index=False)
    )
    report=f"""# NHL SOG cold-start prediction platform V1\n\n## Decision\n\nThe odds-independent feature and prediction platform is ready. `D_PLAYER_ROLE_HIERARCHICAL` is the selected coverage-capable arm. A/B/C, team-aware E, and the gradual ten-appearance G transition are shadow comparators; E's aggregate proper-score change is negligible and G is mixed, so neither can replace D. F is implemented as explicitly unqualified until real strictly-prior preseason evidence accumulates. Sportsbook data is not an input.\n\n## Historical replay\n\n{table}\n\nThe replay covers seasons 2024 and 2025, {summary['replay_target_player_games']:,} participation-conditioned player-games through ten prior team games, and the canonical 1.5/2.5/3.5 ladder. Identical-row, season, line, team, player-class, and exposure-depth slices are in `variant_comparison.csv`. Historical preseason logs do not exist locally, so F is implementation-ready but not historically evaluated. Raw pre-2025 skater team IDs failed plausibility, so the replay deterministically reconstructs canonical team/opponent from official game home/away IDs and each retained `is_home` flag. Position is a current retained reference without historical as-of versions. No September 2026 outcome selected a weight or threshold.\n\n## Exact TOI semantics\n\n`szn_toi_per_game_5on5`, `szn_toi_per_game_pp`, and `szn_toi_per_game_pk` are season-to-date, strictly-prior averages in minutes derived from shift/manpower overlap. `season_5on5_icetime_per_game`, `season_5on4_icetime_per_game`, and `season_4on5_icetime_per_game` are the same average quantities in seconds before division by 60. Rolling `d5/d10/d20_toi_min_avg` fields are prior-game all-situations minutes per game and precede season situation fallbacks in the Poisson scorer. The production Poisson path defaulted a missing rate/TOI chain to lambda zero; legacy Denali training and scoring filled numeric nulls with 0.0. This contract does neither.\n\n## Operational boundary\n\nSeptember 19 remains blocked prospective evidence. The earliest eligible date is September 20, contingent on a retained canonical nonempty pregame slate and active-roster identity. MIDDAY and FINAL_PREGAME predictions are immutable, market-free, and separately gradeable. Market columns remain explicitly unavailable. No wagering, upload, public publication or model promotion is authorized.\n"""
    (out/"report.md").write_text(report)
    (out/"daily_integrity_report.md").write_text("# Daily integrity contract\n\nRequire a pre-start cutoff, unique player-game inputs, three unique half-line predictions per admitted player, immutable create-only run directories, explicit exclusions, no market dependency, and SHA-256 verification. September 19 and earlier are hard-blocked.\n")
    (out/"progress_report.md").write_text("# Progress\n\nFeature inventory: complete. Historical two-season replay: complete. Frozen contract: complete. Prediction capture and grading primitives: ready. Historical preseason validation: unavailable. Market attachment: unavailable and not required. Prospective observations: zero at freeze.\n")
    (out/"operator_runbook.md").write_text("# Operator runbook and rollback\n\nThe installed 900-second NHL shadow runner invokes the prediction-only observer in the already-authorized MIDDAY/FINAL windows. It never requests odds. Inspect `artifacts/operational/nhl/sog_prediction_only`. A repeated phase is a no-op. To roll back, revert the integration commit; retain immutable run artifacts. Never delete or rewrite a prediction run. Grading consumes canonical final outcomes only and creates a separate grade directory.\n")
    manifest(out);print(stable_json(summary));return 0

if __name__=="__main__": raise SystemExit(main())
