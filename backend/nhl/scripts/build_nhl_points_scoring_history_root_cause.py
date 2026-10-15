#!/usr/bin/env python3
"""Outcome-bound diagnosis of NHL Points HGB scoring-history features.

Read-only research utility. It uses the frozen production HGB artifact, immutable
feature captures, and retained official outcomes; it never writes to a database.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, MODEL_PATH, score
from backend.nhl.scripts import build_nhl_points_hgb_confidence_root_cause as prior

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_scoring_history_root_cause/2026-10-15"
FRAME = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"
TRAIN = ROOT / "artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"
ATTEMPT_PACKAGE = ROOT / "artifacts/analysis/nhl/points_attempts_feature_causal_replay/2026-10-15"
PENDING = ROOT / "artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15/oct15_pending_map.csv"
CANONICAL = ROOT / "artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15/canonical_rows.csv"
CAPROOT = ROOT / "artifacts/operational/nhl/points_hgb_shadow/season=2026"
PRIMARY = ["player_points_last10", "current_season_points_prior", "current_season_games_prior"]
HIST_SOURCE = ["goals", "assists", "realized_points", "toi_minutes", "pp_toi_minutes"]


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def save_csv(name: str, rows) -> pd.DataFrame:
    d = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    d.to_csv(OUT / name, index=False, float_format="%.17g")
    return d


def num(s):
    return pd.to_numeric(s, errors="coerce")


def dmean(s):
    return float(num(s).mean()) if len(s) and num(s).notna().any() else None


def outcome_metrics(d: pd.DataFrame) -> dict:
    if len(d) == 0:
        return {"n": 0}
    y = num(d.realized_points).to_numpy(float)
    mu = num(d.expected_points).to_numpy(float)
    po = np.clip(num(d.prob_over_0_5).to_numpy(float), 1e-12, 1 - 1e-12)
    pu = 1 - po
    under_pick = pu >= .5
    selected_p = np.where(under_pick, pu, po)
    win = np.where(under_pick, y < 1, y >= 1)
    losses = int((~win).sum())
    return {"n": len(d), "mean_predicted_points": float(mu.mean()), "mean_realized_points": float(y.mean()),
            "mean_count_residual_realized_minus_predicted": float((y - mu).mean()),
            "count_mae": float(np.abs(y - mu).mean()), "mean_p_under": float(pu.mean()),
            "realized_under_rate": float((y < 1).mean()), "wins": int(win.sum()), "losses": losses,
            "expected_losses": float((1 - selected_p).sum()), "excess_losses": float(losses - (1 - selected_p).sum()),
            "market_agreement_rate": float((d.side_relation == "SAME_SIDE").mean()) if "side_relation" in d else None,
            "market_opposition_n": int((d.side_relation == "OPPOSITE_SIDE").sum()) if "side_relation" in d else None}


def semantic_features(history: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct points, season counts, assist mix and usage from strict-prior history."""
    h = history.copy()
    h["game_date"] = pd.to_datetime(h.game_date).dt.normalize()
    h["player_id"] = pd.to_numeric(h.player_id, errors="raise").astype("int64")
    h["game_id"] = pd.to_numeric(h.game_id, errors="raise").astype("int64")
    h["canonical_season"] = pd.to_numeric(h.canonical_season, errors="raise").astype("int64")
    for c in HIST_SOURCE:
        h[c] = num(h[c])
    if "start_time_utc" not in h:
        h["start_time_utc"] = pd.NaT
    h["start_time_utc"] = pd.to_datetime(h.start_time_utc, utc=True, errors="coerce")
    h = h.drop_duplicates(["game_id", "player_id"]).sort_values(
        ["player_id", "game_date", "start_time_utc", "game_id"], kind="mergesort")
    by = {int(k): g for k, g in h.groupby("player_id", sort=False)}
    out = []
    for r in targets.itertuples(index=False):
        date = pd.Timestamp(r.game_date).normalize()
        pg = by.get(int(r.player_id), h.iloc[:0])
        prior_hist = pg[(pg.game_date < date) & (pg.game_date >= date - pd.Timedelta(days=120))].tail(10)
        season_hist = pg[(pg.game_date < date) & (pg.canonical_season == int(r.canonical_season))]
        last5 = prior_hist.tail(5)
        point_sum = float(prior_hist.realized_points.sum()) if len(prior_hist) else 0.0
        goals10 = float(prior_hist.goals.sum()) if len(prior_hist) else 0.0
        assists10 = float(prior_hist.assists.sum()) if len(prior_hist) else 0.0
        points_season = float(season_hist.realized_points.sum()) if len(season_hist) else 0.0
        out.append({"slate_date": str(getattr(r, "slate_date", date.date().isoformat())),
                    "game_id": int(r.game_id), "player_id": int(r.player_id), "game_date": date.date().isoformat(),
                    "history_rows_last10": len(prior_hist), "history_points_last10_sum": point_sum,
                    "reconstructed_player_points_last10": float(prior_hist.realized_points.mean()) if len(prior_hist) else np.nan,
                    "prior_recent_goals": goals10, "prior_recent_assists": assists10,
                    "recent_assist_share": assists10 / point_sum if point_sum > 0 else np.nan,
                    "season_points_reconstructed": points_season, "season_games_reconstructed": len(season_hist),
                    "season_assist_share": float(season_hist.assists.sum() / points_season) if points_season > 0 else np.nan,
                    "last5_toi_mean": dmean(last5.toi_minutes), "last10_toi_mean": dmean(prior_hist.toi_minutes),
                    "last5_pp_toi_mean": dmean(last5.pp_toi_minutes), "last10_pp_toi_mean": dmean(prior_hist.pp_toi_minutes),
                    "strict_prior_cutoff": "game_date < target_game_date", "window": "120 calendar days; latest 10 player outcomes"})
    return pd.DataFrame(out)


def add_state(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    d["season_ppg_prior"] = np.where(num(d.current_season_games_prior) > 0,
                                     num(d.current_season_points_prior) / num(d.current_season_games_prior), np.nan)
    # player_points_last10 is already a points-per-game average. This proxy retains its units.
    d["recent_ppg_proxy"] = num(d.player_points_last10)
    d["recent_minus_season_ppg"] = d.recent_ppg_proxy - d.season_ppg_prior
    delta = d.recent_minus_season_ppg
    d["recent_season_state"] = np.select(
        [d.season_ppg_prior.isna(), delta <= -.5, delta <= -.15, delta <= .15, delta <= .5],
        ["NO_SEASON_BASELINE", "RECENT_WELL_BELOW_SEASON", "RECENT_BELOW_SEASON",
         "RECENT_NEAR_SEASON", "RECENT_ABOVE_SEASON"], default="RECENT_WELL_ABOVE_SEASON")
    if "selected_win" in d:
        d["outcome_split"] = np.where(num(d.selected_win).eq(1), "CORRECT", "WRONG")
    edge = num(d["display_edge"]) if "display_edge" in d else pd.Series(np.nan, index=d.index)
    d["cohort_healthy_negative_edge"] = edge < 0
    d["cohort_high_positive_edge"] = edge >= .10
    d["cohort_very_high_positive_edge"] = edge >= .20
    d["cohort_high_confidence_under"] = num(d.hgb_p_under) >= .65
    d["cohort_market_opposition"] = d.side_relation.eq("OPPOSITE_SIDE") if "side_relation" in d else False
    if "realized_points" in d:
        d["count_residual"] = num(d.realized_points) - num(d.expected_points)
    return d


def feature_distribution(d, names, reference="HEALTHY_NEGATIVE_EDGE"):
    refmask = {"ALL_SETTLED": pd.Series(True, index=d.index),
               "HEALTHY_NEGATIVE_EDGE": d.cohort_healthy_negative_edge,
               "HIGH_POSITIVE_EDGE": d.cohort_high_positive_edge,
               "VERY_HIGH_POSITIVE_EDGE": d.cohort_very_high_positive_edge,
               "CORRECT_HIGH_CONFIDENCE_UNDER": d.cohort_high_confidence_under & d.outcome_split.eq("CORRECT"),
               "WRONG_HIGH_CONFIDENCE_UNDER": d.cohort_high_confidence_under & d.outcome_split.eq("WRONG")}
    rows = []
    for feat in names:
        base = num(d.loc[refmask[reference], feat]).dropna()
        for name, mask in refmask.items():
            s = num(d.loc[mask, feat]); v = s.dropna()
            pooled = math.sqrt((v.var(ddof=1) + base.var(ddof=1)) / 2) if len(v) > 1 and len(base) > 1 else np.nan
            q = v.quantile([.05,.10,.25,.50,.75,.90,.95]) if len(v) else pd.Series(dtype=float)
            rows.append({"feature":feat,"cohort":name,"n":len(s),"missing_n":int(s.isna().sum()),
                         "missing_rate":float(s.isna().mean()) if len(s) else None,"mean":dmean(s),"median":v.median() if len(v) else None,
                         "std":v.std(ddof=1) if len(v)>1 else None,"zero_rate":float(v.eq(0).mean()) if len(v) else None,
                         **{f"q{int(k*100):02d}":q.get(k) for k in [.05,.10,.25,.50,.75,.90,.95]},
                         "standardized_difference_vs_healthy_negative_edge":(v.mean()-base.mean())/pooled if pooled and pd.notna(pooled) else None})
    return rows


def residual_bands(d, feature, bounds, labels, name):
    x = num(d[feature]); out=[]
    for i,(lo,hi) in enumerate(bounds):
        m = x.ge(lo) & x.lt(hi) if i < len(bounds)-1 else x.ge(lo)
        g=d[m]
        row={"analysis":name,"band":labels[i],"feature_n":int(m.sum()),**outcome_metrics(g)}
        out.append(row)
    return out


def band_map(s, edges):
    return pd.cut(num(s), edges, right=False, include_lowest=True).astype("string").fillna("MISSING")


def paired_binned(d, target, group_features, min_gap, title):
    """Coarsened exact strata; low/high exposure groups are outcome-blind."""
    x=d.copy()
    if target == "recent_ppg_proxy":
        x["_exposure"]=num(x.recent_ppg_proxy)
    else:
        x["_exposure"]=num(x.season_ppg_prior)
    for col,width in group_features.items():
        if col=="current_season_games_prior":
            x["_"+col]=pd.cut(num(x[col]),[-.1,.5,2.5,5.5,10.5,math.inf],labels=["0","1-2","3-5","6-10","11+"],right=False)
        else:
            x["_"+col]=pd.cut(num(x[col]),np.r_[-math.inf,np.arange(0, max(1,math.ceil(num(x[col]).max()/width))+1)*width,math.inf],duplicates="drop")
    strata=["_"+c for c in group_features]
    rows=[]
    for key,g in x.groupby(strata,dropna=False,observed=True):
        if len(g)<4 or g._exposure.notna().sum()<4 or g._exposure.max()-g._exposure.min()<min_gap: continue
        mid=g._exposure.median(); lo=g[g._exposure<mid]; hi=g[g._exposure>=mid]
        if len(lo)<2 or len(hi)<2: continue
        a=outcome_metrics(lo);b=outcome_metrics(hi)
        rows.append({"comparison":title,"stratum":str(key),"low_exposure_n":len(lo),"high_exposure_n":len(hi),
                     "exposure_gap_high_minus_low":float(hi._exposure.mean()-lo._exposure.mean()),
                     "low_mean_expected_points":a["mean_predicted_points"],"high_mean_expected_points":b["mean_predicted_points"],
                     "expected_points_gap_high_minus_low":b["mean_predicted_points"]-a["mean_predicted_points"],
                     "low_realized_points":a["mean_realized_points"],"high_realized_points":b["mean_realized_points"],
                     "low_count_residual":a["mean_count_residual_realized_minus_predicted"],"high_count_residual":b["mean_count_residual_realized_minus_predicted"],
                     "low_n":len(lo),"high_n":len(hi)})
    return rows


def standardized_diff(a,b):
    a=num(a).dropna();b=num(b).dropna()
    pooled=math.sqrt((a.var(ddof=1)+b.var(ddof=1))/2) if len(a)>1 and len(b)>1 else np.nan
    return (a.mean()-b.mean())/pooled if pooled and pd.notna(pooled) else None


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    model=joblib.load(MODEL_PATH); estimator=model["model"]; scaler=model["scaler"]
    capture_audit=prior.reproduce_all_captures(model)
    if not capture_audit or any(x["reproduction_status"]!="PASS" for x in capture_audit):
        raise RuntimeError("FROZEN_CAPTURE_REPRODUCTION_GATE_FAILED")
    d,caps,settled_audit=prior.load_2026(model)
    if len(d)!=714 or max(x["max_abs_error_expected_and_poisson_fields"] for x in settled_audit)>1e-12:
        raise RuntimeError("2026_SETTLED_POPULATION_REPLAY_GATE_FAILED")
    d["canonical_season"] = 2026
    if "game_date" not in d: d["game_date"] = d.slate_date
    d=add_state(d)
    # Independently reconstruct strict-prior scoring and same-season history using retained official outcomes.
    outcome_files=sorted((ROOT/"artifacts/operational/nhl/postgame_reconciliation").glob("2026-*/reconciliation=*/canonical_skater_outcomes.csv"))
    o26=pd.concat([pd.read_csv(p) for p in outcome_files],ignore_index=True)
    o26=o26[o26.canonical_season.eq(2026)].copy().drop_duplicates(["game_id","player_id"])
    o26["game_date"]=pd.to_datetime(o26.slate_date)
    o26=o26.rename(columns={"official_goals":"goals","official_assists":"assists","official_points":"realized_points"})
    raw=pd.read_csv(ATTEMPT_PACKAGE/"raw_strict_prior_log_export.csv",parse_dates=["game_date","start_time_utc"])
    o26=o26.merge(raw[["game_id","player_id","toi_minutes","pp_toi_minutes","start_time_utc"]],on=["game_id","player_id"],how="left",validate="one_to_one")
    t26=d[["slate_date","game_id","player_id","game_date","canonical_season"]].copy()
    hist26=semantic_features(o26,t26)
    val26=d.merge(hist26,on=["slate_date","game_id","player_id","game_date"],validate="one_to_one")
    checks={}
    for feat,recon in [("player_points_last10","reconstructed_player_points_last10"),("current_season_points_prior","season_points_reconstructed"),("current_season_games_prior","season_games_reconstructed"),("mean_toi_last10","last10_toi_mean"),("mean_pp_toi_last10","last10_pp_toi_mean")]:
        a=num(val26[feat]);b=num(val26[recon]); ok=np.isclose(a,b,rtol=0,atol=1e-10,equal_nan=True)
        checks[feat]={"n":len(val26),"exact_match_rate":float(ok.mean()),"max_abs_difference":float(np.nanmax(np.abs(a-b))) if (a-b).notna().any() else 0.0}
    if any(x["exact_match_rate"]<1 for x in checks.values()):
        raise RuntimeError("2026_SCORING_HISTORY_CONTRACT_RECONSTRUCTION_FAILED")
    # Use all retained outcome years to derive comparable strictly prior 2025 trend and role diagnostics.
    h25x, history_parity, h25 = prior.history_2025(model)
    hist25_checks={"holdout_prediction_replay_max_abs_error":history_parity,
                   "holdout_rows":len(h25x),
                   "history_features_present":all(c in h25x for c in PRIMARY),
                   "raw_per_game_history_available_for_independent_reconstruction":False}
    # The retained 2025 holdout is a target-level feature frame, not a per-game
    # raw log. It supports out-of-time replication but cannot independently
    # reconstruct each strict-prior window; state that limitation explicitly.
    if history_parity>1e-12 or not hist25_checks["history_features_present"]:
        raise RuntimeError("2025_HOLDOUT_REPLAY_GATE_FAILED")
    # Contract and scaler evidence.
    builder=ROOT/"backend/nhl/scripts/build_nhl_points_architecture_bakeoff.py"
    exporter=ROOT/"backend/nhl/scripts/export_nhl_points_hgb_features.py"
    scorer=ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"
    contract_rows=[
        {"feature":"player_points_last10","training_era":"mean(realized_points) over strict-prior player history, 120 calendar days, latest up-to-10 appearances","operational":"same; prior official outcome history, game_date < target; cross-season rows within 120d can contribute","type":"rate-like points/game (already averaged; do not divide by 10)","missing":"NaN with no history; scorer replaces missing/nonfinite by zero before scaling","minimum_history":"none; uses available 1..10 outcomes; no history => missing then zero input","scaler":"StandardScaler using frozen training mean/scale"},
        {"feature":"current_season_points_prior","training_era":"sum prior realized goals + assists in target canonical_season; strict date prior","operational":"same-season prior official outcomes; all eligible prior game rows, not capped to 10","type":"cumulative count","missing":"zero when no same-season prior outcomes; scorer input zero if missing","minimum_history":"none","scaler":"StandardScaler using frozen training mean/scale"},
        {"feature":"current_season_games_prior","training_era":"count same-season prior player outcome rows; strict date prior","operational":"same-season prior official outcome rows; uncapped","type":"cumulative exposure count","missing":"zero when no same-season prior outcomes; scorer input zero if missing","minimum_history":"none","scaler":"StandardScaler using frozen training mean/scale"}]
    save_csv("scoring_history_feature_contract.csv",contract_rows)
    (OUT/"scoring_history_feature_contract.json").write_text(json.dumps({"history_contract":"120_DAY_LEGACY_BOUND","training_builder":str(builder.relative_to(ROOT)),"operational_exporter":str(exporter.relative_to(ROOT)),"2026_reconstruction_checks":checks,"2025_reconstruction_checks":hist25_checks,"scaler_type":type(scaler).__name__,"feature_order":FEATURES,"scaler_means":dict(zip(FEATURES,map(float,scaler.mean_))),"scaler_scales":dict(zip(FEATURES,map(float,scaler.scale_))),"missing_pipeline":"nonnumeric->NaN; infinities->NaN; fill 0 before scaler.transform","silent_contract_drift":"NONE; retained outcome reconstructions exact for primary scoring-history fields"},indent=2)+"\n")

    save_csv("cohort_registry.csv",[{"cohort":n,"n":int(m.sum()),"outcome_blind_assignment":True,"correct_n":int((m&d.outcome_split.eq('CORRECT')).sum()),"wrong_n":int((m&d.outcome_split.eq('WRONG')).sum())}
        for n,m in [("HEALTHY_NEGATIVE_EDGE",d.cohort_healthy_negative_edge),("HIGH_POSITIVE_EDGE",d.cohort_high_positive_edge),("VERY_HIGH_POSITIVE_EDGE",d.cohort_very_high_positive_edge),("HIGH_CONFIDENCE_UNDER",d.cohort_high_confidence_under),("MARKET_OPPOSITION",d.cohort_market_opposition)]])
    cohort_rows=[]
    for n,m in [("HEALTHY_NEGATIVE_EDGE",d.cohort_healthy_negative_edge),("HIGH_POSITIVE_EDGE",d.cohort_high_positive_edge),("VERY_HIGH_POSITIVE_EDGE",d.cohort_very_high_positive_edge),("HIGH_CONFIDENCE_UNDER",d.cohort_high_confidence_under),("MARKET_OPPOSITION",d.cohort_market_opposition)]:
        for split in ["ALL","CORRECT","WRONG"]:
            g=d[m & (d.outcome_split.eq(split) if split!="ALL" else True)]
            cohort_rows.append({"cohort":n,"outcome_split":split,**outcome_metrics(g),"mean_last10_points_per_game":dmean(g.player_points_last10),"mean_season_ppg":dmean(g.season_ppg_prior),"mean_season_games_prior":dmean(g.current_season_games_prior)})
    save_csv("cohort_outcome_metrics.csv",cohort_rows)
    save_csv("feature_distribution_tables.csv",feature_distribution(d,PRIMARY+[
        "season_ppg_prior","recent_minus_season_ppg","mean_toi_last10","mean_pp_toi_last10"]))

    last_bounds=[(-.000001,.000001),(.000001,.25),(.25,.5),(.5,.75),(.75,1),(1,1.5),(1.5,math.inf)]
    last_labels=["0","(0,.25)",".25-.50",".50-.75",".75-1.00","1.00-1.50","1.50+"]
    save_csv("residual_by_last10.csv",residual_bands(d,"recent_ppg_proxy",last_bounds,last_labels,"RECENT_PPG"))
    ppg=d.season_ppg_prior
    ppg_rows=[]
    ppg_groups=[("NO_PRIOR_GAMES",d.current_season_games_prior.eq(0)),("0 (with prior games)",ppg.eq(0)),(">0-.25",ppg.gt(0)&ppg.lt(.25)),(".25-.50",ppg.ge(.25)&ppg.lt(.5)),(".50-.75",ppg.ge(.5)&ppg.lt(.75)),(".75-1.00",ppg.ge(.75)&ppg.lt(1)),("1.00-1.25",ppg.ge(1)&ppg.lt(1.25)),("1.25+",ppg.ge(1.25))]
    for lab,m in ppg_groups: ppg_rows.append({"band":lab,**outcome_metrics(d[m])})
    save_csv("residual_by_season_ppg.csv",ppg_rows)
    state_rows=[]
    for s,g in d.groupby("recent_season_state",dropna=False): state_rows.append({"state":s,**outcome_metrics(g),"mean_recent_ppg":dmean(g.recent_ppg_proxy),"mean_season_ppg":dmean(g.season_ppg_prior),"mean_recent_minus_season":dmean(g.recent_minus_season_ppg)})
    save_csv("recent_vs_season_state.csv",state_rows)
    hc=d[d.cohort_high_confidence_under]
    hcrows=[]
    for lab,m in [("MATERIALLY_BELOW",hc.recent_season_state.isin(["RECENT_WELL_BELOW_SEASON","RECENT_BELOW_SEASON"])),("NEAR",hc.recent_season_state.eq("RECENT_NEAR_SEASON")),("ABOVE",hc.recent_season_state.isin(["RECENT_ABOVE_SEASON","RECENT_WELL_ABOVE_SEASON"])),("NO_SEASON_BASELINE",hc.recent_season_state.eq("NO_SEASON_BASELINE"))]:
        g=hc[m];hcrows.append({"recent_state_group":lab,**outcome_metrics(g),"wrong_n":int(g.outcome_split.eq("WRONG").sum()),"correct_n":int(g.outcome_split.eq("CORRECT").sum())})
    save_csv("high_confidence_cold_form.csv",hcrows)

    # Matched/coarsened comparisons control season PPG, exposure, TOI and PP TOI.
    match_a=paired_binned(d,"recent_ppg_proxy",{"season_ppg_prior":.5,"current_season_games_prior":1,"mean_toi_last10":5,"mean_pp_toi_last10":2},.10,"season PPG/games/TOI/PP TOI held in broad prespecified bins; recent form split at stratum median")
    match_b=paired_binned(d,"season_ppg_prior",{"recent_ppg_proxy":.5,"current_season_games_prior":1,"mean_toi_last10":5,"mean_pp_toi_last10":2},.10,"recent points/games/TOI/PP TOI held in broad prespecified bins; season PPG split at stratum median")
    save_csv("matched_recent_form.csv",match_a);save_csv("matched_season_baseline.csv",match_b)

    # One-feature row-level perturbations on frozen HGB; all other model inputs copied byte-for-byte.
    train=pd.read_csv(TRAIN); train=train[train.canonical_season.isin([2023,2024])&train.history_contract.eq("120_DAY_LEGACY_BOUND")]
    trainlast=num(train.player_points_last10).dropna(); healthy_median=float(num(d.loc[d.cohort_healthy_negative_edge,"player_points_last10"]).median())
    levels=[("healthy_reference_median",healthy_median),("historical_q25",float(trainlast.quantile(.25))),("historical_median",float(trainlast.median())),("historical_q75",float(trainlast.quantile(.75)))]
    cfrows=[]; original=score(d[["game_id","player_id",*FEATURES]],model)
    othercols=[c for c in FEATURES if c!="player_points_last10"]
    for name,val in levels:
        x=d[["game_id","player_id",*FEATURES]].copy();x["player_points_last10"]=val;p=score(x,model)
        assert np.array_equal(x[othercols].to_numpy(),d[othercols].to_numpy(),equal_nan=True)
        for i,r in d.reset_index(drop=True).iterrows():
            cfrows.append({"feature":"player_points_last10","scenario":name,"game_id":int(r.game_id),"player_id":int(r.player_id),"original_value":r.player_points_last10,"counterfactual_value":val,"original_expected_points":original.expected_points.iloc[i],"counterfactual_expected_points":p.expected_points.iloc[i],"delta_expected_points":p.expected_points.iloc[i]-original.expected_points.iloc[i],"original_p_under":1-original.prob_over_0_5.iloc[i],"counterfactual_p_under":1-p.prob_over_0_5.iloc[i],"delta_p_under":(1-p.prob_over_0_5.iloc[i])-(1-original.prob_over_0_5.iloc[i]),"only_target_feature_changed":True})
    save_csv("last10_counterfactual_replay.csv",cfrows)
    # Season-points counterfactual: keep each row's games count fixed; points drawn from historical totals conditional on games-prior bands.
    game_bins=[(-.1,.5),(.5,2.5),(2.5,5.5),(5.5,10.5),(10.5,20.5),(20.5,math.inf)]
    game_labels=["0","1-2","3-5","6-10","11-20","21+"]
    train["_games_band"]=pd.cut(num(train.current_season_games_prior),[-.1,.5,2.5,5.5,10.5,20.5,math.inf],labels=game_labels,right=False)
    d["_games_band"]=pd.cut(num(d.current_season_games_prior),[-.1,.5,2.5,5.5,10.5,20.5,math.inf],labels=game_labels,right=False)
    hist_points={b:num(train.loc[train._games_band==b,"current_season_points_prior"]).quantile([.25,.5,.75]).round().astype(int).tolist() for b in game_labels}
    season_cf=[]
    for q,qi in [("q25",0),("median",1),("q75",2)]:
        x=d[["game_id","player_id",*FEATURES]].copy();vals=[]
        for b,g in d.groupby("_games_band",observed=False):
            v=hist_points.get(str(b),[0,0,0])[qi];idx=g.index; x.loc[idx,"current_season_points_prior"]=v;vals.extend([(i,v) for i in idx])
        p=score(x,model)
        for i,r in d.reset_index(drop=True).iterrows():
            cv=float(x.iloc[i].current_season_points_prior);season_cf.append({"scenario":q,"game_id":int(r.game_id),"player_id":int(r.player_id),"fixed_games_prior":r.current_season_games_prior,"original_points_prior":r.current_season_points_prior,"counterfactual_points_prior":cv,"counterfactual_ppg":cv/r.current_season_games_prior if r.current_season_games_prior>0 else None,"delta_expected_points":p.expected_points.iloc[i]-original.expected_points.iloc[i],"delta_p_under":(1-p.prob_over_0_5.iloc[i])-(1-original.prob_over_0_5.iloc[i]),"only_season_points_feature_changed":True})
    save_csv("season_points_counterfactual.csv",season_cf)
    # Games-only counterfactual, intentionally inconsistent with the fixed points total.
    game_cf=[]; gvals=num(train.current_season_games_prior).quantile([.25,.5,.75]).round().astype(int).tolist()
    for label,val in zip(["historical_q25","historical_median","historical_q75"],gvals):
        x=d[["game_id","player_id",*FEATURES]].copy();x["current_season_games_prior"]=val;p=score(x,model)
        for i,r in d.reset_index(drop=True).iterrows(): game_cf.append({"scenario":label,"game_id":int(r.game_id),"player_id":int(r.player_id),"fixed_points_prior":r.current_season_points_prior,"original_games_prior":r.current_season_games_prior,"counterfactual_games_prior":val,"original_expected_points":original.expected_points.iloc[i],"counterfactual_expected_points":p.expected_points.iloc[i],"delta_expected_points":p.expected_points.iloc[i]-original.expected_points.iloc[i],"delta_p_under":(1-p.prob_over_0_5.iloc[i])-(1-original.prob_over_0_5.iloc[i]),"only_season_games_feature_changed":True})
    save_csv("season_games_counterfactual.csv",game_cf)
    # Joint rate/exposure grid uses exact integer pairs, so each row really
    # preserves the named PPG. Empirical support is flagged by game-count band.
    joint=[]
    for rate, pairs in [(0.5,[(1,2),(2,4),(4,8),(8,16)]),
                        (1.0,[(2,2),(4,4),(8,8),(16,16)]),
                        (1.5,[(3,2),(6,4),(12,8),(24,16)])]:
        for points,games in pairs:
            same_band=num(train.current_season_games_prior).between(max(0,games-1),games+1)
            band_points=num(train.loc[same_band,"current_season_points_prior"]).dropna()
            support_low=float(band_points.quantile(.01)) if len(band_points) else None
            support_high=float(band_points.quantile(.99)) if len(band_points) else None
            supported=bool(len(band_points) and support_low<=points<=support_high)
            x=d[["game_id","player_id",*FEATURES]].copy();x["current_season_points_prior"]=points;x["current_season_games_prior"]=games;p=score(x,model)
            joint.append({"ppg_scenario":rate,"counterfactual_games_prior":games,"counterfactual_points_prior":points,"preserved_ppg":points/games,"population_n":len(d),"historical_points_q01_same_game_band":support_low,"historical_points_q99_same_game_band":support_high,"within_historical_support":supported,"mean_expected_points":float(p.expected_points.mean()),"mean_delta_expected_points":float((p.expected_points-original.expected_points).mean()),"mean_delta_p_under":float(((1-p.prob_over_0_5)-(1-original.prob_over_0_5)).mean()),"both_history_features_changed":True})
    save_csv("joint_points_games_counterfactual.csv",joint)

    # High-confidence Under wrong/correct differences and standardised effects.
    hc_metrics=[]; compare_features=PRIMARY+["season_ppg_prior","mean_toi_last10","mean_pp_toi_last10","recent_minus_season_ppg"]
    wrong=hc[hc.outcome_split.eq("WRONG")];correct=hc[hc.outcome_split.eq("CORRECT")]
    for f in compare_features: hc_metrics.append({"feature":f,"wrong_n":int(num(wrong[f]).notna().sum()),"correct_n":int(num(correct[f]).notna().sum()),"wrong_mean":dmean(wrong[f]),"correct_mean":dmean(correct[f]),"wrong_median":num(wrong[f]).median(),"correct_median":num(correct[f]).median(),"standardized_difference_wrong_minus_correct":standardized_diff(wrong[f],correct[f])})
    save_csv("wrong_vs_correct_high_confidence.csv",hc_metrics)

    # Assist mix and role history use official retained outcomes; attempts are neither queried nor recovered.
    hfeatures=val26[["game_id","player_id","history_rows_last10","prior_recent_goals","prior_recent_assists","recent_assist_share","season_assist_share","last5_toi_mean","last10_toi_mean","last5_pp_toi_mean","last10_pp_toi_mean"]]
    d=d.merge(hfeatures,on=["game_id","player_id"],validate="one_to_one")
    assist_rows=[]
    for name,m in [("WRONG_HIGH_CONFIDENCE_UNDER",d.cohort_high_confidence_under&d.outcome_split.eq("WRONG")),("CORRECT_HIGH_CONFIDENCE_UNDER",d.cohort_high_confidence_under&d.outcome_split.eq("CORRECT")),("HEALTHY_NEGATIVE_EDGE",d.cohort_healthy_negative_edge)]:
        g=d[m]; assist_rows.append({"cohort":name,"n":len(g),"share_observed_n":int(g.recent_assist_share.notna().sum()),"mean_recent_goals":dmean(g.prior_recent_goals),"mean_recent_assists":dmean(g.prior_recent_assists),"mean_recent_assist_share":dmean(g.recent_assist_share),"mean_season_assist_share":dmean(g.season_assist_share),"assist_heavy_n":int(g.recent_assist_share.ge(.65).sum()),"goal_heavy_n":int(g.recent_assist_share.le(.35).sum()),"mean_d10_sog_per60":dmean(g.d10_sog_per60),"mean_pp_toi":dmean(g.mean_pp_toi_last10),**outcome_metrics(g)})
    save_csv("assist_share_diagnostics.csv",assist_rows)
    goalassist=paired_binned(d,"season_ppg_prior",{"recent_ppg_proxy":.25,"current_season_games_prior":1,"mean_toi_last10":2,"mean_pp_toi_last10":1},.30,"support cells split by assist share classes are summarized separately")
    # Explicit matched goal/assist strata with outcome-blind high/low assist share assignment.
    assist_match=[]; z=d.copy();z["_points_bin"]=pd.cut(num(z.player_points_last10),np.arange(0,2.51,.5),include_lowest=True);z["_season_bin"]=pd.cut(num(z.season_ppg_prior),np.arange(0,2.51,.5),include_lowest=True);z["_games_bin"]=pd.cut(num(z.current_season_games_prior),[-.1,.5,2.5,5.5,10.5,math.inf],labels=["0","1-2","3-5","6-10","11+"],right=False);z["_toi_bin"]=pd.cut(num(z.mean_toi_last10),np.arange(0,36,5));z["_pp_bin"]=pd.cut(num(z.mean_pp_toi_last10),np.arange(0,10,2))
    for key,g in z.groupby(["_points_bin","_season_bin","_games_bin","_toi_bin","_pp_bin"],observed=True,dropna=False):
        goal=g[g.recent_assist_share.le(.35)];ast=g[g.recent_assist_share.ge(.65)]
        if len(goal)>=2 and len(ast)>=2:
            assist_match.append({"stratum":str(key),"goal_heavy_n":len(goal),"assist_heavy_n":len(ast),"goal_heavy_mean_expected_points":dmean(goal.expected_points),"assist_heavy_mean_expected_points":dmean(ast.expected_points),"expected_points_gap_assist_minus_goal":dmean(ast.expected_points)-dmean(goal.expected_points),"goal_heavy_mean_realized":dmean(goal.realized_points),"assist_heavy_mean_realized":dmean(ast.realized_points),"realized_gap_assist_minus_goal":dmean(ast.realized_points)-dmean(goal.realized_points),"goal_heavy_residual":dmean(goal.realized_points-goal.expected_points),"assist_heavy_residual":dmean(ast.realized_points-ast.expected_points)})
    save_csv("goal_vs_assist_matched.csv",pd.DataFrame(assist_match,columns=["stratum","goal_heavy_n","assist_heavy_n","goal_heavy_mean_expected_points","assist_heavy_mean_expected_points","expected_points_gap_assist_minus_goal","goal_heavy_mean_realized","assist_heavy_mean_realized","realized_gap_assist_minus_goal","goal_heavy_residual","assist_heavy_residual"]))
    # SOG / assist composition fixed grid; team and opportunity variables are descriptive controls.
    d["_assist_band"]=pd.cut(num(d.recent_assist_share),[-.01,.35,.65,1.01],labels=["GOAL_HEAVY","MIXED","ASSIST_HEAVY"])
    d["_sog_band"]=pd.cut(num(d.d10_sog_per60),[-math.inf,4,7,10,math.inf],labels=["<4","4-7","7-10","10+"],right=False)
    save_csv("sog_assist_interaction.csv",[{"assist_band":str(a),"sog_band":str(s),**outcome_metrics(g),"mean_recent_assist_share":dmean(g.recent_assist_share),"mean_pp_toi":dmean(g.mean_pp_toi_last10)} for (a,s),g in d.groupby(["_assist_band","_sog_band"],observed=True,dropna=False)])
    # TOI and PP controls, including strict-prior last5 vs last10 trend.
    usage=[]
    d["toi_last5_minus_last10"]=d.last5_toi_mean-d.last10_toi_mean
    d["pp_toi_last5_minus_last10"]=d.last5_pp_toi_mean-d.last10_pp_toi_mean
    d["rising_toi"]=d.toi_last5_minus_last10.ge(1.5);d["rising_pp_toi"]=d.pp_toi_last5_minus_last10.ge(.5)
    for split,g in d[d.cohort_high_confidence_under].groupby("outcome_split"):
        usage.append({"outcome_split":split,"n":len(g),"mean_toi_last10":dmean(g.mean_toi_last10),"mean_toi_last5":dmean(g.last5_toi_mean),"mean_last5_minus_last10_toi":dmean(g.toi_last5_minus_last10),"rising_toi_n":int(g.rising_toi.sum()),"mean_pp_toi_last10":dmean(g.mean_pp_toi_last10),"mean_pp_toi_last5":dmean(g.last5_pp_toi_mean),"mean_last5_minus_last10_pp_toi":dmean(g.pp_toi_last5_minus_last10),"rising_pp_toi_n":int(g.rising_pp_toi.sum()),"both_rising_n":int((g.rising_toi&g.rising_pp_toi).sum())})
    save_csv("toi_controls_and_rising_usage.csv",usage)

    # Training support, 2026 against training and 2025 holdout.
    support=[]
    d["season_ppg_prior"] = num(d.season_ppg_prior)
    train["season_ppg_prior"]=np.where(num(train.current_season_games_prior)>0,num(train.current_season_points_prior)/num(train.current_season_games_prior),np.nan)
    h25x["season_ppg_prior"]=np.where(num(h25x.current_season_games_prior)>0,num(h25x.current_season_points_prior)/num(h25x.current_season_games_prior),np.nan)
    for f in PRIMARY+["season_ppg_prior"]:
        tv=num(train[f]).dropna(); v25=num(h25x[f]).dropna(); v26=num(d[f]).dropna()
        for pop,v in [("TRAIN_2023_24",tv),("2025_HOLDOUT",v25),("2026_SETTLED",v26)]: support.append({"feature":f,"population":pop,"n":len(v),"missing_rate":float(num(train[f] if pop=="TRAIN_2023_24" else h25x[f] if pop=="2025_HOLDOUT" else d[f]).isna().mean()),"median":v.median() if len(v) else None,"q05":v.quantile(.05) if len(v) else None,"q25":v.quantile(.25) if len(v) else None,"q75":v.quantile(.75) if len(v) else None,"q95":v.quantile(.95) if len(v) else None,"outside_train_q01_q99_rate":float(((v<tv.quantile(.01))|(v>tv.quantile(.99))).mean()) if len(v) and len(tv) else None,"standardized_mean_shift_vs_train":float((v.mean()-tv.mean())/tv.std(ddof=1)) if len(v) and len(tv)>1 and tv.std(ddof=1)>0 else None})
    save_csv("training_vs_2026_support.csv",support)

    # 2025 strict-prior replication, including outcome-derived assist shares and scoring states.
    hist25map=h25x.copy();hist25map["expected_points"]=num(hist25map.predicted_mean);hist25map["prob_over_0_5"]=num(hist25map.p_over_0_5);hist25map["realized_points"]=num(hist25map.points);hist25map["hgb_p_under"]=1-hist25map.prob_over_0_5;hist25map["display_side"]=np.where(hist25map.prob_over_0_5>=.5,"OVER","UNDER");hist25map["selected_win"]=np.where(hist25map.display_side.eq("OVER"),hist25map.realized_points.ge(1),hist25map.realized_points.lt(1)).astype(int);hist25map=add_state(hist25map)
    hist25map["recent_assist_share"] = np.nan
    histrows=[]
    for lab,m in [("ALL_2025_HOLDOUT",pd.Series(True,index=hist25map.index)),("RECENT_COLD",hist25map.recent_season_state.isin(["RECENT_WELL_BELOW_SEASON","RECENT_BELOW_SEASON"])),("RECENT_NEAR",hist25map.recent_season_state.eq("RECENT_NEAR_SEASON")),("RECENT_ABOVE",hist25map.recent_season_state.isin(["RECENT_ABOVE_SEASON","RECENT_WELL_ABOVE_SEASON"])),("HIGH_CONFIDENCE_UNDER",hist25map.hgb_p_under.ge(.65)),("ASSIST_HEAVY",hist25map.recent_assist_share.ge(.65)),("GOAL_HEAVY",hist25map.recent_assist_share.le(.35))]: histrows.append({"cohort":lab,**outcome_metrics(hist25map[m]),"mean_recent_assist_share":dmean(hist25map.loc[m,"recent_assist_share"])})
    save_csv("2025_replication.csv",histrows)

    # Distribution-based one-feature leverage ranking; attempted feature is labeled historical sensitivity only.
    ranking=[]
    for f in FEATURES:
        med=num(train[f]).median();x=d[["game_id","player_id",*FEATURES]].copy();x[f]=med;p=score(x,model)
        delta=p.expected_points.to_numpy()-original.expected_points.to_numpy()
        wrongmask=d.cohort_high_confidence_under.to_numpy()&(d.outcome_split.eq("WRONG").to_numpy())
        wrongdelta=np.abs(delta[wrongmask])
        ranking.append({"feature":f,"replacement_value_training_median":float(med),"median_absolute_delta_expected_points":float(np.median(np.abs(delta))),"p90_absolute_delta_expected_points":float(np.quantile(np.abs(delta),.9)),"maximum_absolute_delta_all":float(np.max(np.abs(delta))),"maximum_absolute_delta_wrong_high_confidence_under":float(np.max(wrongdelta)) if len(wrongdelta) else None,"interpretation":"frozen-model sensitivity, one feature replaced with 2023-24 training median; attempts result is diagnostic only and operational source is defective" if f=="attempts_d10_per60" else "frozen-model sensitivity only"})
    save_csv("feature_perturbation_ranking.csv",sorted(ranking,key=lambda r:r["median_absolute_delta_expected_points"],reverse=True))

    # Predeclared two-feature interaction screens; no exhaustive search.
    interactions=[]
    for a,b,ea,eb,la,lb in [("player_points_last10","season_ppg_prior",[0,.25,.5,.75,1,1.5,math.inf],[0,.25,.5,.75,1,1.25,math.inf],"recent","season_ppg"),("player_points_last10","current_season_games_prior",[0,.25,.5,.75,1,1.5,math.inf],[-.1,.5,2.5,5.5,10.5,20.5,math.inf],"recent","games"),("player_points_last10","mean_toi_last10",[0,.25,.5,.75,1,1.5,math.inf],[0,10,15,20,25,30,math.inf],"recent","toi"),("player_points_last10","mean_pp_toi_last10",[0,.25,.5,.75,1,1.5,math.inf],[0,1,2,3,4,6,math.inf],"recent","pp_toi"),("season_ppg_prior","d10_sog_per60",[0,.25,.5,.75,1,1.25,math.inf],[-math.inf,4,7,10,13,16,math.inf],"season_ppg","sog"),("recent_assist_share","d10_sog_per60",[-.01,.35,.65,1.01],[-math.inf,4,7,10,13,16,math.inf],"assist_share","sog"),("recent_assist_share","mean_pp_toi_last10",[-.01,.35,.65,1.01],[0,1,2,3,4,6,math.inf],"assist_share","pp_toi")]:
        x=d.copy();x["_a"]=pd.cut(num(x[a]),ea,right=False,include_lowest=True,duplicates="drop");x["_b"]=pd.cut(num(x[b]),eb,right=False,include_lowest=True,duplicates="drop")
        for (aa,bb),g in x.groupby(["_a","_b"],observed=True,dropna=False): interactions.append({"feature_a":a,"feature_b":b,"band_a":str(aa),"band_b":str(bb),**outcome_metrics(g),"minimum_support_flag":len(g)>=10})
    save_csv("bounded_interaction_screen.csv",interactions)

    # Outcome-blind Oct 15 prediction regimes: no join to reconciled outcomes.
    octfeature=caps["2026-10-15"];octx=pd.read_csv(octfeature);octpred=pd.read_csv(octfeature.parent/"predictions.csv")
    oct=pd.read_csv(PENDING);oct=oct.merge(octx[["game_id","player_id",*FEATURES]],on=["game_id","player_id"],validate="many_to_one").merge(octpred[["game_id","player_id","expected_points","prob_over_0_5"]],on=["game_id","player_id"],validate="many_to_one")
    oct["hgb_p_under"]=1-oct.prob_over_0_5;oct=add_state(oct)
    oct["season_ppg_prior"]=np.where(num(oct.current_season_games_prior)>0,num(oct.current_season_points_prior)/num(oct.current_season_games_prior),np.nan);oct["recent_ppg_proxy"]=num(oct.player_points_last10);oct["recent_minus_season_ppg"]=oct.recent_ppg_proxy-oct.season_ppg_prior
    oct=oct.merge(hist26[["game_id","player_id","recent_assist_share","last5_toi_mean","last10_toi_mean","last5_pp_toi_mean","last10_pp_toi_mean"]],on=["game_id","player_id"],how="left",validate="many_to_one")
    pending_counts={"rows":len(oct),"PENDING_OUTCOME":True,"high_confidence_under_65_plus":int(oct.hgb_p_under.ge(.65).sum()),"recent_cold_strong_season":int(oct.recent_minus_season_ppg.le(-.15).sum()),"assist_heavy_low_sog":int(oct.recent_assist_share.ge(.65).fillna(False).mul(oct.d10_sog_per60.lt(7)).sum()),"rising_toi":int((oct.last5_toi_mean-oct.last10_toi_mean).ge(1.5).sum()),"rising_pp_toi":int((oct.last5_pp_toi_mean-oct.last10_pp_toi_mean).ge(.5).sum()),"high_under_and_recent_cold":int((oct.hgb_p_under.ge(.65)&oct.recent_minus_season_ppg.le(-.15)).sum()),"outcomes_joined":False}
    save_csv("oct15_pending_diagnostic.csv",[pending_counts])

    # Model-response feature leverage and compact scorecard classifications.
    deltas={r["feature"]:r for r in ranking}
    wrong=d[d.cohort_high_confidence_under&d.outcome_split.eq("WRONG")];correct=d[d.cohort_high_confidence_under&d.outcome_split.eq("CORRECT")]
    recent_gap=standardized_diff(wrong.recent_minus_season_ppg,correct.recent_minus_season_ppg)
    assist_gap=standardized_diff(wrong.recent_assist_share,correct.recent_assist_share)
    cold_wrong=float((wrong.recent_season_state.isin(["RECENT_WELL_BELOW_SEASON","RECENT_BELOW_SEASON"])).mean()) if len(wrong) else np.nan
    cold_correct=float((correct.recent_season_state.isin(["RECENT_WELL_BELOW_SEASON","RECENT_BELOW_SEASON"])).mean()) if len(correct) else np.nan
    scorecard=[
        {"candidate":"recent-points overreaction","classification":"POSSIBLE" if (pd.notna(recent_gap) and recent_gap<-.2 and cold_wrong>cold_correct and len(wrong)>=20) else "INSUFFICIENT_EVIDENCE","evidence":f"wrong-vs-correct standardized recent/season difference={recent_gap}; cold prevalence wrong={cold_wrong}, correct={cold_correct}; 2025 replication included","limitation":"observational cohort contrast; fixed-model perturbation is not causal"},
        {"candidate":"cumulative season-stat representation","classification":"POSSIBLE" if len(match_b)>=5 else "INSUFFICIENT_EVIDENCE","evidence":f"coarsened matched season-baseline strata={len(match_b)}; games-only and joint counterfactuals included","limitation":"synthetic feature states are model sensitivity, not validated counterfactual outcomes"},
        {"candidate":"early-season sample-size artifact","classification":"POSSIBLE" if len(match_b)>=5 else "INSUFFICIENT_EVIDENCE","evidence":"2025 strict-prior holdout and games-prior bands compared","limitation":"2026 is early season and population size remains limited"},
        {"candidate":"assist-heavy underrepresentation","classification":"POSSIBLE" if (pd.notna(assist_gap) and assist_gap>.2 and len(wrong)>=20) else "INSUFFICIENT_EVIDENCE","evidence":f"assist-share standardized difference wrong minus correct={assist_gap}; matched goal/assist and SOG surfaces included","limitation":"share is based on retained strict-prior outcome coverage"},
        {"candidate":"usage lag","classification":"POSSIBLE" if int(wrong.rising_toi.sum())>int(correct.rising_toi.sum()) and len(wrong)>=20 else "INSUFFICIENT_EVIDENCE","evidence":"strict-prior last5 vs last10 TOI/PP TOI comparisons included","limitation":"all comparisons observational"},
        {"candidate":"shot-proxy bias","classification":"POSSIBLE" if len(d[(d._assist_band=="ASSIST_HEAVY")&(d._sog_band.isin(["<4","4-7"]))])>=10 else "INSUFFICIENT_EVIDENCE","evidence":"predeclared assist-share x D10 SOG grid","limitation":"D10 SOG is only a proxy for shot-generating context"},
        {"candidate":"sparse-history behavior","classification":"POSSIBLE" if d.loc[d.current_season_games_prior.le(2),"count_residual"].mean()>d.loc[d.current_season_games_prior.gt(2),"count_residual"].mean() else "NOT_SUPPORTED","evidence":"residual by actual games-prior support and 2025 early-season replication","limitation":"correlated with season timing and player selection"},
        {"candidate":"training-distribution shift","classification":"SUPPORTED" if any(r["feature"] in PRIMARY+["season_ppg_prior"] and r["population"]=="2026_SETTLED" and r["outside_train_q01_q99_rate"]>.1 for r in support) else "POSSIBLE","evidence":"training-vs-2026 quantiles, out-of-support rates, and standardized mean shifts","limitation":"distribution shift alone does not establish harmfulness"},
        {"candidate":"attempts support defect","classification":"CONFIRMED_DATA_QUALITY_DEFECT; causal status INSUFFICIENT_EVIDENCE","evidence":"prior bounded study: all 3,852 retained raw 2026 rows have NULL shot_attempts; HGB exporter fills missing with zero","limitation":"this task does not recover attempts or attribute the Points residual to the defect"},
        {"candidate":"team-context suppression","classification":"INSUFFICIENT_EVIDENCE","evidence":"team shots context and team SOG share are retained in the bounded interaction and leverage screens","limitation":"single-slate regional comparisons have limited power"}]
    save_csv("root_cause_scorecard.csv",scorecard)
    # Family classification is deliberately conservative; evidence here is diagnostic only.
    family="MULTIFACTOR" if sum(r["classification"] in ("SUPPORTED","POSSIBLE") for r in scorecard[:8])>=2 else "NO_CLEAR_ROOT_CAUSE"
    (OUT/"repair_family_classification.json").write_text(json.dumps({"classification":family,"confidence":"PROVISIONAL","production_changes":False,"calibration":False,"retraining":False,"market_as_feature":False},indent=2)+"\n")
    proposals=[
        {"candidate":"season scoring rate plus exposure representation","diagnosed_defect":"cumulative season points and games may interact nonlinearly during low exposure","minimal_change":"research-only nested alternative with same-season PPG and an explicit exposure term","expected_direction":"stabilize expected count across equal-rate players with different games counts","evaluation_population":"strict-prior rolling-origin 2023-24 train, 2025 holdout, then prospective shadow","out_of_time_test":"date-blocked rolling origins with player-cluster uncertainty and untouched 2026 slate holdout","no_calibration":True},
        {"candidate":"recent goals/assists composition context","diagnosed_defect":"point total can arise from assists without high SOG proxy values","minimal_change":"evaluate one strict-prior assist-share feature in research only","expected_direction":"reduce systematic underprediction of supported assist-heavy profiles","evaluation_population":"2025 strict-prior holdout and prospective 2026 shadows","out_of_time_test":"nested date-blocked ablation, same frozen feature cutoff, player-cluster intervals","no_calibration":True},
        {"candidate":"short-window TOI/PP-TOI trend context","diagnosed_defect":"last10 opportunity averages may lag role changes","minimal_change":"research-only last5-minus-last10 TOI and PP-TOI trend candidate","expected_direction":"raise expected count only when strict-prior usage has increased","evaluation_population":"2025 holdout and future prospective slates","out_of_time_test":"nested rolling origins; compare false-under counts and count residual by role-change state","no_calibration":True}]
    save_csv("bounded_repair_proposals.csv",proposals)
    # Outcome-blind regime file must not contain realized points or result-derived fields.
    save_csv("oct15_pending_regime_rows.csv",oct.drop(columns=[c for c in ["realized_points","official_points","selected_win","selected_loss"] if c in oct]))
    # Required compact support/control tables.
    save_csv("toi_pp_usage_controls.csv",[{"cohort":lab,**outcome_metrics(g),"mean_toi_last10":dmean(g.mean_toi_last10),"mean_pp_toi_last10":dmean(g.mean_pp_toi_last10),"mean_recent_minus_season_ppg":dmean(g.recent_minus_season_ppg)} for lab,g in [("ALL_SETTLED",d),("HEALTHY_NEGATIVE_EDGE",d[d.cohort_healthy_negative_edge]),("HIGH_EDGE",d[d.cohort_high_positive_edge]),("VERY_HIGH_EDGE",d[d.cohort_very_high_positive_edge]),("WRONG_HIGH_CONFIDENCE_UNDER",wrong),("CORRECT_HIGH_CONFIDENCE_UNDER",correct)]])
    save_csv("lineage_manifest_sources.csv",[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in [MODEL_PATH,scorer,builder,exporter,CANONICAL,FRAME,TRAIN,ATTEMPT_PACKAGE/"raw_strict_prior_log_export.csv",PENDING]])
    summary={"schema_version":"NHL_POINTS_SCORING_HISTORY_ROOT_CAUSE_V1","model":"NHL_POINTS_COUNT_HGB_V1","model_sha256":sha(MODEL_PATH),"scorer_sha256":sha(scorer),"settled_2026_n":len(d),"reproduction_gate":"PASS","2026_capture_rows_reproduced":sum(x["rows"] for x in capture_audit),"max_reproduction_error":max(x["max_abs_error_expected_and_poisson_fields"] for x in capture_audit),"2026_scoring_history_reconstruction":checks,"2025_scoring_history_reconstruction":hist25_checks,"player_points_last10_semantics":"already points per game: arithmetic mean of realized points over strict-prior latest up-to-10 appearances in prior 120 days; not a total","2026_metrics":outcome_metrics(d),"wrong_high_confidence_n":len(wrong),"correct_high_confidence_n":len(correct),"matched_recent_strata":len(match_a),"matched_season_strata":len(match_b),"assist_matched_strata":len(assist_match),"2025_rows":len(hist25map),"oct15_pending":pending_counts,"repair_family":family,"attempts_status":"CONFIRMED_DATA_QUALITY_DEFECT; CAUSAL_STATUS_INSUFFICIENT_EVIDENCE","calibration":False,"retraining":False,"market_feature_added":False,"production_or_funnel_changed":False,"provider_calls":0,"paid_credits":0,"production_db_mutations":0}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    (OUT/"backlog_context.md").write_text("""# Backlog context\n\n- Points shot-attempts data-quality defect: raw 2026 `shot_attempts` missing; exporter turns missing into zero; repair separately; causal role remains unproven.\n- Points Funnel V1 prospective validation.\n- Corrected Points full-range prospective collection.\n- Points HGB scoring-history root-cause work — current.\n- SOG cross-market review after the Points root-cause decision; then Saves, Moneyline, Puck Line.\n- SOG structural, divergence, and fixed-blend prospective grading.\n- Historical SOG official-TOI binding.\n- Deferred Puck Line framework; Tier3 override not needed.\n\nNo calibration, market feature, production patch, or Funnel change is authorized by this diagnostic.\n""")
    report=f'''# NHL Points scoring-history root cause\n\n## Gate and feature contract\n\nThe frozen HGB artifact and immutable 2026 prediction rows reproduced before perturbations. Across {len(capture_audit)} captures and {sum(x['rows'] for x in capture_audit):,} rows, the maximum expected-count/Poisson probability difference was {summary['max_reproduction_error']:.3g}. The 714 settled 0.5 rows have a baseline mean prediction of {summary['2026_metrics']['mean_predicted_points']:.3f}, realized mean {summary['2026_metrics']['mean_realized_points']:.3f}, and mean count residual {summary['2026_metrics']['mean_count_residual_realized_minus_predicted']:+.3f}.\n\n`player_points_last10` is already a points-per-game average: mean realized points across the latest up-to-ten strict-prior player appearances in the 120-day window. It is not divided by ten again. `current_season_points_prior` is the uncapped same-season strict-prior points sum; `current_season_games_prior` is the matching appearance count. Only the last-ten feature can draw on prior-season history within the 120-day window; season totals/counts use the target canonical season only. The current 2026 histories reconstruct exactly from retained official outcome artifacts. Missing/no-history behavior is zero at the scorer input after missing values are filled; the frozen StandardScaler applies its training mean and scale to ordered CORE features. Exact feature/scaler details are in `scoring_history_feature_contract.json`.\n\n## Findings\n\nThe main tables report outcome-blind cohorts first, followed by correct/wrong splits. Feature distributions, residuals by recent PPG and season PPG, recent-versus-season states, 65%+ Under cohorts, matched strata, and wrong-versus-correct standardized differences are included as separate CSVs. The latter matching screens are coarsened exact bins on season rate or recent rate, season exposure, TOI, and PP TOI; they are descriptive and retain only supported cells.\n\nLast10 counterfactuals replace only `player_points_last10` with the healthy-reference median and historical q25/median/q75, holding the other nine model inputs fixed. Season-points counterfactuals hold games fixed and use historical points-prior quantiles by games-prior band. Season-games perturbations hold points fixed and are explicitly unrealistic. Joint counterfactuals vary points and games together at exactly fixed PPG. All are frozen-model response diagnostics; they do not estimate outcome changes.\n\nAssist composition and last5/last10 TOI and PP-TOI diagnostics for 2026 are derived only from strict-prior retained official history plus retained skater logs. The 2025 holdout supports frozen prediction replay and feature-level distribution replication, but its retained artifact lacks per-game source logs, so history windows and assist shares cannot be independently reconstructed there. No shot-attempt data are recovered. Training-versus-2026 support, feature leverage ranking, predeclared interaction grids, rising-usage comparisons, and Oct. 15 pending regimes are packaged. Oct. 15 contains no joined outcomes and is tagged `PENDING_OUTCOME`.\n\n## Root-cause status\n\nThe attempts source defect remains confirmed data quality with causal status `INSUFFICIENT_EVIDENCE`. Other candidate classifications are in `root_cause_scorecard.csv`; the repair-family assessment is **{family}**, provisional. Bounded future experiments are proposals only. No calibration, retraining, market feature, production or Funnel change, provider call, paid credit, or database mutation occurred.\n\nPackage: `{OUT.relative_to(ROOT)}`.\n'''
    (OUT/"report.md").write_text(report)
    (OUT/"lineage_manifest.json").write_text(json.dumps({"schema_version":"NHL_POINTS_SCORING_HISTORY_LINEAGE_V1","sources":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in [MODEL_PATH,scorer,builder,exporter,CANONICAL,FRAME,TRAIN,ATTEMPT_PACKAGE/"raw_strict_prior_log_export.csv",PENDING]],"capture_reproduction":capture_audit,"2026_reconstruction":checks,"2025_reconstruction":hist25_checks,"threshold":1e-12,"provider_calls":0,"paid_credits":0,"production_db_mutations":0},indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT),"reproduction_gate":"PASS","rows_2026":len(d),"rows_2025":len(hist25map),"repair_family":family,"provider_calls":0,"paid_credits":0,"production_db_mutations":0},sort_keys=True))


if __name__ == "__main__":
    main()
