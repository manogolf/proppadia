#!/usr/bin/env python3
"""Build an isolated, reproducible NHL Moneyline team/game history foundation."""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
SOURCE_DIR = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15"
RAW_ROOT = ROOT / "artifacts/operational/nhl/moneyline_team_history/raw"
OUT_DEFAULT = ROOT / "artifacts/analysis/model_development/nhl_clean_moneyline_team_history/2026-09-30"
FEATURES = ["diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg", "diff_days_rest", "home_back_to_back", "away_back_to_back"]
SCHEDULE_URL = "https://api-web.nhle.com/v1/club-schedule-season/{club}/{season_key}"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_raw(path: Path, body: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = sha(body)
    if path.exists():
        prior = path.read_bytes()
        if sha(prior) != digest:
            raise RuntimeError(f"IMMUTABLE_RAW_SOURCE_CONFLICT:{path}")
        return digest
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(body)
    tmp.replace(path)
    return digest


def get(url: str, *, attempts: int = 5) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "NHL-Clean-Moneyline-History/1.0"})
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP_{response.status}:{url}")
                return response.read()
        except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(2 ** attempt, 16))
    raise RuntimeError(f"OFFICIAL_NHL_REQUEST_FAILED:{url}:{last}")


def schedule_season(season: int, clubs: list[str]) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
    key = f"{season}{season + 1}"
    collected: dict[int, dict[str, Any]] = {}
    ledger = []
    for club in clubs:
        url = SCHEDULE_URL.format(club=club, season_key=key)
        path = RAW_ROOT / f"season={season}" / "schedule" / f"club={club}.json"
        existed = path.exists()
        body = path.read_bytes() if existed else get(url)
        digest = atomic_raw(path, body)
        ledger.append({"endpoint": url, "path": str(path.relative_to(ROOT)), "sha256": digest, "bytes": len(body), "method": "REUSE" if existed else "GET"})
        payload = json.loads(body)
        for game in payload.get("games") or []:
            if int(game.get("gameType", 0)) != 2:
                continue
            gid = int(game["id"])
            row = {
                "season": season, "game_id": gid, "game_date": game["gameDate"],
                "scheduled_start_time_utc": game.get("startTimeUTC"), "game_type": int(game["gameType"]),
                "home_team_id": int(game["homeTeam"]["id"]), "home_team_code": game["homeTeam"]["abbrev"],
                "away_team_id": int(game["awayTeam"]["id"]), "away_team_code": game["awayTeam"]["abbrev"],
                "schedule_home_score": game.get("homeTeam", {}).get("score"), "schedule_away_score": game.get("awayTeam", {}).get("score"),
                "schedule_game_state": game.get("gameState"), "schedule_game_schedule_state": game.get("gameScheduleState"),
                "schedule_source": url, "schedule_source_sha256": digest,
            }
            if gid in collected and any(collected[gid].get(k) != row.get(k) for k in row if k not in {"schedule_source", "schedule_source_sha256"}):
                raise RuntimeError(f"OFFICIAL_SCHEDULE_CONFLICT:{gid}")
            collected[gid] = row
    return collected, ledger


def rows_from_gameweek(payload: dict[str, Any], *, season: int, source: str, digest: str, dates: set[str]) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    schedules: dict[int, dict[str, Any]] = {}
    boxes: dict[int, dict[str, Any]] = {}
    for day in payload.get("gameWeek") or []:
        date = str(day.get("date") or "")
        if date not in dates:
            continue
        for game in day.get("games") or []:
            if int(game.get("gameType", 0)) != 2:
                continue
            gid = int(game["id"])
            schedules[gid] = {
                "season": season, "game_id": gid, "game_date": game.get("gameDate", date),
                "scheduled_start_time_utc": game.get("startTimeUTC"), "game_type": int(game["gameType"]),
                "home_team_id": int(game["homeTeam"]["id"]), "home_team_code": game["homeTeam"]["abbrev"],
                "away_team_id": int(game["awayTeam"]["id"]), "away_team_code": game["awayTeam"]["abbrev"],
                "schedule_home_score": game.get("homeTeam", {}).get("score"), "schedule_away_score": game.get("awayTeam", {}).get("score"),
                "schedule_game_state": game.get("gameState"), "schedule_game_schedule_state": game.get("gameScheduleState"),
                "schedule_source": source, "schedule_source_sha256": digest,
            }
    return schedules, boxes


def acquire_boxscores(games: pd.DataFrame, *, workers: int = 8) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
    paths = {int(row.game_id): RAW_ROOT / f"season={int(row.season)}" / f"game={int(row.game_id)}" / "boxscore.json" for row in games.itertuples()}
    missing = [(gid, path) for gid, path in paths.items() if not path.exists()]
    fetched_ids = {gid for gid, _ in missing}
    def fetch(item: tuple[int, Path]) -> tuple[int, Path, bytes]:
        gid, path = item
        return gid, path, get(BOXSCORE_URL.format(game_id=gid))
    # Write each successful immutable response immediately as it arrives.
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for gid, path, body in pool.map(fetch, missing):
            atomic_raw(path, body)
    parsed: dict[int, dict[str, Any]] = {}
    ledger = []
    for gid, path in sorted(paths.items()):
        body = path.read_bytes()
        data = json.loads(body)
        if int(data.get("id", -1)) != gid:
            raise RuntimeError(f"BOXSCORE_ID_MISMATCH:{gid}")
        parsed[gid] = data
        ledger.append({"endpoint": BOXSCORE_URL.format(game_id=gid), "path": str(path.relative_to(ROOT)), "sha256": sha(body), "bytes": len(body), "method": "GET" if gid in fetched_ids else "REUSE"})
    return parsed, ledger


def make_canonical(schedule: dict[int, dict[str, Any]], boxes: dict[int, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for gid, sch in schedule.items():
        box = boxes.get(gid)
        if box is None:
            rows.append({**sch, "home_final_score": np.nan, "away_final_score": np.nan, "home_shots": np.nan, "away_shots": np.nan, "final_status": "SCHEDULED", "decision_type": None, "boxscore_sha256": None, "boxscore_source": None, "boxscore_raw_path": None, "regular_season": True})
            continue
        home, away = box.get("homeTeam") or {}, box.get("awayTeam") or {}
        for side, src in (("home", home), ("away", away)):
            if int(src.get("id", -1)) != int(sch[f"{side}_team_id"]):
                raise RuntimeError(f"BOXSCORE_TEAM_IDENTITY_CONFLICT:{gid}:{side}")
        for side, src in (("home", home), ("away", away)):
            scheduled_score = sch.get(f"schedule_{side}_score")
            if scheduled_score is not None and int(scheduled_score) != int(src.get("score")):
                raise RuntimeError(f"OFFICIAL_SCORE_SOURCE_CONFLICT:{gid}:{side}")
        raw_path = RAW_ROOT / f"season={int(sch['season'])}" / f"game={gid}" / "boxscore.json"
        digest = sha(raw_path.read_bytes())
        rows.append({**sch,
            "scheduled_start_time_utc": box.get("startTimeUTC") or sch.get("scheduled_start_time_utc"),
            "home_final_score": home.get("score"), "away_final_score": away.get("score"),
            "home_shots": home.get("sog"), "away_shots": away.get("sog"),
            "final_status": box.get("gameState"), "decision_type": (box.get("periodDescriptor") or {}).get("periodType", "REG"),
            "boxscore_sha256": digest, "boxscore_source": BOXSCORE_URL.format(game_id=gid),
            "boxscore_raw_path": str(raw_path.relative_to(ROOT)), "regular_season": int(sch["game_type"]) == 2,
        })
    frame = pd.DataFrame(rows)
    frame["scheduled_start_time_utc"] = pd.to_datetime(frame.scheduled_start_time_utc, utc=True)
    frame = frame.sort_values(["scheduled_start_time_utc", "game_id"], kind="stable").reset_index(drop=True)
    validate_canonical(frame)
    return frame


def validate_canonical(frame: pd.DataFrame) -> None:
    if frame.game_id.duplicated().any():
        raise RuntimeError("DUPLICATE_CANONICAL_GAME_ID")
    if frame[["home_team_id", "away_team_id"]].isna().any(axis=None):
        raise RuntimeError("CANONICAL_TEAM_ID_MISSING")
    if frame.home_team_id.eq(frame.away_team_id).any():
        raise RuntimeError("CANONICAL_HOME_AWAY_IDENTITY_CONFLICT")
    if frame.scheduled_start_time_utc.isna().any():
        raise RuntimeError("CANONICAL_SCHEDULED_START_MISSING")
    final = frame.final_status.isin(["OFF", "FINAL"])
    if (final & frame[["home_final_score", "away_final_score"]].isna().any(axis=1)).any():
        raise RuntimeError("CANONICAL_FINAL_SCORE_MISSING")
    if (frame[["home_shots", "away_shots"]] < 0).any(axis=None):
        raise RuntimeError("CANONICAL_NEGATIVE_SHOTS")


def make_team_games(games: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for g in games.itertuples(index=False):
        for home in (True, False):
            gf, ga = (g.home_final_score, g.away_final_score) if home else (g.away_final_score, g.home_final_score)
            sf, sa = (g.home_shots, g.away_shots) if home else (g.away_shots, g.home_shots)
            team, opp = (g.home_team_id, g.away_team_id) if home else (g.away_team_id, g.home_team_id)
            rows.append({"season": g.season, "game_id": g.game_id, "scheduled_start_time_utc": g.scheduled_start_time_utc,
                "game_date": g.game_date, "team_id": team, "opponent_id": opp, "is_home": int(home),
                "regular_season": bool(g.regular_season), "final_status": g.final_status, "goals_for": gf, "goals_against": ga, "goal_diff": gf-ga, "shots_for": sf,
                "shots_against": sa, "shot_diff": sf-sa, "won_game": (int(gf>ga) if pd.notna(gf) and pd.notna(ga) else np.nan), "decision_type": g.decision_type})
    frame = pd.DataFrame(rows).sort_values(["season", "team_id", "scheduled_start_time_utc", "game_id"], kind="stable").reset_index(drop=True)
    frame["game_number"] = frame.groupby(["season", "team_id"]).cumcount() + 1
    return frame


def validate_team_games(games: pd.DataFrame, team_games: pd.DataFrame) -> None:
    if len(team_games) != 2 * len(games):
        raise RuntimeError("TEAM_GAME_ROW_COUNT_CONFLICT")
    for game in games.itertuples(index=False):
        pair = team_games[team_games.game_id.eq(game.game_id)]
        home, away = pair[pair.is_home.eq(1)], pair[pair.is_home.eq(0)]
        if len(home) != 1 or len(away) != 1:
            raise RuntimeError(f"TEAM_GAME_HOME_AWAY_ORIENTATION_CONFLICT:{game.game_id}")
        h, a = home.iloc[0], away.iloc[0]
        if int(h.team_id) != int(game.home_team_id) or int(a.team_id) != int(game.away_team_id):
            raise RuntimeError(f"TEAM_GAME_IDENTITY_CONFLICT:{game.game_id}")
        if pd.notna(game.home_final_score) and (h.goals_for != game.home_final_score or a.goals_for != game.away_final_score or h.won_game != int(game.home_final_score > game.away_final_score)):
            raise RuntimeError(f"TEAM_GAME_SCORE_OR_WINNER_CONFLICT:{game.game_id}")
        if pd.notna(game.home_shots) and (h.shots_for != game.home_shots or a.shots_for != game.away_shots):
            raise RuntimeError(f"TEAM_GAME_SHOT_ORIENTATION_CONFLICT:{game.game_id}")


def add_strict_prior(team_games: pd.DataFrame) -> pd.DataFrame:
    result = team_games.copy()
    result["prior_games_played"] = 0
    result["days_rest"] = np.nan
    result["back_to_back"] = np.nan
    result["prior_game_id"] = pd.Series([pd.NA] * len(result), dtype="Int64")
    result["prior_game_start_utc"] = pd.Series([pd.NaT] * len(result), dtype="object")
    is_final = result.final_status.isin(["OFF", "FINAL"])
    result["prior_feature_status"] = np.where(is_final & result.goal_diff.isna(), "SOURCE_UNAVAILABLE", "INSUFFICIENT_HISTORY")
    for col in ("std_goal_diff_pg", "std_shot_diff_pg", "r10_goal_diff_pg", "r10_shot_diff_pg", "prior_goals_for", "prior_goals_against", "prior_shots_for", "prior_shots_against"):
        result[col] = np.nan
    result["scheduled_start_time_utc"] = pd.to_datetime(result.scheduled_start_time_utc, utc=True)
    for _, group in result.groupby(["season", "team_id"], sort=True):
        group = group.sort_values(["scheduled_start_time_utc", "game_id"], kind="stable")
        seen: list[int] = []
        for timestamp, same_start in group.groupby("scheduled_start_time_utc", sort=True):
            prior = result.loc[seen]
            prior = prior[prior.regular_season & prior.goal_diff.notna()]
            if not prior.empty:
                last = prior.iloc[-1]
                for idx in same_start.index:
                    row = result.loc[idx]
                    result.at[idx, "prior_games_played"] = len(prior)
                    delta = (row.scheduled_start_time_utc.date() - last.scheduled_start_time_utc.date()).days
                    result.at[idx, "days_rest"] = max(delta - 1, 0)
                    result.at[idx, "back_to_back"] = int(delta == 1)
                    result.at[idx, "prior_game_id"] = int(last.game_id)
                    result.at[idx, "prior_game_start_utc"] = last.scheduled_start_time_utc
                    result.at[idx, "std_goal_diff_pg"] = prior.goal_diff.mean()
                    result.at[idx, "std_shot_diff_pg"] = prior.shot_diff.mean()
                    result.at[idx, "r10_goal_diff_pg"] = prior.tail(10).goal_diff.mean()
                    result.at[idx, "r10_shot_diff_pg"] = prior.tail(10).shot_diff.mean()
                    result.at[idx, "prior_goals_for"] = prior.goals_for.sum()
                    result.at[idx, "prior_goals_against"] = prior.goals_against.sum()
                    result.at[idx, "prior_shots_for"] = prior.shots_for.sum()
                    result.at[idx, "prior_shots_against"] = prior.shots_against.sum()
                    result.at[idx, "prior_feature_status"] = "OBSERVED"
            # Same-start targets never observe each other. Results are added only
            # after the whole timestamp batch has received its prior snapshot.
            seen.extend(same_start.index.tolist())
    return result


def make_matrix(games: pd.DataFrame, team_games: pd.DataFrame, *, include_scheduled: bool = False) -> pd.DataFrame:
    by = add_strict_prior(team_games)
    homes = by[by.is_home.eq(1)].set_index("game_id")
    aways = by[by.is_home.eq(0)].set_index("game_id")
    rows=[]
    for g in games.itertuples(index=False):
        if not g.regular_season or (not include_scheduled and (pd.isna(g.home_final_score) or pd.isna(g.away_final_score))):
            continue
        h, a = homes.loc[g.game_id], aways.loc[g.game_id]
        target = int(g.home_final_score>g.away_final_score) if pd.notna(g.home_final_score) and pd.notna(g.away_final_score) else np.nan
        row={"season":g.season,"game_id":g.game_id,"game_date":g.game_date,"scheduled_start_time_utc":g.scheduled_start_time_utc,"home_win":target}
        row.update({"diff_std_goal_diff_pg":h.std_goal_diff_pg-a.std_goal_diff_pg,"diff_r10_goal_diff_pg":h.r10_goal_diff_pg-a.r10_goal_diff_pg,"diff_std_shot_diff_pg":h.std_shot_diff_pg-a.std_shot_diff_pg,"diff_days_rest":h.days_rest-a.days_rest,"home_back_to_back":h.back_to_back,"away_back_to_back":a.back_to_back})
        row["home_prior_games"]=h.prior_games_played; row["away_prior_games"]=a.prior_games_played
        rows.append(row)
    return pd.DataFrame(rows)


def metrics(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    edges=np.linspace(0,1,11); ece=0.0
    for lo,hi in zip(edges[:-1],edges[1:]):
        mask=(p>=lo)&((p<hi) if hi<1 else (p<=hi))
        if mask.any(): ece += mask.mean()*abs(float(y[mask].mean()-p[mask].mean()))
    return {"rows":int(len(y)),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,p,labels=[0,1])),"auc":float(roc_auc_score(y,p)),"accuracy":float(accuracy_score(y,p>=.5)),"ece_10_bin":float(ece)}


def fit_eval(matrix: pd.DataFrame) -> dict[str, Any]:
    splits=[("2023_to_2024",[2023],[2024]),("2024_to_2025",[2024],[2025]),("2023_2024_to_2025",[2023,2024],[2025])]
    out={}
    for name,train_seasons,test_seasons in splits:
        train=matrix[matrix.season.isin(train_seasons)]; test=matrix[matrix.season.isin(test_seasons)]
        imputer=SimpleImputer(strategy="median",keep_empty_features=True); xtr=imputer.fit_transform(train[FEATURES]); xte=imputer.transform(test[FEATURES])
        scaler=StandardScaler(); xtr=scaler.fit_transform(xtr); xte=scaler.transform(xte)
        model=LogisticRegression(C=1.0,solver="liblinear",random_state=20260713,max_iter=1000).fit(xtr,train.home_win)
        out[name]={**metrics(test.home_win.to_numpy(),model.predict_proba(xte)[:,1]),"train_rows":len(train),"train_seasons":train_seasons,"test_seasons":test_seasons,"imputer_medians":dict(zip(FEATURES,imputer.statistics_.tolist()))}
    return out


def compare_frozen_v2(matrix: pd.DataFrame) -> dict[str, Any]:
    path=SOURCE_DIR/"v2_predictions.csv"
    old=pd.read_csv(path,low_memory=False)
    if not {"game_id","v2_home_win_probability"}.issubset(old.columns):
        return {"status":"UNAVAILABLE_REQUIRED_V2_COLUMNS_MISSING"}
    paired=matrix.merge(old[["game_id","v2_home_win_probability"]],on="game_id",how="inner",validate="one_to_one")
    result={"status":"SAME_GAME_ID_POPULATION","old_v2_rows":int(len(old)),"paired_rows":int(len(paired)),"by_season":{}}
    for season, part in paired.groupby("season"):
        result["by_season"][str(int(season))]={"rows":int(len(part)),"clean_six_feature_complete":int(part[FEATURES].notna().all(axis=1).sum()),"frozen_v2_metrics":metrics(part.home_win.to_numpy(),part.v2_home_win_probability.to_numpy())}
    result["semantic_differences"]=["clean source is retained official NHL gamecenter boxscore plus official club-season schedule responses","clean chronology uses scheduled start UTC and strictly earlier timestamps, excluding simultaneous starts","the frozen V2 comparison uses its previously fitted parameters and V2 median/scaler policy; no V2 artifact is modified"]
    return result


def build(output: Path, *, workers: int=8) -> dict[str, Any]:
    base=[]
    for season in (2023,2024,2025):
        path=SOURCE_DIR/f"corrected_spine_{season}.csv"
        df=pd.read_csv(path,low_memory=False)
        df=df[df.game_type.eq(2)]
        base.extend(df[["game_id","home_team","away_team"]].assign(season=season).to_dict("records"))
    seed_ids=pd.DataFrame(base).drop_duplicates().astype({"season":int,"game_id":int})
    schedules={}; schedule_ledger=[]
    for season in (2023,2024,2025):
        clubs=sorted(set(seed_ids.loc[seed_ids.season.eq(season),"home_team"])|set(seed_ids.loc[seed_ids.season.eq(season),"away_team"]))
        found, ledger=schedule_season(season,clubs); schedules.update(found); schedule_ledger.extend(ledger)
    wanted={(int(r.season),int(r.game_id)) for r in seed_ids.itertuples()}
    schedules={gid:r for gid,r in schedules.items() if (int(r["season"]),gid) in wanted}
    if wanted != {(int(v["season"]),gid) for gid,v in schedules.items()}:
        missing=sorted(wanted-{(int(v["season"]),gid) for gid,v in schedules.items()})
        raise RuntimeError(f"SCHEDULE_SOURCE_MISSING:{missing[:10]}")
    schedule_df=pd.DataFrame(schedules.values())
    boxes,box_ledger=acquire_boxscores(schedule_df,workers=workers)
    canonical=make_canonical(schedules,boxes)
    # Reuse the same canonical parser for retained official Sep 29 outcomes and
    # the Sep 30 target schedule, using the immutable schedule/boxscore bodies.
    rec=ROOT/"artifacts/operational/nhl/postgame_reconciliation/2026-09-29/reconciliation=6d05dcde9b5e46bd775a"
    outcome=pd.read_csv(rec/"canonical_game_outcomes.csv")
    idx=json.loads((rec/"request_lineage.json").read_text())
    acq=ROOT/"artifacts/operational/nhl/postgame_reconciliation/request_runs/2026-09-29/nhlpostgameacq_20260929_20260930T160249132353Z_9648a128/preserved_responses/objects"
    authority=next(source for source in idx["response_sources"] if source["role"]=="AUTHORITY_RESPONSE_SOURCE")
    sched_claim=next(claim for claim in authority["responses"] if claim["endpoint_family"]=="SCHEDULE")
    schedule_path=acq/f"{sched_claim['object_sha256']}.json"
    schedule_body=schedule_path.read_bytes()
    schedule_raw=RAW_ROOT/"season=2026"/"schedule"/"slate=2026-09-29.json"
    schedule_digest=atomic_raw(schedule_raw,schedule_body)
    nhl_2026_schedule=json.loads(schedule_body)
    schedules_2026, _=rows_from_gameweek(nhl_2026_schedule,season=2026,source=str(schedule_raw.relative_to(ROOT)),digest=schedule_digest,dates={"2026-09-29","2026-09-30"})
    boxes_2026={}
    box_claims={int(claim["resource_identity"]["game_id"]):claim for claim in authority["responses"] if claim["endpoint_family"]=="BOXSCORE"}
    for gid, claim in box_claims.items():
        p=acq/f"{claim['object_sha256']}.json"; body=p.read_bytes()
        bp=RAW_ROOT/f"season=2026/game={gid}/boxscore.json"; atomic_raw(bp,body)
        boxes_2026[gid]=json.loads(body)
    if set(boxes_2026)!=set(box_claims) or set(boxes_2026)-set(schedules_2026):
        raise RuntimeError("SEP29_RETAINED_OFFICIAL_SOURCE_SET_MISMATCH")
    sep30_raw_path=ROOT/"artifacts/operational/nhl/slates/2026-09-30/raw_schedule_response.json"
    sep30_payload=json.loads(sep30_raw_path.read_text())
    sep30_rows,_=rows_from_gameweek(sep30_payload,season=2026,source=str(sep30_raw_path.relative_to(ROOT)),digest=sha(sep30_raw_path.read_bytes()),dates={"2026-09-30"})
    for gid,row in sep30_rows.items():
        retained=schedules_2026.get(gid)
        if retained is None or any(retained[key]!=row[key] for key in ("scheduled_start_time_utc","home_team_id","away_team_id","home_team_code","away_team_code")):
            raise RuntimeError(f"SEP30_SCHEDULE_SOURCE_CONFLICT:{gid}")
    canonical_2026=make_canonical(schedules_2026,boxes_2026)
    reconciled=outcome.set_index("game_id")
    sep29=canonical_2026[canonical_2026.game_date.eq("2026-09-29")]
    if set(sep29.game_id.astype(int)) != set(outcome.game_id.astype(int)):
        raise RuntimeError("SEP29_RECONCILIATION_GAME_SET_CONFLICT")
    for game in sep29.itertuples():
        official=reconciled.loc[int(game.game_id)]
        if int(game.home_team_id)!=int(official.home_team_id) or int(game.away_team_id)!=int(official.away_team_id):
            raise RuntimeError(f"SEP29_RECONCILIATION_IDENTITY_CONFLICT:{game.game_id}")
        if int(game.home_final_score)!=int(official.official_final_home_goals) or int(game.away_final_score)!=int(official.official_final_away_goals):
            raise RuntimeError(f"SEP29_RECONCILIATION_OUTCOME_CONFLICT:{game.game_id}")
    all_games=pd.concat([canonical,canonical_2026],ignore_index=True).sort_values(["scheduled_start_time_utc","game_id"],kind="stable").reset_index(drop=True)
    team_games=make_team_games(all_games)
    validate_team_games(all_games,team_games)
    state=make_matrix(all_games,team_games,include_scheduled=True)
    matrix=state[state.home_win.notna()].copy()
    output.mkdir(parents=True,exist_ok=True)
    write=lambda d,p: d.to_csv(p,index=False,lineterminator="\n",float_format="%.15g")
    write(all_games,output/"canonical_games.csv"); write(team_games,output/"team_game_history.csv"); write(add_strict_prior(team_games),output/"strict_prior_team_features.csv"); write(matrix,output/"moneyline_training_matrix.csv"); write(state,output/"moneyline_scoring_state.csv")
    output_hashes={name:sha((output/name).read_bytes()) for name in ("canonical_games.csv","team_game_history.csv","strict_prior_team_features.csv","moneyline_training_matrix.csv","moneyline_scoring_state.csv")}
    (output/"reproducibility_hashes.json").write_text(json.dumps(output_hashes,indent=2,sort_keys=True)+"\n")
    season_stats={}
    for season in (2023,2024,2025):
        m=matrix[matrix.season.eq(season)]; complete=m[FEATURES].notna().all(axis=1)
        season_stats[str(season)]={"canonical_games":int(((all_games.season==season)&all_games.regular_season).sum()),"completed_canonical_games":int(((all_games.season==season)&all_games.regular_season&all_games.final_status.isin(["OFF","FINAL"])).sum()),"team_game_rows":int((team_games.season==season).sum()),"training_rows":len(m),"complete_six_feature_rows":int(complete.sum()),"nulls_by_feature":m[FEATURES].isna().sum().to_dict(),"earliest_complete_start_utc":str(m.loc[complete,"scheduled_start_time_utc"].min()) if complete.any() else None,"rows_old_v2_median_imputation_required":int((~complete).sum())}
    metrics_out=fit_eval(matrix[matrix.season.isin([2023,2024,2025])].reset_index(drop=True))
    v2_comparison=compare_frozen_v2(matrix[matrix.season.isin([2023,2024,2025])].reset_index(drop=True))
    manifest={"schema":"NHL_CLEAN_MONEYLINE_TEAM_HISTORY_V1","created_at_utc":datetime.now(timezone.utc).isoformat(),"regular_season_counts":season_stats,"baseline_metrics":metrics_out,"frozen_v2_comparison":v2_comparison,"output_sha256":output_hashes,"current_2026_append":{"sep29_completed_games":int(((all_games.season==2026)&(all_games.game_date=="2026-09-29")&all_games.final_status.isin(["OFF","FINAL"])).sum()),"sep30_scheduled_games":int(((all_games.season==2026)&(all_games.game_date=="2026-09-30")&~all_games.final_status.isin(["OFF","FINAL"])).sum()),"sep30_scheduled_matrix_rows":int(((state.season==2026)&(state.game_date=="2026-09-30")).sum()),"sep30_complete_six_feature_rows":int((state[(state.season==2026)&(state.game_date=="2026-09-30")][FEATURES].notna().all(axis=1)).sum())},"acquisition_accounting":{"new_official_nhl_network_requests":sum(row["method"]=="GET" for row in schedule_ledger+box_ledger),"schedule_responses_preserved":len(schedule_ledger),"boxscore_responses_preserved":len(box_ledger),"failed_requests":0,"paid_requests":0,"2026_sep29_boxscores_reused_from_completed_reconciliation":len(boxes_2026),"sep30_schedule_reused_from_preserved_official_response":True},"raw_schedule_response_count":len(schedule_ledger),"raw_boxscore_count":len(box_ledger),"schedule_request_ledger":schedule_ledger,"boxscore_request_ledger":box_ledger,"data_quality":{"canonical_duplicate_game_ids":int(all_games.game_id.duplicated().sum()),"identity_missing":int(all_games[["home_team_id","away_team_id"]].isna().any(axis=1).sum()),"same_home_away":int((all_games.home_team_id==all_games.away_team_id).sum()),"completed_missing_scores":int((all_games.final_status.isin(["OFF","FINAL"]) & all_games[["home_final_score","away_final_score"]].isna().any(axis=1)).sum()),"negative_shots":int((all_games[["home_shots","away_shots"]]<0).sum().sum()),"scores_over_20":int((all_games[["home_final_score","away_final_score"]]>20).sum().sum()),"team_game_rows_twice_canonical_games":bool(len(team_games)==2*len(all_games)),"team_game_score_winner_orientation_valid":True,"team_game_shot_orientation_valid":True}}
    (output/"build_summary.json").write_text(json.dumps(manifest,indent=2,sort_keys=True,allow_nan=False)+"\n")
    return manifest


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--output-dir",type=Path,default=OUT_DEFAULT)
    parser.add_argument("--workers",type=int,default=8)
    args=parser.parse_args()
    summary=build(args.output_dir,workers=args.workers)
    print(json.dumps({"output_dir":str(args.output_dir),"counts":summary["regular_season_counts"],"data_quality":summary["data_quality"],"baseline_metrics":summary["baseline_metrics"]},indent=2))


if __name__=="__main__": main()
