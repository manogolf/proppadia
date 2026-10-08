#!/usr/bin/env python3
"""Build a read-only, retained-evidence NHL player performance pulse.

The pulse joins eligible pregame scoring states to their final reconciled
outcomes, then to the next eligible pregame state for the same player. It does
not score, train, call providers, or access the database.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import numbers
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
START_DATE = "2026-09-29"
LANES = {
    "sog": {
        "outcome_file": "canonical_skater_outcomes.csv",
        "outcome_col": "official_sog",
        "input_file": "sog_features",
        "prediction_file": "sog_predictions_wide_calibrated.csv",
        "prediction_line_cols": ["p_over_1_5", "p_over_2_5", "p_over_3_5"],
        "feature_candidates": [
            "d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "attempts_d10_per60",
            "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg", "num_sog_last5",
            "num_sog_last10", "num_sog_szn_to_date", "num_event_last5", "num_event_last10",
            "num_event_szn_to_date", "role_pp_share", "last10_team_sog_share",
            "team_d10_sf_per_game", "opp_d10_sf_allowed_per_game", "pace_matchup_index",
            "rest_days", "team_szn_5on5_top_line_xgf_share", "team_5v5_top_line_icetime_share",
            "team_5v5_top_line_shotattempts_share", "d10_top_mate_overlap_share_avg",
            "d10_top3_mates_overlap_share_avg", "d10_shiftcharts_coverage_rate",
            "d20_top_mate_overlap_share_avg", "d20_top3_mates_overlap_share_avg",
            "d20_shiftcharts_coverage_rate", "opp_d10_sf_per60", "team_d10_sa_per60",
            "opp_d10_sa_per60", "szn_toi_per_game_5on5", "szn_toi_per_game_pp",
            "szn_toi_per_game_pk", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game",
            "season_4on5_icetime_per_game", "hot_last5_flag", "is_home",
        ],
        "model_state_features": ["d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "attempts_d10_per60", "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg"],
        "outcome_aliases": ["official_sog", "shots_on_goal"],
    },
    "points": {
        "outcome_file": "canonical_skater_outcomes.csv",
        "outcome_col": "official_points",
        "input_file": "points_scoring_input.csv",
        "prediction_file": "points_predictions.csv",
        "prediction_line_cols": [],
        "feature_candidates": [
            "is_home", "d5_sog_per60", "d10_sog_per60", "attempts_d10_per60",
            "team_d10_sf_per_game", "last10_team_sog_share", "num_shotwasongoal_last5",
            "num_shotwasongoal_last10", "num_shotwasongoal_season_to_date",
            "num_event_shot_last5", "num_event_shot_last10", "num_event_shot_season_to_date",
            "team_num_event_shot_for_last10", "team_num_shotwasongoal_for_last10", "hot_last5_flag",
        ],
        "model_state_features": [
            "d5_sog_per60", "d10_sog_per60", "attempts_d10_per60", "team_d10_sf_per_game",
            "last10_team_sog_share", "num_shotwasongoal_last5", "num_shotwasongoal_last10",
            "num_shotwasongoal_season_to_date", "num_event_shot_last5", "num_event_shot_last10",
            "num_event_shot_season_to_date", "team_num_event_shot_for_last10",
            "team_num_shotwasongoal_for_last10", "hot_last5_flag",
        ],
        "outcome_aliases": ["official_points", "points"],
    },
    "saves": {
        "outcome_file": "canonical_goalie_outcomes.csv",
        "outcome_col": "official_saves",
        "input_file": "saves_scoring_input.csv",
        "prediction_file": "saves_predictions.csv",
        "prediction_line_cols": [],
        "feature_candidates": [
            "is_home", "rest_days", "b2b_flag", "start_prob", "d5_saves_per60",
            "d10_saves_per60", "d5_shots_faced_per60", "season_save_pct", "opponent_id",
            "d10_shots_faced_per60", "d10_save_pct", "team_d10_sf_per_game",
            "opp_d10_sf_allowed_per_game", "pace_index", "opp_d10_sf_per60", "team_d10_sa_per60",
            "pace_matchup_index", "d20_saves_per60", "team_d10_sf_per60", "opp_d10_sa_per60",
        ],
        "model_state_features": [
            "is_home", "rest_days", "b2b_flag", "start_prob", "d5_saves_per60",
            "d10_saves_per60", "d5_shots_faced_per60", "season_save_pct", "opponent_id",
        ],
        "outcome_aliases": ["official_saves", "saves"],
    },
}


def normalize_json_value(value: Any) -> Any:
    """Convert pandas/numpy missing and non-finite scalars to JSON null values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, dict):
        return {key: normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_json_value(item) for item in value]
    if isinstance(value, np.ndarray):
        return normalize_json_value(value.tolist())
    if isinstance(value, np.generic):
        return normalize_json_value(value.item())
    if isinstance(value, numbers.Real):
        return float(value) if math.isfinite(float(value)) else None
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        missing = False
    if isinstance(missing, (bool, np.bool_)) and missing:
        return None
    return value


def strict_json_dumps(value: Any, **kwargs: Any) -> str:
    """Serialize pulse JSON after normalization, rejecting non-standard tokens."""
    return json.dumps(normalize_json_value(value), allow_nan=False, **kwargs)


def write_json(path: Path, value: Any, **kwargs: Any) -> None:
    Path(path).write_text(strict_json_dumps(value, **kwargs) + "\n")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve(path_text: str) -> Path:
    p = Path(path_text)
    if p.is_absolute():
        # Receipts come from the author's machine; map their repo-rooted path
        # onto this checkout while retaining absolute-path support elsewhere.
        marker = "/proppadia/"
        if marker in path_text:
            return ROOT / path_text.split(marker, 1)[1]
    return p if p.is_absolute() else ROOT / p


def find_outcome(date: str, filename: str) -> Path | None:
    base = ROOT / "artifacts/operational/nhl/postgame_reconciliation" / date
    found = sorted(base.glob(f"reconciliation=*/{filename}")) if base.exists() else []
    # A date can have restatements. Select the latest materialized package;
    # every referenced file is hashed into the resulting pulse evidence.
    return max(found, key=lambda x: x.stat().st_mtime_ns) if found else None


def parse_start(row: pd.Series) -> pd.Timestamp | None:
    value = row.get("game_start_utc")
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    return pd.to_datetime(value, utc=True, errors="coerce")


def normalize_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().map({"true": 1.0, "t": 1.0, "1": 1.0, "false": 0.0, "f": 0.0, "0": 0.0})


def run_records() -> list[tuple[Path, dict[str, Any]]]:
    records = []
    root = ROOT / "artifacts/operational/nhl/daily_runs"
    for receipt_path in sorted(root.glob("run_id=*/parent_receipt.json")):
        try:
            receipt = json.loads(receipt_path.read_text())
            date = str(receipt.get("slate_date", ""))
            if date >= START_DATE and receipt.get("final_classification") == "READY":
                records.append((receipt_path, receipt))
        except (OSError, json.JSONDecodeError):
            continue
    return records


def load_outcomes(date: str, lane: str, audit: dict[str, Any]) -> pd.DataFrame:
    cfg = LANES[lane]
    p = find_outcome(date, cfg["outcome_file"])
    if not p:
        return pd.DataFrame()
    d = pd.read_csv(p)
    if lane == "saves":
        if "actual_start_flag" in d:
            d = d[d.actual_start_flag.astype(str).str.lower().isin(["true", "t", "1"])]
        elif "participation_state" in d:
            d = d[d.participation_state.astype(str).str.upper().isin(["STARTED", "PARTICIPATED"])]
    if "official_final" in d:
        d = d[d.official_final.astype(str).str.lower().isin(["true", "t", "1"])]
    if "game_type_code" in d:
        d = d[d.game_type_code.astype(str).str.strip().isin(["2", "2.0"])]
    elif "canonical_game_outcomes.csv" in cfg["outcome_file"]:
        pass
    d["game_id"] = pd.to_numeric(d.game_id, errors="coerce")
    player_col = "goalie_id" if lane == "saves" and "goalie_id" in d else "player_id"
    if player_col not in d:
        return pd.DataFrame()
    d["player_id"] = pd.to_numeric(d[player_col], errors="coerce")
    d["outcome_value"] = pd.to_numeric(d.get(cfg["outcome_col"]), errors="coerce")
    d["outcome_date"] = date
    d["outcome_path"] = str(p.relative_to(ROOT))
    d["outcome_sha256"] = sha256(p)
    d["outcome_source_timestamp_utc"] = d.get("outcome_source_timestamp_utc", "")
    return d.dropna(subset=["game_id", "player_id", "outcome_value"])


def collect_states(lane: str, records: list[tuple[Path, dict[str, Any]]], audit: dict[str, Any]) -> pd.DataFrame:
    cfg = LANES[lane]
    grouped: dict[tuple[str, int, int], dict[str, Any]] = {}
    for receipt_path, receipt in records:
        date = str(receipt["slate_date"])
        lane_rec = receipt.get("lanes", {}).get("points" if lane == "points" else "saves" if lane == "saves" else "legacy_sog", {})
        if lane_rec.get("status") != "COMPLETE":
            continue
        inputs = lane_rec.get("inputs", [])
        outputs = lane_rec.get("outputs", [])
        input_entry = next((x for x in inputs if str(x.get("path", "")).endswith(cfg["input_file"])), None)
        game_start_lookup: dict[int, Any] = {}
        if lane == "sog":
            input_entry = None
            point_entry = next((x for x in receipt.get("lanes", {}).get("points", {}).get("inputs", []) if str(x.get("path", "")).endswith("points_scoring_input.csv")), None)
            if point_entry:
                point_path = resolve(str(point_entry.get("path", "")))
                if point_path.exists():
                    point_state = pd.read_csv(point_path, usecols=["game_id", "game_start_utc"])
                    game_start_lookup = dict(zip(pd.to_numeric(point_state.game_id, errors="coerce").dropna().astype(int), point_state.game_start_utc))
            for c in receipt.get("child_summaries", []):
                cmd = c.get("command_identity", "")
                m = re.search(r"--in\s+(\S+sog_features_" + re.escape(date) + r"_denali\.csv)", cmd)
                if m and c.get("exit_status") == 0:
                    p = resolve(m.group(1))
                    if p.exists():
                        input_entry = {"path": str(p), "receipt_input_hash_bound": False}
                        break
        prediction_entry = next((x for x in outputs if str(x.get("path", "")).endswith(cfg["prediction_file"])), None)
        if not input_entry or not prediction_entry:
            continue
        inp = resolve(str(input_entry["path"]))
        pred = resolve(str(prediction_entry["path"]))
        if not inp.exists() or not pred.exists():
            audit["missing_artifacts"] += 1
            continue
        declared_input_hash = input_entry.get("sha256")
        declared_pred_hash = prediction_entry.get("sha256")
        input_hash = sha256(inp)
        pred_hash = sha256(pred)
        if (declared_input_hash and input_hash != declared_input_hash) or (declared_pred_hash and pred_hash != declared_pred_hash):
            audit["hash_mismatches"] += 1
            continue
        d = pd.read_csv(inp)
        p = pd.read_csv(pred)
        if not {"game_id", "player_id"}.issubset(d.columns):
            continue
        d["game_id"] = pd.to_numeric(d.game_id, errors="coerce")
        d["player_id"] = pd.to_numeric(d.player_id, errors="coerce")
        if "game_start_utc" not in d.columns and game_start_lookup:
            d["game_start_utc"] = d.game_id.map(game_start_lookup)
        d["game_start"] = pd.to_datetime(d["game_start_utc"], utc=True, errors="coerce") if "game_start_utc" in d.columns else pd.Series(pd.NaT, index=d.index, dtype="datetime64[ns, UTC]")
        cutoff = pd.to_datetime(input_entry.get("feature_input_cutoff_utc"), utc=True, errors="coerce") if input_entry.get("feature_input_cutoff_utc") else pd.NaT
        if pd.isna(cutoff) and "feature_input_cutoff_utc" in d.columns and len(d):
            cutoff = pd.to_datetime(d.feature_input_cutoff_utc.iloc[0], utc=True, errors="coerce")
        if lane == "sog":
            cutoff = pd.to_datetime(lane_rec.get("ended_at_utc"), utc=True, errors="coerce")
        d["feature_cutoff"] = cutoff
        d["lane_end"] = pd.to_datetime(lane_rec.get("ended_at_utc"), utc=True, errors="coerce")
        d["eligible_pregame"] = d.game_start.notna() & d.feature_cutoff.notna() & d.lane_end.notna() & (d.feature_cutoff < d.game_start) & (d.lane_end < d.game_start)
        d = d[d.eligible_pregame].dropna(subset=["game_id", "player_id"])
        if d.empty:
            continue
        p["game_id"] = pd.to_numeric(p.get("game_id"), errors="coerce")
        p["player_id"] = pd.to_numeric(p.get("player_id"), errors="coerce")
        keys = ["game_id", "player_id"]
        if "line" in p.columns:
            p["line"] = pd.to_numeric(p.line, errors="coerce")
            p = p.sort_values("line").drop_duplicates(keys + ["line"])
            ladder_rows = []
            for identity, ladder_group in p.groupby(keys, sort=False):
                ladder_rows.append({"game_id": identity[0], "player_id": identity[1], "prediction_ladder": strict_json_dumps([{"line": _jsonval(r.get("line")), "prob_over": _jsonval(r.get("prob_over"))} for _, r in ladder_group.iterrows()])})
            probs = pd.DataFrame(ladder_rows)
            d = d.merge(probs[keys + ["prediction_ladder"]], on=keys, how="left")
        elif lane in ("sog", "saves"):
            ladder_rows = []
            for _, prediction in p.iterrows():
                columns = cfg["prediction_line_cols"] if lane == "sog" else [c for c in p.columns if c.startswith("p_over_")]
                ladder_rows.append({"game_id": prediction.game_id, "player_id": prediction.player_id, "prediction_ladder": strict_json_dumps([{"line": float(col.removeprefix("p_over_").replace("_", ".")), "prob_over": _jsonval(prediction.get(col))} for col in columns if col in p.columns])})
            d = d.merge(pd.DataFrame(ladder_rows), on=keys, how="left")
        else:
            prob_col = next((c for c in ["prob_over", "prob_over_1.5", "expected_saves"] if c in p.columns), None)
            if prob_col:
                p = p.sort_values("line" if "line" in p.columns else keys).drop_duplicates(keys)
                d = d.merge(p[keys + [prob_col]], on=keys, how="left")
                d["prediction_ladder"] = d[prob_col].map(lambda x: strict_json_dumps([{"line": None, "prob_over": _jsonval(x)}]))
            else:
                d["prediction_ladder"] = "[]"
        for _, row in d.iterrows():
            k = (str(date), int(row.game_id), int(row.player_id))
            name = row.get("full_name", row.get("player_name", ""))
            candidate = {
                "lane": lane, "slate_date": date, "game_id": int(row.game_id), "player_id": int(row.player_id),
                "player_name": "" if pd.isna(name) else str(name), "team_id": _jsonval(row.get("team_id")),
                "opponent_id": _jsonval(row.get("opponent_id")), "game_start_utc": _iso(row.game_start),
                "feature_cutoff_utc": _iso(row.feature_cutoff), "lane_end_utc": _iso(row.lane_end),
                "parent_daily_run_id": receipt.get("parent_daily_run_id"), "feature_path": str(inp.relative_to(ROOT)),
                "feature_sha256": input_hash, "feature_hash_bound_by_receipt": bool(declared_input_hash),
                "prediction_path": str(pred.relative_to(ROOT)), "prediction_sha256": pred_hash,
                "prediction_ladder": row.get("prediction_ladder", "[]"),
                "model_family_version": _model_identity(lane),
                "feature_values": {c: _featureval(c, row.get(c)) for c in cfg["feature_candidates"] if c in d.columns},
            }
            if candidate["team_id"] is None and "home_team_id" in row and "away_team_id" in row:
                home = str(row.get("is_home", "")).lower() in ("1", "true", "t")
                candidate["team_id"] = _jsonval(row.get("home_team_id" if home else "away_team_id"))
            old = grouped.get(k)
            if old is None or (candidate["feature_cutoff_utc"] or "") > (old["feature_cutoff_utc"] or ""):
                grouped[k] = candidate
        audit["eligible_state_rows"] += len(d)
    return pd.DataFrame(grouped.values())


def _model_identity(lane: str) -> str:
    if lane == "sog": return "poisson_baseline/baseline_v1 (historic fitted artifact hash not receipt-bound)"
    if lane == "points": return "phoenix/phoenix_v2 (per-line logistic; historic fitted artifact hash not receipt-bound)"
    return "phoenix_v2 goalie_saves (Poisson + eligible line calibration; historic fitted artifact hash not receipt-bound)"


def _jsonval(v: Any) -> Any:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) or pd.isna(v): return None
    if isinstance(v, (np.integer,)): return int(v)
    if isinstance(v, (np.floating,)): return float(v)
    if isinstance(v, (np.bool_,)): return bool(v)
    return v


def _iso(v: Any) -> str | None:
    if v is None or pd.isna(v): return None
    return pd.Timestamp(v).isoformat().replace("+00:00", "Z")


def stats_summary(values: list[float]) -> dict[str, Any]:
    s = pd.Series(values, dtype="float64").dropna()
    if s.empty: return {"count": 0, "median_absolute_movement": None, "iqr": None, "p90": None, "p95": None, "unchanged_rate": None}
    a = s.abs()
    return {"count": int(len(s)), "median_absolute_movement": float(a.median()), "iqr": float(a.quantile(.75)-a.quantile(.25)), "p90": float(a.quantile(.90)), "p95": float(a.quantile(.95)), "unchanged_rate": float((s == 0).mean())}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--through-date", default="2026-10-06")
    ap.add_argument("--output-dir", default="artifacts/analysis/nhl/player_performance_pulse/2026-10-07")
    args = ap.parse_args()
    out = Path(args.output_dir)
    if not out.is_absolute(): out = ROOT / out
    if out.exists(): raise SystemExit(f"Refusing to overwrite existing pulse package: {out}")
    out.mkdir(parents=True, exist_ok=False)
    records = [(p, r) for p, r in run_records() if START_DATE <= str(r["slate_date"]) <= args.through_date]
    audit = {"missing_artifacts": 0, "hash_mismatches": 0, "eligible_state_rows": 0}
    states = {lane: collect_states(lane, records, audit) for lane in LANES}
    rolling_checks = independent_skater_window_checks(states, args.through_date)
    transitions: list[dict[str, Any]] = []
    movements: list[dict[str, Any]] = []
    rolling_audits: list[dict[str, Any]] = []
    availability: dict[str, Any] = {}
    for lane, frame in states.items():
        if frame.empty:
            availability[lane] = {"pregame_state_rows": 0, "transitions": 0, "dates_available": []}
            continue
        outcomes = pd.concat([load_outcomes(date, lane, audit) for date in sorted(frame.slate_date.unique())], ignore_index=True)
        if not outcomes.empty:
            outcomes = outcomes.sort_values("outcome_source_timestamp_utc").drop_duplicates(["game_id", "player_id"], keep="last")
            ocols = [c for c in ["game_id", "player_id", "outcome_value", "outcome_date", "outcome_path", "outcome_sha256", "outcome_source_timestamp_utc", "participation_state", "official_final"] if c in outcomes]
            frame = frame.merge(outcomes[ocols], on=["game_id", "player_id"], how="left")
        frame["game_start_ts"] = pd.to_datetime(frame.game_start_utc, utc=True, errors="coerce")
        frame["cutoff_ts"] = pd.to_datetime(frame.feature_cutoff_utc, utc=True, errors="coerce")
        frame = frame.sort_values(["player_id", "game_start_ts", "game_id"])
        lane_transitions = 0
        feat_keys = set().union(*(x.keys() for x in frame.feature_values))
        for player_id, group in frame.groupby("player_id", sort=False):
            rows = list(group.to_dict("records"))
            for ix, cur in enumerate(rows[:-1]):
                nxt = rows[ix+1]
                if cur.get("outcome_value") is None or pd.isna(cur.get("outcome_value")):
                    continue
                realized_at = pd.to_datetime(cur.get("outcome_source_timestamp_utc"), utc=True, errors="coerce")
                next_cutoff = pd.to_datetime(nxt.get("cutoff_ts"), utc=True, errors="coerce")
                next_start = pd.to_datetime(nxt.get("game_start_ts"), utc=True, errors="coerce")
                if pd.isna(realized_at) or pd.isna(next_cutoff) or realized_at >= next_cutoff or next_cutoff >= next_start:
                    continue
                prevvals = cur.get("feature_values") or {}; nowvals = nxt.get("feature_values") or {}
                p_ladder = _ladder(cur.get("prediction_ladder")); n_ladder = _ladder(nxt.get("prediction_ladder"))
                rec = {
                    "lane": lane, "player_id": int(player_id), "player_name": nxt.get("player_name") or cur.get("player_name"),
                    "team_id_game_n": cur.get("team_id"), "team_id_game_n1": nxt.get("team_id"),
                    "game_n_id": cur["game_id"], "game_n_date": cur["slate_date"], "game_n1_id": nxt["game_id"], "game_n1_date": nxt["slate_date"],
                    "elapsed_days": (pd.Timestamp(nxt["slate_date"])-pd.Timestamp(cur["slate_date"])).days,
                    "realized_stat": cur.get("outcome_value"), "outcome_source_timestamp_utc": cur.get("outcome_source_timestamp_utc"),
                    "outcome_source_path": cur.get("outcome_path"), "outcome_source_sha256": cur.get("outcome_sha256"),
                    "feature_state_prior": strict_json_dumps(prevvals, sort_keys=True), "feature_state_current": strict_json_dumps(nowvals, sort_keys=True),
                    "prediction_prior": strict_json_dumps(p_ladder, sort_keys=True), "prediction_current": strict_json_dumps(n_ladder, sort_keys=True),
                    "model_family_version": nxt.get("model_family_version"),
                    "feature_artifact_identity_prior": cur.get("feature_path"), "feature_artifact_sha256_prior": cur.get("feature_sha256"),
                    "feature_artifact_identity_current": nxt.get("feature_path"), "feature_artifact_sha256_current": nxt.get("feature_sha256"),
                    "prediction_artifact_identity_prior": cur.get("prediction_path"), "prediction_artifact_sha256_prior": cur.get("prediction_sha256"),
                    "prediction_artifact_identity_current": nxt.get("prediction_path"), "prediction_artifact_sha256_current": nxt.get("prediction_sha256"),
                    "feature_hash_receipt_bound_prior": cur.get("feature_hash_bound_by_receipt"), "feature_hash_receipt_bound_current": nxt.get("feature_hash_bound_by_receipt"),
                }
                transitions.append(rec); lane_transitions += 1
                for feature in sorted(feat_keys):
                    a = _num(prevvals.get(feature)); b = _num(nowvals.get(feature))
                    if a is None and b is None: continue
                    delta = None if a is None or b is None else b-a
                    movements.append({"lane": lane, "player_id": int(player_id), "game_n_id": cur["game_id"], "game_n1_id": nxt["game_id"], "feature": feature, "prior_value": a, "current_value": b, "signed_change": delta, "absolute_change": abs(delta) if delta is not None else None, "percent_change": (delta/abs(a)*100) if delta is not None and a != 0 else None, "standardized_movement": None, "movement_status": "MISSING_TO_PRESENT" if a is None else "PRESENT_TO_MISSING" if b is None else "VALUE"})
                for feature in LANES[lane]["model_state_features"]:
                    a = _num(prevvals.get(feature)); b = _num(nowvals.get(feature))
                    rolling_audits.append({"lane": lane, "player_id": int(player_id), "game_n_id": cur["game_id"], "game_n1_id": nxt["game_id"], "feature": feature, "prior_value": a, "current_value": b, "realized_game_n_value": _jsonval(cur.get("outcome_value")), "audit_status": "PRODUCTION_STATE_COMPARISON_ONLY"})
        availability[lane] = {"pregame_state_rows": int(len(frame)), "transitions": lane_transitions, "dates_available": sorted(frame.slate_date.unique().tolist()), "outcome_join_rows": int(frame.outcome_value.notna().sum()) if "outcome_value" in frame else 0}
    # Standardize movement by feature/lane across the observed window; never pool unlike fields.
    movement_df = pd.DataFrame(movements)
    if not movement_df.empty:
        for (lane, feature), idx in movement_df.groupby(["lane", "feature"]).groups.items():
            s = pd.to_numeric(movement_df.loc[idx, "signed_change"], errors="coerce")
            sd = s.std(ddof=1)
            if len(s.dropna()) >= 2 and sd and sd > 0:
                movement_df.loc[idx, "standardized_movement"] = (s - s.mean()) / sd
        lane_summary = {}
        for (lane, feature), group in movement_df.groupby(["lane", "feature"]):
            delta = pd.to_numeric(group.absolute_change, errors="coerce")
            present = group.movement_status.eq("VALUE")
            lane_summary[f"{lane}.{feature}"] = {**stats_summary(delta[present].tolist()), "missing_to_present": int(group.movement_status.eq("MISSING_TO_PRESENT").sum()), "present_to_missing": int(group.movement_status.eq("PRESENT_TO_MISSING").sum())}
        movement_df.to_csv(out / "feature_movements.csv", index=False)
    else:
        lane_summary = {}
    trans_df = pd.DataFrame(transitions)
    trans_df.to_csv(out / "player_state_transitions.csv", index=False)
    pd.DataFrame(rolling_audits).to_csv(out / "rolling_state_audit.csv", index=False)
    pd.DataFrame(rolling_checks).to_csv(out / "independent_rolling_checks.csv", index=False)
    model_inventory = {
        "sog": {"dynamic_player_performance": LANES["sog"]["model_state_features"], "dynamic_context_present_but_not_consumed_by_baseline": ["role_pp_share", "pairings", "team_context"], "static_descriptors": ["player_id"], "parameters": "Frozen Poisson baseline; historical fitted artifact bytes not receipt-bound."},
        "points": {"dynamic_player_performance": [x for x in LANES["points"]["model_state_features"] if not x.startswith("team_") and x not in ["is_home"]], "dynamic_context": ["is_home", "team_d10_sf_per_game", "last10_team_sog_share", "team_num_event_shot_for_last10", "team_num_shotwasongoal_for_last10"], "static_descriptors": ["player_id"], "parameters": "Frozen per-line logistic Phoenix V2; historical fitted artifact bytes not receipt-bound."},
        "saves": {"dynamic_player_performance": ["d5_saves_per60", "d10_saves_per60", "d5_shots_faced_per60", "season_save_pct", "rest_days", "b2b_flag"], "dynamic_context": ["is_home", "opponent_id"], "conditional_start_treatment": "start_prob is operational constant 1.0 conditional-start flag, not estimated probability; actual start is postgame outcome only.", "static_descriptors": ["player_id"], "parameters": "Frozen Phoenix V2 Poisson coefficients with eligible line calibration; historical fitted artifact bytes not receipt-bound."},
    }
    summary = {
        "schema_version": "NHL_PLAYER_PERFORMANCE_PULSE_V1", "window_start": START_DATE, "through_date": args.through_date,
        "regular_season_only": True, "read_only": True, "does_not_retrain": True, "inventory": model_inventory,
        "availability": availability, "artifact_audit": audit, "transition_count": len(transitions),
        "feature_movement_summary": lane_summary,
        "independent_rolling_check_counts": pd.Series([r["status"] for r in rolling_checks], dtype="string").value_counts().to_dict(),
        "limits": ["This attachment ends mid-sentence after section 6; omitted later requirements could not be applied.", "Historical model parameter hashes are not bound to these run receipts, so parameter stability is not inferred.", "SOG feature input hashes are not receipt-bound; its input is identified by the successful scoring command and retrospectively hashed.", "Independent rolling recomputation is limited to retained official regular-season outcome rows since 2026-09-29; unresolved differences may reflect shorter audit history versus production logs that include earlier games and, for some fields, preseason appearances. Goalie windows are not independently recomputed."],
    }
    write_json(out / "summary.json", summary, indent=2, sort_keys=True)
    return 0


def _num(v: Any) -> float | None:
    if v is None or pd.isna(v): return None
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError): return None


def _featureval(name: str, value: Any) -> Any:
    if name in {"hot_last5_flag", "is_home", "b2b_flag"}:
        mapped = normalize_bool(pd.Series([value])).iloc[0]
        if not pd.isna(mapped): return float(mapped)
    return _jsonval(value)


def independent_skater_window_checks(states: dict[str, pd.DataFrame], through: str) -> list[dict[str, Any]]:
    """Recompute strict-prior skater windows from retained official outcomes."""
    outcome_frames = []
    for date_dir in sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob("2026-*")):
        date = date_dir.name
        if START_DATE <= date <= through:
            p = find_outcome(date, "canonical_skater_outcomes.csv")
            if p:
                part = pd.read_csv(p)
                if "official_final" in part:
                    part = part[part.official_final.astype(str).str.lower().isin(["true", "t", "1"])]
                part["game_id"] = pd.to_numeric(part.game_id, errors="coerce")
                part["player_id"] = pd.to_numeric(part.player_id, errors="coerce")
                part = part[part.game_id.astype("Int64").astype(str).str.slice(4, 6).eq("02")]
                part["official_sog"] = pd.to_numeric(part.official_sog, errors="coerce")
                part["toi_minutes_num"] = part.toi.map(_toi_minutes)
                part["slate_date"] = date
                outcome_frames.append(part)
    if not outcome_frames:
        return []
    logs = pd.concat(outcome_frames, ignore_index=True).dropna(subset=["game_id", "player_id"])
    checks = []
    for lane in ("sog", "points"):
        frame = states.get(lane, pd.DataFrame())
        if frame.empty: continue
        for state in frame.to_dict("records"):
            prior = logs[(logs.player_id == state["player_id"]) & (logs.slate_date < state["slate_date"])].sort_values(["slate_date", "game_id"], ascending=False)
            for n in (5, 10, 20):
                field = f"d{n}_sog_per60"
                if field not in LANES[lane]["feature_candidates"]: continue
                win = prior.head(n)
                eligible = win.dropna(subset=["toi_minutes_num"])
                expected = (eligible.official_sog * 60.0 / eligible.toi_minutes_num).mean() if len(eligible) else None
                observed = _num((state.get("feature_values") or {}).get(field))
                if expected is None or observed is None:
                    status = "UNAVAILABLE_VALUE"
                else:
                    match = abs(expected-observed) <= 1e-5
                    status = ("MATCH_PARTIAL_WINDOW" if len(win) < n else "MATCH") if match else "UNRESOLVED_SOURCE_COVERAGE"
                checks.append({"lane": lane, "player_id": state["player_id"], "game_id": state["game_id"], "slate_date": state["slate_date"], "feature": field, "window_games_expected": n, "retained_prior_games": int(len(win)), "expected_value": expected, "production_value": observed, "absolute_error": abs(expected-observed) if expected is not None and observed is not None else None, "status": status})
            for n in (5, 10):
                field = "num_sog_last" + str(n) if lane == "sog" else "num_shotwasongoal_last" + str(n)
                win = prior.head(n)
                expected = float(win.official_sog.sum()) if len(win) else 0.0
                observed = _num((state.get("feature_values") or {}).get(field))
                status = ("MATCH_PARTIAL_WINDOW" if len(win) < n else "MATCH") if observed is not None and abs(expected-observed) <= 1e-8 else "UNRESOLVED_SOURCE_COVERAGE" if observed is not None else "UNAVAILABLE_VALUE"
                checks.append({"lane": lane, "player_id": state["player_id"], "game_id": state["game_id"], "slate_date": state["slate_date"], "feature": field, "window_games_expected": n, "retained_prior_games": int(len(win)), "expected_value": expected, "production_value": observed, "absolute_error": abs(expected-observed) if observed is not None else None, "status": status})
    return checks


def _toi_minutes(value: Any) -> float | None:
    if value is None or pd.isna(value): return None
    s = str(value).strip()
    try:
        if ":" in s:
            m, sec = s.split(":", 1)
            return int(m) + int(sec) / 60.0
        return float(s)
    except (TypeError, ValueError): return None


def _ladder(v: Any) -> Any:
    try: return json.loads(v) if isinstance(v, str) else []
    except json.JSONDecodeError: return []


if __name__ == "__main__":
    raise SystemExit(main())
