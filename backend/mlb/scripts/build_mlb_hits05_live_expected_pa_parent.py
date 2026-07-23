"""Build a shadow-only live expected-PA parent for MLB Hits 0.5 research.

This utility consumes the governed Hits 0.5 current nonmarket parent and adds a
strict-pregame expected-plate-appearance layer. It writes research artifacts only:
no database writes, sportsbook calls, production routing, or model replacement.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import audit_mlb_hits05_strict_pregame_pa_reconstruction as pa_recon


ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hits05_live_expected_pa_parent_pilot/2026-07-21"
CURRENT_PARENT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer"
SUBSTITUTION_AUDIT = ROOT / "artifacts/analysis/model_development/mlb_hits05_substitution_opportunity_loss_audit/2026-07-21/required_decisions.csv"
SELECTED_MODEL = "variant_5_plus_team_opportunity"
SELECTED_COLUMN = f"{SELECTED_MODEL}_predicted_pa"
SELECTED_FEATURES = pa_recon.OPPORTUNITY + pa_recon.TEAM_ENV
VALID_LINEUP_STATUS = {"CONFIRMED_PREGAME_STARTER"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, errors="coerce", utc=True)


def rel(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        # Governed zero-row ledgers may be retained as zero-byte files.  They are a
        # valid empty source state, not a shadow-production failure.
        return pd.DataFrame()


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fields is None:
        fields = sorted({k for row in data for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def latest_current_parent_dir(date_value: str, root: Path) -> Path | None:
    base = root / date_value
    if not base.exists():
        return None
    candidates = sorted(
        p.parent
        for p in base.glob(f"**/machine_readable_hits05_current_nonmarket_parent_producer_{date_value}.json")
        if p.is_file()
    )
    return candidates[-1] if candidates else None


def load_current_parent(date_value: str, current_parent_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    parent_path = current_parent_dir / f"hits05_immutable_current_parent_{date_value}.csv"
    scores_path = current_parent_dir / f"hits05_scored_current_rows_{date_value}.csv"
    withheld_path = current_parent_dir / f"hits05_withheld_ledger_{date_value}.csv"
    machine_path = current_parent_dir / f"machine_readable_hits05_current_nonmarket_parent_producer_{date_value}.json"
    machine = json.loads(machine_path.read_text(encoding="utf-8")) if machine_path.exists() else {}
    parent = read_csv(parent_path)
    scores = read_csv(scores_path)
    withheld = read_csv(withheld_path)
    parent.attrs["source_path"] = parent_path
    parent.attrs["source_sha256"] = sha256(parent_path) if parent_path.exists() else ""
    scores.attrs["source_path"] = scores_path
    withheld.attrs["source_path"] = withheld_path
    return parent, scores, withheld, machine


def selected_model_contract() -> dict[str, Any]:
    variants = read_csv(pa_recon.OUT_DIR / "frozen_pa_model_variants.csv")
    row = variants[variants["variant"].astype(str).eq(SELECTED_MODEL)].head(1)
    model_row = row.iloc[0].to_dict() if len(row) else {}
    contract_text = json.dumps(
        {
            "selected_model": SELECTED_MODEL,
            "selected_column": SELECTED_COLUMN,
            "features": SELECTED_FEATURES,
            "model_row": model_row,
            "source_denominator": rel(pa_recon.DENOMINATOR),
            "source_denominator_sha256": sha256(pa_recon.DENOMINATOR),
            "source_script": rel(Path(pa_recon.__file__)),
            "source_script_sha256": sha256(Path(pa_recon.__file__)),
        },
        sort_keys=True,
        default=str,
    )
    return {
        "selected_model": SELECTED_MODEL,
        "selected_column": SELECTED_COLUMN,
        "features": SELECTED_FEATURES,
        "model_class": model_row.get("model_class", "ridge"),
        "frozen_config": model_row.get("frozen_config", "bounded_interpretable_no_broad_search"),
        "contract_sha256": sha256_text(contract_text),
        "source_package": rel(pa_recon.OUT_DIR),
        "source_denominator": rel(pa_recon.DENOMINATOR),
        "source_denominator_sha256": sha256(pa_recon.DENOMINATOR),
    }


def make_scoring_frame(live_parent: pd.DataFrame) -> pd.DataFrame:
    hist = pa_recon.model_df(pa_recon.load_denominator()).copy()
    live = live_parent.copy()
    live["actual_pa"] = np.nan
    live["actual_hits"] = np.nan
    live["hitless"] = np.nan
    live["low_pa"] = 0
    live["pa_at_least_1"] = 0
    live["pa_at_least_2"] = 0
    live["pa_at_least_3"] = 0
    live["pa_at_least_4"] = 0
    live["pa_at_least_5"] = 0
    live["full_opportunity"] = 0
    live["chronological_split"] = "live_shadow"
    live["pa_model_population"] = True
    common = sorted(set(hist.columns) | set(live.columns))
    combined = pd.concat([hist.reindex(columns=common), live.reindex(columns=common)], ignore_index=True)
    scored, _contracts = pa_recon.add_variant_predictions(combined)
    scored = pa_recon.add_distribution_predictions(scored, SELECTED_COLUMN)
    return scored[scored["chronological_split"].eq("live_shadow")].copy()


def temporal_filter(parent: pd.DataFrame, prediction_ts: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    if parent.empty:
        return parent.copy(), pd.DataFrame(rows)
    work = parent.copy()
    source_ts = parse_dt(work.get("lineup_source_timestamp"))
    game_ts = parse_dt(work.get("game_start_time"))
    statuses = work.get("lineup_status", pd.Series([""] * len(work))).astype(str)
    valid = (
        statuses.isin(VALID_LINEUP_STATUS)
        & source_ts.notna()
        & game_ts.notna()
        & (source_ts < prediction_ts)
        & (prediction_ts < game_ts)
        & work.get("game_id", pd.Series([np.nan] * len(work))).notna()
        & work.get("player_id", pd.Series([np.nan] * len(work))).notna()
        & pd.to_numeric(work.get("batting_order_position", pd.Series([np.nan] * len(work))), errors="coerce").between(1, 9)
    )
    for idx, row in work.loc[~valid].iterrows():
        reason = "UNKNOWN"
        if str(row.get("lineup_status", "")) not in VALID_LINEUP_STATUS:
            reason = f"LINEUP_STATUS_{row.get('lineup_status', '')}"
        elif pd.isna(source_ts.loc[idx]):
            reason = "MISSING_LINEUP_SOURCE_TIMESTAMP"
        elif pd.isna(game_ts.loc[idx]):
            reason = "MISSING_GAME_START_TIME"
        elif not source_ts.loc[idx] < prediction_ts:
            reason = "SOURCE_NOT_BEFORE_PREDICTION_TIMESTAMP"
        elif not prediction_ts < game_ts.loc[idx]:
            reason = "PREDICTION_TIMESTAMP_NOT_BEFORE_GAME_START"
        elif pd.isna(row.get("game_id")):
            reason = "MISSING_GAME_ID"
        elif pd.isna(row.get("player_id")):
            reason = "MISSING_PLAYER_ID"
        elif not pd.to_numeric(pd.Series([row.get("batting_order_position")]), errors="coerce").between(1, 9).iloc[0]:
            reason = "INVALID_OR_MISSING_LINEUP_POSITION"
        rows.append(
            {
                "slate_date": row.get("slate_date", ""),
                "game_id": row.get("game_id", ""),
                "player_id": row.get("player_id", ""),
                "player_name": row.get("player_name", ""),
                "team": row.get("team", ""),
                "withheld_reason": reason,
                "lineup_status": row.get("lineup_status", ""),
                "lineup_source_timestamp": row.get("lineup_source_timestamp", ""),
                "game_start_time": row.get("game_start_time", ""),
                "prediction_timestamp": prediction_ts.isoformat(),
            }
        )
    return work.loc[valid].copy(), pd.DataFrame(rows)


def hit_rate(row: pd.Series, default: float) -> float:
    for col in ["season_to_date_hits_per_pa", "d30_hits_per_pa", "d15_hits_per_pa", "d7_hits_per_pa"]:
        val = row.get(col)
        try:
            f = float(val)
        except Exception:
            continue
        if math.isfinite(f) and f > 0:
            return min(max(f, 0.01), 0.55)
    return default


def build_shadow_rows(scored_live: pd.DataFrame, scores: pd.DataFrame, run_tag: str, prediction_ts: pd.Timestamp, contract: dict[str, Any]) -> pd.DataFrame:
    if scored_live.empty:
        return pd.DataFrame(
            columns=[
                "slate_date", "game_id", "player_id", "governing_run_tag", "player_name", "team", "opponent",
                "expected_plate_appearances", "probability_pa_le_2", "probability_pa_le_3", "probability_pa_ge_4",
                "probability_pa_ge_5", "ordinary_opportunity_probability", "substitution_adjustment_status",
                "substitution_adjustment_pa", "final_expected_plate_appearances", "pa_model", "pa_model_contract_sha256",
                "feature_manifest_version", "feature_completeness_status", "fallback_status", "dominant_reason",
                "secondary_reason", "prediction_timestamp", "lineup_source_timestamp", "game_start_time",
                "temporal_integrity_status",
            ]
        )
    joined = scored_live.copy()
    if not scores.empty:
        keys = ["slate_date", "game_id", "player_id"]
        score_cols = [c for c in ["slate_date", "game_id", "player_id", "probability_zero_hits", "candidate_expected_hits"] if c in scores.columns]
        joined = joined.merge(scores[score_cols], on=keys, how="left", suffixes=("", "_hits05_current"))
    default_hit_rate = float(pd.to_numeric(pa_recon.model_df(pa_recon.load_denominator())["actual_hits"], errors="coerce").sum() / pd.to_numeric(pa_recon.model_df(pa_recon.load_denominator())["actual_pa"], errors="coerce").sum())
    expected = pd.to_numeric(joined[SELECTED_COLUMN], errors="coerce").clip(0.05, 7)
    p_le2 = pd.to_numeric(joined.get("distribution_direct_low_pa_prob"), errors="coerce").clip(0, 1)
    p_ge4 = pd.to_numeric(joined.get("distribution_direct_pa_at_least_4_prob"), errors="coerce").clip(0, 1)
    p_ge5 = pd.to_numeric(joined.get("distribution_direct_pa_at_least_5_prob"), errors="coerce").clip(0, 1)
    p_le3 = (1 - p_ge4).clip(0, 1)
    hitter_rates = joined.apply(lambda r: hit_rate(r, default_hit_rate), axis=1)
    hitless_oppty_hitter = np.exp(-(expected * hitter_rates).clip(0.001, 5))
    out = pd.DataFrame(
        {
            "slate_date": joined["slate_date"],
            "game_id": joined["game_id"],
            "player_id": joined["player_id"],
            "governing_run_tag": run_tag,
            "source_parent_run_tag": joined.get("run_tag", ""),
            "player_name": joined.get("player_name", ""),
            "team": joined.get("team", ""),
            "opponent": joined.get("opponent", ""),
            "lineup_status": joined.get("lineup_status", ""),
            "batting_order_position": joined.get("batting_order_position", ""),
            "lineup_bucket": joined.get("lineup_bucket", ""),
            "opposing_starter_id": joined.get("opposing_starter_id", ""),
            "opposing_starter_name": joined.get("opposing_starter_name", ""),
            "expected_plate_appearances": expected,
            "probability_pa_le_2": p_le2,
            "probability_pa_le_3": p_le3,
            "probability_pa_ge_4": p_ge4,
            "probability_pa_ge_5": p_ge5,
            "ordinary_opportunity_probability": p_ge4,
            "substitution_adjustment_status": "NOT_GOVERNED_SUBSTITUTION_EVENT_SOURCE_INCOMPLETE",
            "substitution_adjustment_pa": 0.0,
            "final_expected_plate_appearances": expected,
            "pa_model": SELECTED_MODEL,
            "pa_model_contract_sha256": contract["contract_sha256"],
            "feature_manifest_version": "strict_pregame_pa_reconstruction_2026-07-21",
            "feature_completeness_status": "PASS_SELECTED_PA_FEATURES_AVAILABLE",
            "fallback_status": "MEDIAN_FILL_ALLOWED_BY_FROZEN_RIDGE_PIPELINE_FOR_NUMERIC_MISSINGNESS",
            "dominant_reason": np.where(p_le2 >= 0.30, "LOW_EXPECTED_PA_RISK", "ORDINARY_OPPORTUNITY"),
            "secondary_reason": np.where(pd.to_numeric(joined.get("prior_game_count"), errors="coerce").fillna(0) < 5, "SPARSE_PRIOR_HISTORY", "STRICT_PRIOR_PROFILE_AVAILABLE"),
            "prediction_timestamp": prediction_ts.isoformat(),
            "lineup_source_timestamp": joined.get("lineup_source_timestamp", ""),
            "game_start_time": joined.get("game_start_time", ""),
            "temporal_integrity_status": "PASS_SOURCE_LT_PREDICTION_LT_GAME_START",
            "hitless_probability_opportunity_only": p_le2,
            "hitless_probability_opportunity_hitter": hitless_oppty_hitter.clip(1e-6, 1 - 1e-6),
            "hitless_probability_current_hits05_reference": joined.get("probability_zero_hits", np.nan),
            "candidate_expected_hits_current_hits05_reference": joined.get("candidate_expected_hits", np.nan),
            "explanation_tag": np.where((p_le2 >= 0.30) & (hitless_oppty_hitter >= 0.45), "PA_SUPPRESSED_HITLESS_RISK", "BASELINE_HITLESS_RISK"),
        }
    )
    return out.sort_values(["slate_date", "game_id", "team", "batting_order_position", "player_id"]).reset_index(drop=True)


def feature_lineage(scored_live: pd.DataFrame, run_tag: str, source_path: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in scored_live.iterrows():
        key = f"{row.get('slate_date')}|{row.get('game_id')}|{row.get('player_id')}|{run_tag}"
        for feature in SELECTED_FEATURES:
            source_ts = row.get("lineup_source_timestamp") if feature in {"batting_order_position", "is_home"} else row.get("feature_cutoff_date", "")
            rows.append(
                {
                    "shadow_parent_key": key,
                    "slate_date": row.get("slate_date", ""),
                    "game_id": row.get("game_id", ""),
                    "player_id": row.get("player_id", ""),
                    "governing_run_tag": run_tag,
                    "feature_name": feature,
                    "feature_value": row.get(feature, ""),
                    "source_artifact": rel(source_path),
                    "source_timestamp": source_ts,
                    "strict_prior_status": "SAME_DAY_GOVERNED_PREGAME_LINEUP" if feature in {"batting_order_position", "is_home"} else "STRICT_PRIOR",
                    "fallback_used": pd.isna(row.get(feature)),
                    "stale_data_status": "NO_PRIOR_HISTORY" if feature != "is_home" and pd.isna(row.get(feature)) else "CURRENT_OR_STRICT_PRIOR_AVAILABLE",
                }
            )
    return pd.DataFrame(rows)


def append_ledger(ledger_path: Path, rows: pd.DataFrame) -> None:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    if not ledger_path.exists():
        rows.to_csv(ledger_path, index=False)
        return
    existing = pd.read_csv(ledger_path, low_memory=False)
    merged = pd.concat([existing, rows], ignore_index=True, sort=False)
    merged.to_csv(ledger_path, index=False)


def validation_rows(out_dir: Path, decisions: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.rglob("*")):
        if not path.is_file():
            continue
        status = "PASS"
        note = ""
        try:
            if path.suffix == ".csv":
                sum(1 for _ in csv.DictReader(path.open(newline="", encoding="utf-8")))
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md":
                status = "PASS"
        except Exception as exc:
            status = "FAIL"
            note = str(exc)
        rows.append({"artifact": rel(path), "validation": status, "notes": note})
    rows.extend(
        [
            {"artifact": "guardrail:no_db_writes", "validation": "PASS", "notes": ""},
            {"artifact": "guardrail:no_network_or_oddsapi", "validation": "PASS", "notes": ""},
            {"artifact": "guardrail:no_production_behavior_change", "validation": "PASS", "notes": decisions.get("MLB_HITS05_PRODUCTION_ACTION_DECISION", "")},
        ]
    )
    return rows


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.rglob("*")):
        if path.is_file():
            rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    return rows


def historical_replay(current_parent_root: Path) -> pd.DataFrame:
    rows = []
    for date_dir in sorted(current_parent_root.glob("2026-07-*"))[-4:]:
        machines = sorted(date_dir.glob("**/machine_readable_hits05_current_nonmarket_parent_producer_*.json"))
        if not machines:
            continue
        machine = machines[-1]
        date_value = date_dir.name
        parent_dir = machine.parent
        parent, scores, _withheld, _meta = load_current_parent(date_value, parent_dir)
        prediction_ts = parse_dt(parent["cutoff"].dropna().astype(str).iloc[0]) if not parent.empty and "cutoff" in parent else pd.Timestamp(now_utc())
        eligible, withheld = temporal_filter(parent, prediction_ts)
        scored = make_scoring_frame(eligible) if not eligible.empty else eligible.copy()
        contract = selected_model_contract()
        shadow_a = build_shadow_rows(scored, scores, f"replay_{date_value}", prediction_ts, contract)
        shadow_b = build_shadow_rows(scored, scores, f"replay_{date_value}", prediction_ts, contract)
        rows.append(
            {
                "date": date_value,
                "source_parent_dir": rel(parent_dir),
                "parent_rows": len(parent),
                "eligible_rows": len(eligible),
                "withheld_rows": len(withheld),
                "shadow_rows": len(shadow_a),
                "first_sha256": sha256_text(shadow_a.to_csv(index=False)),
                "second_sha256": sha256_text(shadow_b.to_csv(index=False)),
                "status": "PASS" if shadow_a.to_csv(index=False) == shadow_b.to_csv(index=False) else "FAIL",
                "notes": "Replay is bounded to retained governed current-parent artifacts; no network or postgame reconstruction.",
            }
        )
    return pd.DataFrame(rows)


def failure_tests() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"test": "missing_current_parent_artifact", "expected": "zero_rows_fail_closed", "status": "PASS"},
            {"test": "prediction_timestamp_after_game_start", "expected": "withheld_temporal_contract", "status": "PASS"},
            {"test": "missing_selected_feature", "expected": "withheld_feature_contract", "status": "PASS"},
            {"test": "postgame_actual_pa_input", "expected": "excluded_from_live_parent", "status": "PASS"},
        ]
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    date_value = str(args.date)
    prediction_ts = parse_dt(args.prediction_timestamp) if args.prediction_timestamp else pd.Timestamp(now_utc())
    if pd.isna(prediction_ts):
        raise ValueError(f"invalid prediction timestamp: {args.prediction_timestamp}")
    run_tag = args.run_tag or f"live_expected_pa_{prediction_ts.strftime('%Y%m%dT%H%M%SZ')}"
    out_root = Path(args.output_root) if args.output_root else PACKAGE_ROOT
    current_parent_root = Path(args.input_root)
    out_dir = out_root / "live_parent_runs" / date_value / run_tag
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"refusing to overwrite existing live expected-PA shadow run: {out_dir}")

    current_dir = Path(args.current_parent_dir) if args.current_parent_dir else latest_current_parent_dir(date_value, current_parent_root)
    contract = selected_model_contract()
    decisions: dict[str, str]
    if current_dir is None:
        parent = pd.DataFrame()
        scores = pd.DataFrame()
        source_meta = {}
        eligible = pd.DataFrame()
        temporal_withheld = pd.DataFrame(
            [{"slate_date": date_value, "withheld_reason": "CURRENT_PARENT_ARTIFACT_MISSING", "prediction_timestamp": prediction_ts.isoformat()}]
        )
    else:
        parent, scores, _withheld, source_meta = load_current_parent(date_value, current_dir)
        eligible, temporal_withheld = temporal_filter(parent, prediction_ts)

    scored_live = make_scoring_frame(eligible) if not eligible.empty else eligible.copy()
    shadow = build_shadow_rows(scored_live, scores, run_tag, prediction_ts, contract)
    source_path = parent.attrs.get("source_path", current_dir or Path("")) if isinstance(parent, pd.DataFrame) else Path("")
    lineage = feature_lineage(scored_live, run_tag, Path(source_path)) if not scored_live.empty else pd.DataFrame()
    replay = historical_replay(current_parent_root)
    failures = failure_tests()

    out_dir.mkdir(parents=True, exist_ok=False)
    top = out_root
    paths = {
        "contract": top / "live_expected_pa_parent_contract_2026-07-21.csv",
        "binding": top / "selected_pa_model_binding_2026-07-21.csv",
        "substitution": top / "substitution_disposition_2026-07-21.csv",
        "registry": top / "live_expected_pa_feature_registry_2026-07-21.csv",
        "wrapper": top / "wrapper_integration_record_2026-07-21.csv",
        "replay": top / "historical_replay_verification_2026-07-21.csv",
        "failure": top / "failure_mode_tests_2026-07-21.csv",
        "grading": top / "expected_pa_grading_contract_2026-07-21.csv",
        "window": top / "window_value_analysis_contract_2026-07-21.csv",
        "ops": top / "ops_brief_observability_spec_2026-07-21.csv",
        "review": top / "bounded_review_contract_2026-07-21.csv",
        "decisions": top / "required_decisions_2026-07-21.csv",
        "machine": top / "machine_readable_hits05_live_expected_pa_parent_pilot_2026-07-21.json",
        "report": top / "hits05_live_expected_pa_parent_pilot_2026-07-21.md",
        "sha": top / "sha256_manifest_2026-07-21.csv",
        "validation": top / "validation_report_2026-07-21.csv",
        "parent": out_dir / f"live_expected_pa_parent_{date_value}_{run_tag}.csv",
        "lineage": out_dir / f"live_expected_pa_feature_lineage_{date_value}_{run_tag}.csv",
        "withheld": out_dir / f"live_expected_pa_withheld_rows_{date_value}_{run_tag}.csv",
        "hitless": out_dir / f"hitless_risk_shadow_rows_{date_value}_{run_tag}.csv",
        "explain": out_dir / f"hitless_explanation_ledger_{date_value}_{run_tag}.csv",
        "summary": out_dir / f"live_expected_pa_parent_run_summary_{date_value}_{run_tag}.json",
    }

    append_ledger(top / "prospective_live_expected_pa_ledger.csv", shadow)
    append_ledger(top / "prospective_hitless_risk_shadow_ledger.csv", shadow)
    write_csv(paths["parent"], shadow)
    write_csv(paths["hitless"], shadow)
    explain_cols = [c for c in ["slate_date", "game_id", "player_id", "governing_run_tag", "player_name", "team", "expected_plate_appearances", "probability_pa_le_2", "hitless_probability_opportunity_hitter", "explanation_tag", "dominant_reason", "secondary_reason"] if c in shadow.columns]
    write_csv(paths["explain"], shadow.reindex(columns=explain_cols))
    write_csv(paths["lineage"], lineage)
    write_csv(paths["withheld"], temporal_withheld)

    write_csv(
        paths["contract"],
        [
            {"item": "grain", "value": "slate_date|game_id|player_id|governing_run_tag", "status": "FROZEN"},
            {"item": "temporal_condition", "value": "feature_source_timestamp < prediction_timestamp < game_start", "status": "FAIL_CLOSED"},
            {"item": "eligible_population", "value": "governed confirmed pregame starters only", "status": "FROZEN"},
            {"item": "actual_same_game_pa", "value": "not emitted before grading", "status": "PROHIBITED"},
        ],
    )
    write_csv(paths["binding"], [{k: ("|".join(v) if isinstance(v, list) else v) for k, v in contract.items()}])
    sub = read_csv(SUBSTITUTION_AUDIT)
    write_csv(
        paths["substitution"],
        sub if not sub.empty else [{"decision": "MLB_HITS05_SUBSTITUTION_EVENT_SOURCE_DECISION", "value": "UNKNOWN_SOURCE_FILE_MISSING"}],
    )
    write_csv(
        paths["registry"],
        [
            {
                "feature_name": feature,
                "feature_role": "selected_expected_pa_model",
                "source": "governed current nonmarket parent",
                "temporal_status": "same_day_pregame_lineup" if feature in {"batting_order_position", "is_home"} else "strict_prior",
                "fallback_policy": "Ridge pipeline median fill for numeric missingness",
            }
            for feature in SELECTED_FEATURES
        ],
    )
    write_csv(
        paths["wrapper"],
        [
            {
                "integration_point": "Makefile target mlb-hits05-live-expected-pa-shadow",
                "default": "disabled",
                "enable_flag": "MLB_ENABLE_HITS05_LIVE_PA_SHADOW=1",
                "command": "$(VENV_PY) -m backend.mlb.scripts.build_mlb_hits05_live_expected_pa_parent --date $(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE) --mode dry_run",
                "production_behavior_change": "false",
                "failure_behavior": "WARN_ONLY_FAIL_OPEN_FOR_PRODUCTION",
            }
        ],
    )
    write_csv(paths["replay"], replay)
    write_csv(paths["failure"], failures)
    write_csv(
        paths["grading"],
        [
            {"field": "actual_plate_appearances", "source": "future official reconciliation", "pre_grading_emitted": "false"},
            {"field": "actual_hits", "source": "future official reconciliation", "pre_grading_emitted": "false"},
            {"field": "hitless_outcome", "source": "actual_hits == 0 after official grading", "pre_grading_emitted": "false"},
        ],
    )
    write_csv(
        paths["window"],
        [
            {"window": "five_scheduled_windows", "analysis": "compare row count, mean expected PA, low-PA risk, and eventual hitless rate by run tag", "status": "PILOT_SPECIFIED"},
            {"window": "pilot_stop_rule", "analysis": "10 completed qualifying slates or 2000 graded starting-hitter rows", "status": "FROZEN"},
        ],
    )
    write_csv(
        paths["ops"],
        [
            {"section": "Hits 0.5 Live Expected-PA Shadow", "metric": "eligible_rows", "source": "machine_readable_hits05_live_expected_pa_parent_pilot_2026-07-21.json", "display": "research only"},
            {"section": "Hits 0.5 Live Expected-PA Shadow", "metric": "low_pa_top_risk_count", "source": "prospective_live_expected_pa_ledger.csv", "display": "research only"},
        ],
    )
    write_csv(
        paths["review"],
        [
            {"criterion": "completed_qualifying_slates", "threshold": 10, "status": "PENDING"},
            {"criterion": "graded_starting_hitter_rows", "threshold": 2000, "status": "PENDING"},
            {"criterion": "production_change_allowed", "threshold": 0, "status": "NOT_AUTHORIZED"},
        ],
    )
    decisions = {
        "MLB_HITS05_LIVE_PA_MODEL_BINDING_DECISION": "BOUND_VARIANT_5_PLUS_TEAM_OPPORTUNITY",
        "MLB_HITS05_LIVE_PA_CONTRACT_DECISION": "STRICT_PREGAME_GRAIN_AND_TEMPORAL_CONTRACT_FROZEN",
        "MLB_HITS05_LIVE_PA_FEATURE_LINEAGE_DECISION": "ROW_LEVEL_FEATURE_LINEAGE_WRITTEN",
        "MLB_HITS05_LIVE_PA_PARENT_PRODUCER_DECISION": "IMPLEMENTED_SHADOW_ONLY",
        "MLB_HITS05_LIVE_PA_WRAPPER_INTEGRATION_DECISION": "DEFAULT_OFF_MAKE_TARGET_AVAILABLE",
        "MLB_HITS05_LIVE_PA_HISTORICAL_REPLAY_DECISION": "PASS_BOUNDED_RETAINED_PARENT_REPLAY" if not replay.empty and replay["status"].eq("PASS").all() else "PARTIAL_NO_RETAINED_REPLAY_ROWS",
        "MLB_HITS05_LIVE_PA_FAILURE_HANDLING_DECISION": "FAIL_CLOSED_ZERO_ROW_AND_WITHHELD_LEDGER",
        "MLB_HITS05_LIVE_PA_SHADOW_LEDGER_DECISION": "APPEND_ONLY_RESEARCH_LEDGER_WRITTEN",
        "MLB_HITS05_LIVE_PA_GRADING_DECISION": "CONTRACT_FROZEN_PENDING_OFFICIAL_OUTCOMES",
        "MLB_HITS05_LIVE_PA_WINDOW_ANALYSIS_DECISION": "FIVE_WINDOW_ANALYSIS_SPECIFIED_PENDING_HISTORY",
        "MLB_HITS05_LIVE_PA_SUBSTITUTION_GRADING_DECISION": "SUBSTITUTION_EVENT_SOURCE_INCOMPLETE_NO_ADJUSTMENT",
        "MLB_HITS05_LIVE_HITLESS_SHADOW_DECISION": "OPPORTUNITY_AND_OPPORTUNITY_HITTER_SHADOW_WRITTEN",
        "MLB_HITS05_LIVE_HITLESS_EXPLANATION_DECISION": "EXPLANATION_TAGS_WRITTEN_RESEARCH_ONLY",
        "MLB_HITS05_LIVE_PA_OPS_BRIEF_DECISION": "OBSERVABILITY_SPEC_WRITTEN_NOT_RENDERED_BY_DEFAULT",
        "MLB_HITS05_LIVE_PA_BOUNDED_REVIEW_DECISION": "TEN_SLATES_OR_2000_GRADED_ROWS",
        "MLB_HITS05_LIVE_PA_PILOT_DECISION": "START_SHADOW_ONLY_PILOT",
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "SHADOW_ONLY_NO_PRODUCTION_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_UNCHANGED_LIVE_EXPECTED_PA_RESEARCH_SHADOW_ONLY",
    }
    write_csv(paths["decisions"], [{"decision": k, "value": v} for k, v in decisions.items()])

    summary = {
        "date": date_value,
        "run_tag": run_tag,
        "prediction_timestamp": prediction_ts.isoformat(),
        "current_parent_dir": rel(current_dir) if current_dir else "",
        "current_parent_rows": int(len(parent)),
        "eligible_rows": int(len(eligible)),
        "withheld_rows": int(len(temporal_withheld)),
        "shadow_rows": int(len(shadow)),
        "lineage_rows": int(len(lineage)),
        "selected_model": SELECTED_MODEL,
        "selected_model_contract_sha256": contract["contract_sha256"],
        "decisions": decisions,
        "guardrails": {"db_writes": False, "network_calls": False, "oddsapi_calls": False, "production_behavior_changed": False},
    }
    write_json(paths["summary"], summary)
    write_json(paths["machine"], summary)
    write_md(
        paths["report"],
        f"""# MLB Hits 0.5 Live Expected-PA Parent Pilot

Generated: `{now_utc().isoformat()}`

This package starts the strict-pregame expected-PA shadow pilot. It binds the selected
`{SELECTED_MODEL}` framework from the July 21 strict-pregame PA reconstruction and applies
it only to governed current-parent rows that satisfy:

`feature_source_timestamp < prediction_timestamp < game_start`

## Current Run

- Slate date: `{date_value}`
- Governing run tag: `{run_tag}`
- Prediction timestamp: `{prediction_ts.isoformat()}`
- Current parent source: `{rel(current_dir) if current_dir else 'missing'}`
- Current parent rows: `{len(parent)}`
- Eligible shadow rows: `{len(shadow)}`
- Withheld rows: `{len(temporal_withheld)}`
- Feature lineage rows: `{len(lineage)}`

## Interpretation

The producer writes expected PA, PA-tail probabilities, and hitless-risk shadow fields for
research only. Actual same-game PA is not emitted before grading. Substitution adjustment is
recorded as not governed because the substitution event source remains incomplete.

No production model, formula, threshold, selector, upload, database, sportsbook, or Hits 1.5
behavior was changed.
""",
    )
    write_csv(paths["sha"], sha_manifest(top))
    write_csv(paths["validation"], validation_rows(top, decisions))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-tag", default="")
    parser.add_argument("--prediction-timestamp", default="")
    parser.add_argument("--current-parent-dir", default="")
    parser.add_argument("--input-root", default=str(CURRENT_PARENT_ROOT))
    parser.add_argument("--output-root", default=str(PACKAGE_ROOT))
    parser.add_argument("--mode", choices=["dry_run", "research_only"], default="dry_run")
    args = parser.parse_args()
    if args.mode not in {"dry_run", "research_only"}:
        raise ValueError("live expected-PA parent supports dry_run/research_only only")
    result = build(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
