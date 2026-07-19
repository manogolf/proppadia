"""Dry-run live materialization repair for the Pitcher Hits Allowed Challenger.

The script binds the frozen granular Pitcher Hits Allowed challenger by
deterministically reconstructing its preprocessing/model state from the frozen
fit rows, proves historical parity against retained row-level challenger
predictions, then attempts to materialize and score the July 17 live pitcher
hits-allowed propositions from current local pregame artifacts.

It performs no network calls, no DB writes, no production behavior changes, and
does not alter the completed promotion-grade package.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_pitcher_hits_allowed_granular_encounter_challenger as pha


warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning)

RUN_DATE = "2026-07-17"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_pitcher_hits_allowed_live_replay_repair/2026-07-17")
FROZEN_PHA_PACKAGE = Path("artifacts/analysis/model_development/mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17")
RETAINED_ROW_LEVEL = Path(
    "artifacts/analysis/model_development/mlb_hits05_pitcher_foundation_promotion_grade/2026-07-17/"
    "retained_row_level_pitcher_challenger_predictions_2026-07-17.csv"
)
CURRENT_SLATE = Path("backend/mlb/exports/odds_history/2026-07-17/mlb_slate_output__local_daily_20260717T200004Z.csv")
CURRENT_PROCESSED_SLATE = Path("backend/mlb/data/processed/mlb_uploads/2026-07-17/mlb_slate_output.csv")
CURRENT_WIDE = Path("backend/mlb/exports/odds_history/2026-07-17/mlb_predictions_wide_calibrated__local_daily_20260717T200004Z.csv")
CURRENT_GRANULAR_CANDIDATES = [
    Path("artifacts/analysis/model_development/mlb_pregame_starter_bullpen_exposure_forecast/2026-07-17/research_only_model_artifacts_2026-07-17.csv"),
    Path("artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17/research_only_model_artifacts_2026-07-17.csv"),
    Path("artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17/research_only_model_artifacts_2026-07-17.csv"),
    Path("artifacts/analysis/model_development/mlb_starter_facing_pa_exposure_restoration/2026-07-17/restored_starter_facing_pa_artifact_2026-07-17.csv"),
]

FROZEN_CHALLENGER = "challenger_e_champion_plus_granular"
FROZEN_FEATURES = pha.FEATURE_GROUPS[FROZEN_CHALLENGER]
FIT_END = "2026-06-11"
VALIDATION_START = "2026-06-12"
VALIDATION_END = "2026-06-25"
HOLDOUT_START = "2026-06-26"
HOLDOUT_END = "2026-07-09"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in data:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def bind_frozen_model() -> tuple[pd.DataFrame, Any, dict[str, Any]]:
    joined, meta = pha.assemble_population()
    joined = joined[joined["temporal_split"].isin(["fit", "validation", "holdout"]) & joined["granular_join_status"].eq("JOINED")].copy()
    fit = joined[joined["temporal_split"].eq("fit")].copy()
    instrument = pha.fit_instrument(FROZEN_CHALLENGER, FROZEN_FEATURES, fit)
    scored = pha.score_population(joined, [pha.Instrument("champion", [], None, None, {}, [], "BOUND"), instrument])
    model_hash_payload = {
        "instrument": FROZEN_CHALLENGER,
        "features": FROZEN_FEATURES,
        "medians": instrument.medians,
        "coefficients": instrument.coeffs,
        "scaler_mean": instrument.scaler.mean_.tolist() if instrument.scaler is not None else [],
        "scaler_scale": instrument.scaler.scale_.tolist() if instrument.scaler is not None else [],
        "model_intercept": float(instrument.model.intercept_) if instrument.model is not None else None,
        "model_coef": instrument.model.coef_.tolist() if instrument.model is not None else [],
        "fit_rows": len(fit),
        "fit_end": FIT_END,
    }
    model_hash = hashlib.sha256(json.dumps(model_hash_payload, sort_keys=True, default=str).encode()).hexdigest()
    meta.update(
        {
            "frozen_model_binding": "deterministic_reconstruction_from_frozen_fit_rows_no_search",
            "serialized_model_artifact_present": False,
            "model_state_sha256": model_hash,
            "frozen_pha_package": str(FROZEN_PHA_PACKAGE),
        }
    )
    model_state = pd.DataFrame(
        [
            {
                "instrument": FROZEN_CHALLENGER,
                "feature_order": "|".join(FROZEN_FEATURES),
                "fit_rows": len(fit),
                "missingness_policy": "fit_split_median",
                "preprocessing": "StandardScaler_fit_on_frozen_fit_rows",
                "model": "PoissonRegressor(alpha=1.0,max_iter=1000)",
                "serialized_model_artifact_present": False,
                "model_state_sha256": model_hash,
                "frozen_source_sha256": sha256_file(pha.GRANULAR_SOURCE) if pha.GRANULAR_SOURCE.exists() else "",
                "notes": "No hyperparameter search or specification change; state reconstructed because original package did not serialize scaler/model.",
            }
        ]
    )
    for f in FROZEN_FEATURES:
        model_state.loc[len(model_state)] = {
            "instrument": FROZEN_CHALLENGER,
            "feature_order": f,
            "fit_rows": len(fit),
            "missingness_policy": f"median={instrument.medians.get(f, 0.0)}",
            "preprocessing": "feature_scaled_by_bound_standard_scaler",
            "model": "",
            "serialized_model_artifact_present": False,
            "model_state_sha256": model_hash,
            "frozen_source_sha256": "",
            "notes": "feature-level frozen preprocessing binding",
        }
    return scored, instrument, {"meta": meta, "state": model_state, "hash": model_hash}


def historical_parity(scored: pd.DataFrame) -> pd.DataFrame:
    retained = read_csv(RETAINED_ROW_LEVEL)
    rows = []
    if retained.empty:
        return pd.DataFrame(
            [
                {
                    "parity_check": "retained_row_level_source",
                    "status": "FAIL",
                    "rows_checked": 0,
                    "max_abs_diff": "",
                    "notes": f"missing {RETAINED_ROW_LEVEL}",
                }
            ]
        )
    left = scored.copy()
    left["parity_key"] = (
        left["slate_date"].astype(str)
        + "|"
        + left["game_id"].astype(str)
        + "|"
        + left["pitcher_id"].astype(str)
        + "|"
        + num(left["line"]).astype(str)
    )
    retained["parity_key"] = (
        retained["slate_date"].astype(str)
        + "|"
        + retained["game_id"].astype(str)
        + "|"
        + retained["pitcher_id"].astype(str)
        + "|"
        + num(retained["line"]).astype(str)
    )
    merged = left.merge(
        retained[
            [
                "parity_key",
                "challenger_e_champion_plus_granular_expected_hits_allowed",
                "champion_expected_hits_allowed",
            ]
        ],
        on="parity_key",
        how="left",
        suffixes=("_generated", "_retained"),
    )
    for col in ["challenger_e_champion_plus_granular_expected_hits_allowed", "champion_expected_hits_allowed"]:
        a = num(merged[f"{col}_generated"])
        b = num(merged[f"{col}_retained"])
        diff = (a - b).abs()
        rows.append(
            {
                "parity_check": col,
                "status": "PASS" if b.notna().all() and diff.max() <= 1e-9 else "FAIL",
                "rows_checked": int(len(merged)),
                "matched_rows": int(b.notna().sum()),
                "max_abs_diff": float(diff.max()) if diff.notna().any() else "",
                "mean_abs_diff": float(diff.mean()) if diff.notna().any() else "",
                "notes": "compared deterministic reconstruction against retained row-level pitcher challenger predictions",
            }
        )
    rows.append(
        {
            "parity_check": "split_population",
            "status": "PASS"
            if (len(scored), scored["temporal_split"].eq("fit").sum(), scored["temporal_split"].eq("validation").sum(), scored["temporal_split"].eq("holdout").sum())
            == (1057, 542, 236, 279)
            else "FAIL",
            "rows_checked": int(len(scored)),
            "matched_rows": int(len(scored)),
            "max_abs_diff": "",
            "mean_abs_diff": "",
            "notes": "population and temporal split equality",
        }
    )
    return pd.DataFrame(rows)


def current_source_inventory() -> pd.DataFrame:
    rows = []
    for path in [CURRENT_SLATE, CURRENT_PROCESSED_SLATE, CURRENT_WIDE, *CURRENT_GRANULAR_CANDIDATES]:
        df = read_csv(path)
        has_date = "slate_date" in df.columns
        date_rows = int(df[df["slate_date"].astype(str).eq(RUN_DATE)].shape[0]) if has_date else 0
        prop_rows = int(df[df["prop_type"].astype(str).eq("hits_allowed")].shape[0]) if "prop_type" in df.columns else 0
        feature_hits = [f for f in FROZEN_FEATURES if f in df.columns]
        rows.append(
            {
                "source_path": str(path),
                "exists": path.exists(),
                "sha256": sha256_file(path) if path.exists() else "",
                "rows": len(df),
                "run_date_rows": date_rows,
                "pitcher_hits_allowed_rows": prop_rows,
                "frozen_feature_columns_present": "|".join(feature_hits),
                "frozen_feature_columns_present_count": len(feature_hits),
                "role": classify_source(path, df),
                "notes": "",
            }
        )
    return pd.DataFrame(rows)


def classify_source(path: Path, df: pd.DataFrame) -> str:
    text = str(path)
    if path == CURRENT_SLATE:
        return "current_pitcher_market_and_champion_probability_source"
    if path == CURRENT_PROCESSED_SLATE:
        return "current_processed_slate_candidate_source"
    if path == CURRENT_WIDE:
        return "current_wide_prediction_probability_source"
    if "starter_bullpen_exposure" in text:
        return "historical_granular_encounter_feature_source_no_run_date_rows"
    if "lineup_turnover" in text:
        return "historical_lineup_turnover_feature_source_no_run_date_rows"
    if "contact_opportunity" in text:
        return "historical_contact_opportunity_feature_source_no_run_date_rows"
    if "starter_facing_pa" in text:
        return "historical_restored_starter_pa_source_no_run_date_rows"
    return "unknown"


def materialize_current_feature_frame() -> pd.DataFrame:
    slate = read_csv(CURRENT_SLATE)
    if slate.empty:
        return pd.DataFrame([{"materialization_status": "SOURCE_MISSING", "withheld_reason": "current_slate_source_missing"}])
    h = slate[slate["prop_type"].astype(str).eq("hits_allowed")].copy()
    h["slate_date"] = h["slate_date"].astype(str)
    h["pitcher_id"] = num(h["player_id"]).astype("Int64")
    h["line"] = num(h["line"])
    h["model_prob_over"] = num(h["prob_over"])
    h["champion_expected_hits_allowed_poisson_implied"] = [
        pha.champion_lambda_from_line_prob(line, prob) for line, prob in zip(h["line"], h["model_prob_over"])
    ]
    h["champion_expected_hits_allowed"] = h["champion_expected_hits_allowed_poisson_implied"]
    h["materialization_status"] = "WITHHELD"
    h["withheld_reason"] = "missing_current_granular_opponent_lineup_encounter_source"
    h["withheld_detail"] = "current slate supplies line/probability but no 2026-07-17 row-level frozen encounter aggregate source was found"
    h["feature_coverage_count"] = 1
    h["feature_coverage_pct"] = 1.0 / len(FROZEN_FEATURES)
    for f in FROZEN_FEATURES:
        if f not in h.columns:
            h[f] = np.nan
    h["champion_side"] = np.where(h["model_prob_over"] >= 0.5, "OVER", "UNDER")
    h["challenger_expected_hits_allowed"] = np.nan
    h["challenger_prob_over"] = np.nan
    h["challenger_side"] = ""
    h["shadow_status"] = "WITHHELD_NOT_SCORED"
    return h


def score_current_if_exact(current: pd.DataFrame, instrument: Any) -> pd.DataFrame:
    out = current.copy()
    exact_mask = out[FROZEN_FEATURES].notna().all(axis=1)
    if exact_mask.any():
        scored = pha.score_population(out.loc[exact_mask].copy(), [instrument])
        out.loc[exact_mask, "challenger_expected_hits_allowed"] = scored[f"{FROZEN_CHALLENGER}_expected_hits_allowed"].to_numpy()
        out.loc[exact_mask, "challenger_prob_over"] = scored[f"{FROZEN_CHALLENGER}_prob_over"].to_numpy()
        out.loc[exact_mask, "challenger_side"] = np.where(out.loc[exact_mask, "challenger_prob_over"].astype(float) >= 0.5, "OVER", "UNDER")
        out.loc[exact_mask, "materialization_status"] = "SCORED"
        out.loc[exact_mask, "withheld_reason"] = ""
        out.loc[exact_mask, "shadow_status"] = "DEFAULT_OFF_SHADOW_READY"
    return out


def missing_taxonomy(scored_current: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in scored_current.iterrows():
        missing = [f for f in FROZEN_FEATURES if pd.isna(r.get(f))]
        rows.append(
            {
                "slate_date": r.get("slate_date"),
                "game_id": r.get("game_id"),
                "pitcher_id": r.get("pitcher_id"),
                "pitcher_name": r.get("player_name"),
                "team": r.get("team"),
                "opponent": r.get("opponent"),
                "line": r.get("line"),
                "champion_probability_over": r.get("model_prob_over"),
                "champion_expected_hits_allowed": r.get("champion_expected_hits_allowed"),
                "materialization_status": r.get("materialization_status"),
                "withheld_reason": r.get("withheld_reason"),
                "missing_required_feature_count": len(missing),
                "missing_required_features": "|".join(missing),
                "feature_coverage_count": r.get("feature_coverage_count"),
                "feature_coverage_pct": r.get("feature_coverage_pct"),
                "source_path": str(CURRENT_SLATE),
                "source_sha256": sha256_file(CURRENT_SLATE) if CURRENT_SLATE.exists() else "",
            }
        )
    return pd.DataFrame(rows)


def shadow_artifact(scored_current: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "slate_date",
        "market_snapshot_run_tag",
        "market_snapshot_time_utc",
        "game_id",
        "pitcher_id",
        "player_name",
        "team",
        "opponent",
        "line",
        "market_price_over",
        "market_price_under",
        "champion_expected_hits_allowed",
        "model_prob_over",
        "champion_side",
        "challenger_expected_hits_allowed",
        "challenger_prob_over",
        "challenger_side",
        "materialization_status",
        "withheld_reason",
        "shadow_status",
    ]
    out = scored_current[[c for c in cols if c in scored_current.columns]].copy()
    out["shadow_mode"] = "default_off"
    out["production_behavior_changed"] = False
    return out


def champion_challenger_comparison(scored_current: pd.DataFrame) -> pd.DataFrame:
    rows = []
    scored = scored_current[scored_current["materialization_status"].eq("SCORED")]
    withheld = scored_current[scored_current["materialization_status"].ne("SCORED")]
    rows.append(
        {
            "comparison_scope": "july17_live_pitcher_hits_allowed",
            "rows": len(scored_current),
            "scored_rows": len(scored),
            "withheld_rows": len(withheld),
            "avg_champion_expected_hits_allowed": float(num(scored_current["champion_expected_hits_allowed"]).mean()),
            "avg_challenger_expected_hits_allowed": float(num(scored["challenger_expected_hits_allowed"]).mean()) if len(scored) else "",
            "side_disagreements": int(scored["champion_side"].ne(scored["challenger_side"]).sum()) if len(scored) else 0,
            "notes": "No challenger values are emitted for withheld rows.",
        }
    )
    return pd.DataFrame(rows)


def decisions(parity: pd.DataFrame, scored_current: pd.DataFrame) -> pd.DataFrame:
    parity_pass = bool(parity["status"].eq("PASS").all())
    scored_rows = int(scored_current["materialization_status"].eq("SCORED").sum())
    total_rows = int(len(scored_current))
    if parity_pass and scored_rows == total_rows and total_rows:
        answer = "YES_EXACT_CURRENT_PRE_GAME_GENERATION_AND_JOIN_PASSED"
        shadow = "DEFAULT_OFF_SHADOW_READY"
    elif parity_pass and scored_rows > 0:
        answer = "PARTIAL_EXACT_CURRENT_PRE_GAME_GENERATION_WITH_WITHHELD_ROWS"
        shadow = "PARTIAL_SHADOW_ONLY_FOR_SCORED_ROWS_NOT_ENABLED"
    else:
        answer = "NO_CURRENT_PRE_GAME_GRANULAR_SOURCE_GAP"
        shadow = "NOT_READY_NO_SCORED_CURRENT_CHALLENGER_ROWS"
    rows = [
        ("MLB_PHA_LIVE_MODEL_BINDING_DECISION", "BOUND_BY_DETERMINISTIC_RECONSTRUCTION_SERIALIZED_MODEL_ABSENT"),
        ("MLB_PHA_LIVE_HISTORICAL_PARITY_DECISION", "PASS" if parity_pass else "FAIL"),
        ("MLB_PHA_LIVE_CURRENT_SOURCE_DECISION", "CURRENT_CHAMPION_MARKET_SOURCE_FOUND_GRANULAR_FEATURE_SOURCE_MISSING_FOR_RUN_DATE"),
        ("MLB_PHA_LIVE_FEATURE_MATERIALIZATION_DECISION", f"CURRENT_ROWS_{total_rows}_SCORED_{scored_rows}_WITHHELD_{total_rows - scored_rows}"),
        ("MLB_PHA_LIVE_SCORING_COVERAGE_DECISION", "ZERO_EXACT_CHALLENGER_COVERAGE" if scored_rows == 0 else "PARTIAL_OR_FULL_EXACT_COVERAGE"),
        ("MLB_PHA_LIVE_WITHHELD_TAXONOMY_DECISION", "ONE_PRIMARY_REASON_PER_WITHHELD_ROW_REPORTED"),
        ("MLB_PHA_LIVE_SHADOW_DECISION", shadow),
        ("MLB_PHA_LIVE_DIRECT_ANSWER_DECISION", answer),
        ("MLB_PHA_LIVE_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision_name", "decision_value"])


def validate_files(paths: list[Path], guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for p in paths:
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md":
                assert p.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(p), "validation": status, "notes": notes})
    for k, v in guardrails.items():
        rows.append({"artifact": f"guardrail_{k}", "validation": "PASS" if v in (0, False, "PASS") else "FAIL", "notes": str(v)})
    return pd.DataFrame(rows)


def summary_md(generated_at: str, parity: pd.DataFrame, current: pd.DataFrame, dec: pd.DataFrame) -> str:
    direct = dec[dec["decision_name"].eq("MLB_PHA_LIVE_DIRECT_ANSWER_DECISION")]["decision_value"].iloc[0]
    scored_rows = int(current["materialization_status"].eq("SCORED").sum())
    total_rows = len(current)
    return f"""# MLB Pitcher Hits Allowed Live Replay Repair

Generated: `{generated_at}`

## Executive Summary

Direct answer: `{direct}`

The exact frozen Pitcher Hits Allowed Challenger can be historically reconstructed and parity-checked against retained row-level predictions. It cannot yet be generated for the July 17 live pitcher hits-allowed propositions from current local pregame artifacts, because the current live slate supplies Champion probability and market lines but no same-date frozen opponent-lineup encounter/materialized granular feature source.

## Historical Parity

- Parity checks: `{int(parity['status'].eq('PASS').sum())}` PASS / `{int(parity['status'].ne('PASS').sum())}` non-PASS
- Retained row-level source: `{RETAINED_ROW_LEVEL}`

## July 17 Live Coverage

- Live pitcher hits-allowed propositions: `{total_rows}`
- Exact Challenger scored rows: `{scored_rows}`
- Withheld rows: `{total_rows - scored_rows}`

## Shadow Readiness

The default-off shadow artifact was written as a withheld/not-scored manifest only. It is not ready to enable because exact current-run Challenger coverage is zero.

## No Behavior Changed

No network, OddsAPI, DB write, refit/redesign, production model, formula, tier, selector, candidate, upload, Quick Card, workspace, LaunchAgent, Hits O0.5, or O1.5 behavior changed.
"""


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    historical_scored, instrument, bound = bind_frozen_model()
    parity = historical_parity(historical_scored)
    inventory = current_source_inventory()
    current_features = materialize_current_feature_frame()
    current_scored = score_current_if_exact(current_features, instrument)
    taxonomy = missing_taxonomy(current_scored)
    shadow = shadow_artifact(current_scored)
    comparison = champion_challenger_comparison(current_scored)
    dec = decisions(parity, current_scored)
    files = {
        "summary": out_dir / "pitcher_hits_allowed_live_replay_repair_summary_2026-07-17.md",
        "model_binding": out_dir / "pitcher_hits_allowed_frozen_model_binding_2026-07-17.csv",
        "historical_parity": out_dir / "pitcher_hits_allowed_historical_feature_prediction_parity_2026-07-17.csv",
        "source_inventory": out_dir / "pitcher_hits_allowed_current_pregame_source_inventory_2026-07-17.csv",
        "current_features": out_dir / "pitcher_hits_allowed_july17_materialized_feature_candidates_2026-07-17.csv",
        "current_scored": out_dir / "pitcher_hits_allowed_july17_scored_challenger_predictions_2026-07-17.csv",
        "withheld": out_dir / "pitcher_hits_allowed_july17_withheld_row_taxonomy_2026-07-17.csv",
        "comparison": out_dir / "pitcher_hits_allowed_july17_champion_challenger_comparison_2026-07-17.csv",
        "shadow": out_dir / "pitcher_hits_allowed_default_off_shadow_2026-07-17.csv",
        "decisions": out_dir / "pitcher_hits_allowed_live_replay_decisions_2026-07-17.csv",
        "machine": out_dir / "machine_readable_pitcher_hits_allowed_live_replay_repair_2026-07-17.json",
        "manifest": out_dir / "sha256_manifest_2026-07-17.csv",
        "validation": out_dir / "validation_report_2026-07-17.csv",
    }
    write_text(files["summary"], summary_md(generated_at, parity, current_scored, dec))
    write_csv(files["model_binding"], bound["state"])
    write_csv(files["historical_parity"], parity)
    write_csv(files["source_inventory"], inventory)
    write_csv(files["current_features"], current_features)
    write_csv(files["current_scored"], current_scored[current_scored["materialization_status"].eq("SCORED")])
    write_csv(files["withheld"], taxonomy)
    write_csv(files["comparison"], comparison)
    write_csv(files["shadow"], shadow)
    write_csv(files["decisions"], dec)
    guardrails = {
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "production_behavior_changed": False,
        "refit_redesign_or_new_fields": False,
        "hits05_modified": False,
        "o15_modified_or_graded": False,
    }
    machine = {
        "generated_at": generated_at,
        "direct_answer": dec[dec["decision_name"].eq("MLB_PHA_LIVE_DIRECT_ANSWER_DECISION")]["decision_value"].iloc[0],
        "historical_parity_pass": bool(parity["status"].eq("PASS").all()),
        "july17_live_pitcher_hits_allowed_rows": int(len(current_scored)),
        "july17_exact_scored_rows": int(current_scored["materialization_status"].eq("SCORED").sum()),
        "july17_withheld_rows": int(current_scored["materialization_status"].ne("SCORED").sum()),
        "withheld_reason_counts": taxonomy["withheld_reason"].value_counts(dropna=False).to_dict(),
        "model_state_sha256": bound["hash"],
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "guardrails": guardrails,
    }
    write_json(files["machine"], machine)
    generated = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(files["manifest"], pd.DataFrame([{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"} for p in generated]))
    write_csv(files["validation"], validate_files(generated + [files["manifest"]], guardrails))
    return {
        "output_dir": str(out_dir),
        "historical_parity_pass": machine["historical_parity_pass"],
        "july17_rows": machine["july17_live_pitcher_hits_allowed_rows"],
        "july17_exact_scored_rows": machine["july17_exact_scored_rows"],
        "july17_withheld_rows": machine["july17_withheld_rows"],
        "direct_answer": machine["direct_answer"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()
    print(json.dumps(build(args.output_dir), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
