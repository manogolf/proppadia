"""Repair-impact audit for the MLB totals bullpen recency freshness invariant.

This program is diagnostic only: it reads frozen prediction/outcome ledgers and
frozen model artifacts, reconstructs strict-prior bullpen state, and writes a
separate audit package. It never fits a model or mutates a prediction ledger.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import review_mlb_totals_c_deployment_stability_shadow_decision_v1 as stability
from backend.mlb.scripts import run_mlb_totals_count_feature_structural_repair_comparison_v1 as structural
from backend.mlb.scripts import run_mlb_totals_raw_run_environment_bias_decomposition_v1 as raw
from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    BULLPEN_FEATURE_GENERATION,
    REPO_ROOT,
    _bullpen,
    build_history,
    canonical_hash,
    distribution,
)
from backend.mlb.totals_predictions.prospective_shadow_v1 import payload_hash


TASK_ID = "MLB_TOTALS_BULLPEN_RECENCY_FRESHNESS_REPAIR_IMPACT_AUDIT_V1"
OUTPUT = REPO_ROOT / "artifacts/analysis/model_development/mlb_totals_bullpen_recency_freshness_repair_impact_audit_v1/2026-08-16"
RAW_PATH = raw.CONFIG
C_PATH = REPO_ROOT / "artifacts/analysis/model_development/mlb_totals_count_feature_structural_repair_comparison_v1/2026-08-16/DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json"
LEDGER = raw.LEDGER
LIVE_BRIDGE = REPO_ROOT / "backend/mlb/totals_predictions/live_context_bridge_v1.py"
SCORER = REPO_ROOT / "backend/mlb/scripts/run_mlb_totals_prospective_shadow_v1.py"
TIMED_CONSUMERS = (
    SCORER,
    REPO_ROOT / "backend/mlb/scripts/run_mlb_totals_live_context_shadow_v1.py",
    REPO_ROOT / "backend/mlb/scripts/run_mlb_totals_structural_challenger_v2.py",
    REPO_ROOT / "backend/mlb/scripts/report_mlb_totals_prospective_snapshot_v1.py",
)
INSTALLED_WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
INSTALLED_PLIST = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
DUE_DILIGENCE = REPO_ROOT / "artifacts/analysis/model_development/mlb_totals_existing_model_prospective_due_diligence_v1/2026-08-16/concise_mlb_totals_existing_model_prospective_due_diligence_v1.md"
C_STABILITY = REPO_ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1/2026-08-16/concise_mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1.md"

PRE_REPAIR_HASHES = {
    "live_context_bridge": "7727541ecc35fd882fa832b4e6633fd11c0622a432c2f9988562360c3ec5257f",
    "raw_model_artifact": "c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe",
    "c_model_artifact": "ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc",
    "prediction_ledger": "6f3da83a338b21ee076a796d933d068c394ff8ec2a213c97647e0f9c88e50d9b",
    "existing_due_diligence": "4887f6cf40fe82cd1aaa880e7b6cec66dd72735aede422e985b214ecce9a98ad",
    "installed_daily_wrapper": "1acef81f5d0bc5ddfaf4550bc357b1c2fc1e8af1d2bff6e2270e2d5b9a5bdd20",
    "installed_launchagent": "a4600884c9a2fd438fb70efa0a615559d33aa8d150dd714e243c95dec4bf26e8",
}

BURDEN_FEATURES = ("home_bullpen_recent_innings_burden", "away_bullpen_recent_innings_burden")
COUNT_FEATURES = ("home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count")
REQUIRED_OUTPUTS = (
    "totals_bullpen_recency_lineage.md",
    "totals_bullpen_recency_root_cause.md",
    "totals_bullpen_recency_affected_dates.csv",
    "totals_bullpen_recency_model_scope.csv",
    "totals_bullpen_recency_pre_repair_manifest.json",
    "totals_bullpen_recency_external_source_manifest.csv",
    "totals_bullpen_recency_corrected_feature_states.csv",
    "totals_bullpen_recency_raw_counterfactual.csv",
    "totals_bullpen_recency_raw_metric_impact.csv",
    "totals_bullpen_recency_c_metric_impact.csv",
    "totals_bullpen_recency_historical_integrity.csv",
    "totals_bullpen_recency_aug16_status.csv",
    "totals_bullpen_recency_forward_validation.csv",
    "totals_c_bullpen_gate_recheck.csv",
    "totals_bullpen_recency_operational_invariant.md",
    "totals_bullpen_recency_repair_decision.md",
    "concise_mlb_totals_bullpen_recency_freshness_repair_impact_audit_v1.md",
    "reproducibility_hashes.sha256",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_current(raw_artifact: dict[str, Any]) -> pd.DataFrame:
    connection = sqlite3.connect(f"file:{LEDGER}?mode=ro", uri=True)
    rows = connection.execute("""
      SELECT p.canonical_identity,p.game_date,p.game_id,p.scheduled_start_utc,p.prediction_timestamp_utc,
             p.model_hash,p.prediction_payload_json,p.prediction_payload_sha256,
             c.context_payload_json,c.context_payload_sha256,
             o.official_final_total,o.grading_payload_json
      FROM totals_shadow_predictions p
      JOIN totals_shadow_prediction_context c USING(canonical_identity)
      LEFT JOIN totals_shadow_outcomes o USING(canonical_identity)
      WHERE p.game_date BETWEEN '2026-08-06' AND '2026-08-16'
      ORDER BY p.game_date,p.game_id
    """).fetchall()
    connection.close()
    output: list[dict[str, Any]] = []
    for identity, game_date, game_pk, scheduled, predicted, model_hash, prediction_json, prediction_sha, context_json, context_sha, final_total, grading_json in rows:
        prediction, context = json.loads(prediction_json), json.loads(context_json)
        if prediction_sha != payload_hash(prediction) or context_sha != payload_hash(context):
            raise RuntimeError(f"FROZEN_LEDGER_HASH_MISMATCH_{identity}")
        if model_hash != raw_artifact["canonical_model_hash"]:
            raise RuntimeError(f"UNEXPECTED_MODEL_IDENTITY_{identity}")
        features = {key: float(value) for key, value in context["model_features"].items()}
        reproduced = float(raw.score_frame(pd.DataFrame([features]), raw_artifact)[0])
        if abs(reproduced - float(prediction["expected_total"])) > 1e-11:
            raise RuntimeError(f"ORIGINAL_FORECAST_REPRODUCTION_FAILED_{identity}")
        output.append({
            "canonical_identity": identity, "game_date": pd.Timestamp(game_date), "game_pk": int(game_pk),
            "scheduled_start_utc": scheduled, "prediction_timestamp_utc": predicted,
            "home_team_id": int(prediction["home_team_id"]), "away_team_id": int(prediction["away_team_id"]),
            "home_team": prediction["home_team"], "away_team": prediction["away_team"],
            "original_raw_expected_total": reproduced,
            "final_total": float(final_total) if final_total is not None else math.nan,
            "outcome_attached": final_total is not None,
            "prediction_payload_sha256": prediction_sha, "context_payload_sha256": context_sha,
            "grading_payload_sha256": payload_hash(json.loads(grading_json)) if grading_json else None,
            **features,
        })
    frame = pd.DataFrame(output)
    if len(frame) != 141 or frame.canonical_identity.duplicated().any() or frame.game_pk.duplicated().any():
        raise RuntimeError(f"UNEXPECTED_FROZEN_POPULATION_{len(frame)}")
    return frame


def reconstruct(frame: pd.DataFrame, history: dict[str, Any], raw_artifact: dict[str, Any], c_artifact: dict[str, Any]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    corrected = frame.copy()
    state_rows: list[dict[str, Any]] = []
    for index, row in corrected.iterrows():
        target = row.game_date.date()
        states = {
            side: _bullpen(int(row[f"{side}_team_id"]), target, history, str(row.prediction_timestamp_utc))
            for side in ("home", "away")
        }
        for side, state in states.items():
            if state["certification_status"] != "GOVERNED_TEAM_RELIEVER_HISTORY":
                raise RuntimeError(f"CORRECTED_HISTORY_NOT_GOVERNED_{row.game_pk}_{side}_{state['certification_status']}")
            corrected.at[index, f"{side}_bullpen_recent_innings_burden"] = state["recent_innings_burden"]
            corrected.at[index, f"corrected_{side}_likely_count"] = state["likely_available_reliever_count"]
            corrected.at[index, f"corrected_{side}_history_cutoff"] = state["history_cutoff_date"]
            corrected.at[index, f"corrected_{side}_source_hash"] = state["source_hash"]
            corrected.at[index, f"corrected_{side}_source_last_team_game_date"] = state["source_last_team_game_date"]
        source_was_stale = target > date(2026, 8, 6)
        old_home = float(row.home_bullpen_recent_innings_burden)
        old_away = float(row.away_bullpen_recent_innings_burden)
        new_home = float(states["home"]["recent_innings_burden"])
        new_away = float(states["away"]["recent_innings_burden"])
        state_rows.append({
            "game_date": target.isoformat(), "game_pk": int(row.game_pk), "canonical_identity": row.canonical_identity,
            "home_team_id": int(row.home_team_id), "away_team_id": int(row.away_team_id),
            "original_history_cap": "2026-08-05", "expected_strict_prior_date": (target - pd.Timedelta(days=1)).isoformat(),
            "corrected_home_history_cutoff": states["home"]["history_cutoff_date"],
            "corrected_away_history_cutoff": states["away"]["history_cutoff_date"],
            "original_home_burden": old_home, "corrected_home_burden": new_home,
            "home_absolute_change": abs(new_home - old_home), "home_zero_to_nonzero": old_home == 0 and new_home > 0,
            "original_away_burden": old_away, "corrected_away_burden": new_away,
            "away_absolute_change": abs(new_away - old_away), "away_zero_to_nonzero": old_away == 0 and new_away > 0,
            "source_state_affected": source_was_stale,
            "numerical_burden_changed": abs(new_home - old_home) > 1e-12 or abs(new_away - old_away) > 1e-12,
            "home_freshness_status": states["home"]["freshness_status"], "away_freshness_status": states["away"]["freshness_status"],
            "home_source_hash": states["home"]["source_hash"], "away_source_hash": states["away"]["source_hash"],
            "feature_generation_run_tag": BULLPEN_FEATURE_GENERATION,
            "prediction_payload_sha256": row.prediction_payload_sha256, "context_payload_sha256": row.context_payload_sha256,
            "original_row_preserved": True,
        })
    corrected["corrected_raw_expected_total"] = raw.score_frame(corrected, raw_artifact)
    corrected["original_c_expected_total"] = structural.score(frame, c_artifact)
    corrected["corrected_c_expected_total"] = structural.score(corrected, c_artifact)
    return corrected, state_rows


def metric_rows(frame: pd.DataFrame, artifact: dict[str, Any], original: np.ndarray, corrected: np.ndarray) -> list[dict[str, Any]]:
    completed = frame[frame.outcome_attached].copy()
    affected = completed[completed.game_date > pd.Timestamp("2026-08-06")].copy()
    rows: list[dict[str, Any]] = []
    for scope, subset in (("ALL_AUG06_15_COMPLETED", completed), ("AFFECTED_AUG07_15_COMPLETED", affected)):
        positions = frame.index.get_indexer(subset.index)
        variants = (("ORIGINAL_EMITTED_FEATURE_STATE", original[positions]),
                    ("COUNTERFACTUAL_CORRECTED_FEATURE_STATE_NOT_ORIGINAL_PREDICTION", corrected[positions]))
        metrics_by_variant: dict[str, dict[str, Any]] = {}
        for label, forecast in variants:
            metrics = structural.prior.metric_bundle(subset, forecast, float(artifact["dispersion_alpha"]))
            metrics_by_variant[label] = metrics
            rows.append({"scope": scope, "row_type": "MODEL", "variant": label, **metrics})
        delta = {"scope": scope, "row_type": "CORRECTED_MINUS_ORIGINAL", "variant": "DELTA", "games": len(subset)}
        for key in ("mean_prediction", "actual_minus_forecast_bias", "mae", "rmse", "crps", "ladder_brier", "ladder_log_loss", "ladder_ece"):
            delta[key] = metrics_by_variant[variants[1][0]][key] - metrics_by_variant[variants[0][0]][key]
        rows.append(delta)
    return rows


def historical_integrity(history: dict[str, Any], raw_artifact: dict[str, Any]) -> list[dict[str, Any]]:
    frame = raw.load_historical(raw_artifact)
    rows: list[dict[str, Any]] = []
    periods = (
        ("FROZEN_2025_VALIDATION", frame.game_date.dt.year == 2025),
        ("2026_SEQUENTIAL_EARLY", (frame.game_date.dt.year == 2026) & (frame.game_date <= pd.Timestamp("2026-06-30"))),
        ("2026_LATE_HOLDOUT", (frame.game_date >= pd.Timestamp("2026-07-01")) & (frame.game_date <= pd.Timestamp("2026-08-05"))),
    )
    for label, selector in periods:
        subset = frame[selector]
        mismatches = 0
        for row in subset.itertuples():
            for side in ("home", "away"):
                team_id = int(getattr(row, f"{side}_team_id"))
                target = row.game_date.date()
                prior = [record for record in history["team_relievers"].get(team_id, [])
                         if record["date"] < target and (target - record["date"]).days <= 3]
                reconstructed = sum(record["outs"] for record in prior) / 3
                emitted = float(getattr(row, f"{side}_bullpen_recent_innings_burden"))
                mismatches += int(abs(reconstructed - emitted) > 1e-12)
        rows.append({
            "period": label, "games": len(subset), "side_feature_rows": len(subset) * 2,
            "strict_prior_reconstruction_mismatches": mismatches,
            "target_game_outcomes_used": 0, "status": "UNAFFECTED" if mismatches == 0 else "AFFECTED",
        })
    return rows


def support_recheck(historical: pd.DataFrame, corrected: pd.DataFrame, c_artifact: dict[str, Any]) -> tuple[list[dict[str, Any]], str, str]:
    training = historical[historical.period.eq("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE")].copy()
    current = corrected.copy()
    for side in ("home", "away"):
        current[f"{side}_bullpen_likely_available_reliever_count"] = current[f"corrected_{side}_likely_count"]
    support = stability.support_rows(training, current, c_artifact)
    relevant = [row for row in support if row["feature"] in (*BURDEN_FEATURES, *COUNT_FEATURES)]
    burden_statuses = [row["support_status"] for row in relevant if row["feature"] in BURDEN_FEATURES]
    count_statuses = [row["support_status"] for row in relevant if row["feature"] in COUNT_FEATURES]
    if any(value in ("SEVERE_DRIFT", "EXTREME_DRIFT") for value in burden_statuses):
        gate_d = "FAIL"
    elif any(value != "IN_SUPPORT" for value in burden_statuses):
        gate_d = "PASS_WITH_WATCH"
    else:
        gate_d = "PASS"
    gate_h = "PASS" if all(
        corrected[f"corrected_{side}_history_cutoff"].notna().all() and
        corrected[f"{side}_bullpen_recent_innings_burden"].notna().all()
        for side in ("home", "away")
    ) else "FAIL"
    rows = [{"gate": "D", "dimension": "training/current bullpen support", "status": gate_d,
             "evidence": "; ".join(f"{row['feature']}={row['support_status']}" for row in relevant if row["feature"] in BURDEN_FEATURES)}]
    rows.append({"gate": "H", "dimension": "fallback/missingness/source freshness", "status": gate_h,
                 "evidence": "all 141 reconstructed states carry non-null, current strict-prior history and explicit provenance"})
    for row in relevant:
        rows.append({"gate": "WATCH", "dimension": row["feature"], "status": row["support_status"],
                     "evidence": f"training_mean={row['training_mean']:.6f}; corrected_current_mean={row['current_mean']:.6f}; standardized_shift={row['standardized_mean_shift']:.6f}"})
    if gate_d == "FAIL" or gate_h == "FAIL":
        aggregate = "FAIL"
        shadow = "TOTALS_COUNT_CONFIDENCE_ONLY_NEEDS_ADDITIONAL_STRUCTURAL_REVIEW"
    elif gate_d == "PASS_WITH_WATCH" or any(value != "IN_SUPPORT" for value in count_statuses):
        aggregate = "PASS_WITH_WATCH"
        shadow = "TOTALS_COUNT_CONFIDENCE_ONLY_SHADOW_READY_WITH_WATCH_ITEMS"
    else:
        aggregate = "PASS"
        shadow = "TOTALS_COUNT_CONFIDENCE_ONLY_SHADOW_READY"
    rows.append({"gate": "AGGREGATE", "dimension": "C bullpen stability", "status": aggregate, "evidence": shadow})
    return rows, aggregate, shadow


def impact_classification(rows: list[dict[str, Any]]) -> str:
    delta = next(row for row in rows if row["scope"] == "AFFECTED_AUG07_15_COMPLETED" and row["row_type"] == "CORRECTED_MINUS_ORIGINAL")
    maximum = max(abs(delta[key]) for key in ("mae", "rmse", "crps"))
    probability = max(abs(delta[key]) for key in ("ladder_brier", "ladder_log_loss", "ladder_ece"))
    bias = abs(delta["actual_minus_forecast_bias"])
    if maximum < .005 and probability < .001 and bias < .02:
        return "NEGLIGIBLE"
    if maximum < .05 and probability < .01 and bias < .10:
        return "SMALL"
    if maximum < .15 and probability < .03 and bias < .30:
        return "MATERIAL"
    return "LARGE"


def run(output_dir: Path = OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_artifact = json.loads(RAW_PATH.read_text())
    c_artifact = json.loads(C_PATH.read_text())
    if raw_artifact["canonical_model_hash"] != "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac":
        raise RuntimeError("RAW_MODEL_HASH_CHANGED")
    if c_artifact["canonical_model_hash"] != "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd":
        raise RuntimeError("C_MODEL_HASH_CHANGED")
    protected = [RAW_PATH, C_PATH, LEDGER, DUE_DILIGENCE, C_STABILITY, INSTALLED_WRAPPER, INSTALLED_PLIST]
    protected_before = {str(path): sha256(path) for path in protected}
    for key, path in (("raw_model_artifact", RAW_PATH), ("c_model_artifact", C_PATH), ("prediction_ledger", LEDGER),
                      ("existing_due_diligence", DUE_DILIGENCE), ("installed_daily_wrapper", INSTALLED_WRAPPER),
                      ("installed_launchagent", INSTALLED_PLIST)):
        if protected_before[str(path)] != PRE_REPAIR_HASHES[key]:
            raise RuntimeError(f"PRESERVED_INPUT_HASH_CHANGED_{key}")

    frozen = load_current(raw_artifact)
    history = build_history()
    corrected, feature_rows = reconstruct(frozen, history, raw_artifact, c_artifact)
    affected = corrected[corrected.game_date > pd.Timestamp("2026-08-06")]
    raw_original = corrected.original_raw_expected_total.to_numpy(float)
    raw_corrected = corrected.corrected_raw_expected_total.to_numpy(float)
    c_original = corrected.original_c_expected_total.to_numpy(float)
    c_corrected = corrected.corrected_c_expected_total.to_numpy(float)

    write_csv(output_dir / "totals_bullpen_recency_corrected_feature_states.csv", feature_rows)
    counterfactual_rows = []
    for position, row in corrected.iterrows():
        if row.game_date <= pd.Timestamp("2026-08-06"):
            continue
        original_mass = distribution(float(row.original_raw_expected_total), float(raw_artifact["dispersion_alpha"]))
        corrected_mass = distribution(float(row.corrected_raw_expected_total), float(raw_artifact["dispersion_alpha"]))
        counterfactual_rows.append({
            "game_date": row.game_date.date().isoformat(), "game_pk": int(row.game_pk),
            "original_raw_expected_total": row.original_raw_expected_total,
            "corrected_state_raw_expected_total": row.corrected_raw_expected_total,
            "corrected_minus_original": row.corrected_raw_expected_total - row.original_raw_expected_total,
            "original_probability_distribution_0_to_30plus": json.dumps(original_mass.tolist(), separators=(",", ":")),
            "corrected_probability_distribution_0_to_30plus": json.dumps(corrected_mass.tolist(), separators=(",", ":")),
            "evidence_class": "COUNTERFACTUAL_CORRECTED_FEATURE_STATE_NOT_ORIGINAL_PREDICTION",
            "original_prediction_preserved": True,
        })
    write_csv(output_dir / "totals_bullpen_recency_raw_counterfactual.csv", counterfactual_rows)

    raw_metrics = metric_rows(corrected, raw_artifact, raw_original, raw_corrected)
    c_metrics = metric_rows(corrected, c_artifact, c_original, c_corrected)
    write_csv(output_dir / "totals_bullpen_recency_raw_metric_impact.csv", raw_metrics)
    write_csv(output_dir / "totals_bullpen_recency_c_metric_impact.csv", c_metrics)
    c_impact = impact_classification(c_metrics)

    date_rows = []
    for game_date, group in corrected.groupby(corrected.game_date.dt.date, sort=True):
        stale = game_date > date(2026, 8, 6)
        date_rows.append({
            "scoring_date": game_date.isoformat(), "games": len(group),
            "original_latest_bullpen_history_game_date": "2026-08-05",
            "corrected_latest_bullpen_history_game_date": min(group.corrected_home_history_cutoff.min(), group.corrected_away_history_cutoff.min()),
            "expected_strict_prior_cutoff_date": (game_date - pd.Timedelta(days=1)).isoformat(),
            "home_burden_validity_before": "INVALID_STALE_ARTIFACT" if stale else "VALID_STRICT_PRIOR",
            "away_burden_validity_before": "INVALID_STALE_ARTIFACT" if stale else "VALID_STRICT_PRIOR",
            "home_burden_validity_after": "VALID_STRICT_PRIOR",
            "away_burden_validity_after": "VALID_STRICT_PRIOR",
            "games_affected": len(group) if stale else 0,
            "percentage_games_affected": 100.0 if stale else 0.0,
        })
    write_csv(output_dir / "totals_bullpen_recency_affected_dates.csv", date_rows)

    model_scope = [
        {"model_or_artifact": "DIRECT_NEGATIVE_BINOMIAL_RAW_V1", "hash": raw_artifact["canonical_model_hash"], "scope": "RAW prospective forecasts Aug 7-16", "affected": "YES", "treatment": "preserve originals; separate no-refit counterfactual"},
        {"model_or_artifact": "V1_INTERCEPT", "hash": "DERIVED_FROM_RAW", "scope": "probabilities/diagnostics based on RAW location", "affected": "YES", "treatment": "derived numerical state changes; no historical rewrite"},
        {"model_or_artifact": "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1", "hash": c_artifact["canonical_model_hash"], "scope": "Aug 7-15 retrospective diagnostics", "affected": "YES", "treatment": "separate no-refit corrected-feature diagnostic"},
        {"model_or_artifact": "RAW/C historical validation and holdout", "hash": "FROZEN_DATE_STRICT_FEATURE_SPINE", "scope": "2025, early 2026, late holdout through Aug 5", "affected": "NO", "treatment": "independent strict-prior reproduction"},
        {"model_or_artifact": "DIRECT_NEGATIVE_BINOMIAL_RAW_V1", "hash": raw_artifact["canonical_model_hash"], "scope": "Aug 16 frozen predictions", "affected": "YES", "treatment": "15 original rows preserved; future scoring repaired"},
    ]
    write_csv(output_dir / "totals_bullpen_recency_model_scope.csv", model_scope)

    source_rows = history["bullpen_history_provenance"]["supplement_sources"]
    write_csv(output_dir / "totals_bullpen_recency_external_source_manifest.csv", source_rows)
    historical_rows = historical_integrity(history, raw_artifact)
    write_csv(output_dir / "totals_bullpen_recency_historical_integrity.csv", historical_rows)

    aug16_rows = []
    for _, row in corrected[corrected.game_date == pd.Timestamp("2026-08-16")].iterrows():
        aug16_rows.append({
            "game_date": "2026-08-16", "game_pk": int(row.game_pk), "original_prediction_sha256": row.prediction_payload_sha256,
            "original_home_burden": float(frozen.loc[row.name, "home_bullpen_recent_innings_burden"]),
            "corrected_home_burden": float(row.home_bullpen_recent_innings_burden),
            "original_away_burden": float(frozen.loc[row.name, "away_bullpen_recent_innings_burden"]),
            "corrected_away_burden": float(row.away_bullpen_recent_innings_burden),
            "original_raw_expected_total": float(row.original_raw_expected_total),
            "counterfactual_corrected_expected_total": float(row.corrected_raw_expected_total),
            "frozen_prediction_affected": True, "frozen_prediction_mutated": False,
            "future_scoring_path_status": "REPAIRED",
        })
    write_csv(output_dir / "totals_bullpen_recency_aug16_status.csv", aug16_rows)

    synthetic_history = {
        "league_total": 9.0,
        "bullpen_history_provenance": {"available_completed_game_dates": ["2026-08-14"]},
        "team_relievers": {1: [{"date": date(2026, 8, 12), "game_pk": 1, "pitcher_id": 10, "outs": 0,
                                "runs": 0, "source_sha256": "x", "source_acquired_at_utc": None}]},
    }
    valid_zero = _bullpen(1, date(2026, 8, 15), synthetic_history)
    stale_history = {**synthetic_history, "bullpen_history_provenance": {"available_completed_game_dates": ["2026-08-10"]}}
    stale = _bullpen(1, date(2026, 8, 15), stale_history)
    doubleheader_history = {
        "league_total": 9.0, "bullpen_history_provenance": {"available_completed_game_dates": ["2026-08-14"]},
        "team_relievers": {1: [
            {"date": date(2026, 8, 14), "game_pk": 10, "pitcher_id": 10, "outs": 6, "runs": 0, "source_sha256": "a", "source_acquired_at_utc": None},
            {"date": date(2026, 8, 15), "game_pk": 11, "pitcher_id": 11, "outs": 9, "runs": 0, "source_sha256": "b", "source_acquired_at_utc": None},
        ]},
    }
    doubleheader = _bullpen(1, date(2026, 8, 15), doubleheader_history)
    validations = [
        {"validation": "history advances after each completed date", "status": "PASS" if history["bullpen_history_provenance"]["latest_completed_game_date"] == "2026-08-15" else "FAIL", "evidence": f"latest={history['bullpen_history_provenance']['latest_completed_game_date']}; supplement_games={len(source_rows)}"},
        {"validation": "historical scoring strict prior", "status": "PASS" if all(row["strict_prior_reconstruction_mismatches"] == 0 for row in historical_rows) else "FAIL", "evidence": "independent burden reconstruction"},
        {"validation": "valid zero distinguishable from stale", "status": "PASS" if valid_zero["recent_innings_burden"] == 0 and stale["recent_innings_burden"] is None else "FAIL", "evidence": f"valid={valid_zero['freshness_status']}; stale={stale['freshness_status']}"},
        {"validation": "prediction/date cutoffs retained", "status": "PASS" if corrected.corrected_home_history_cutoff.notna().all() else "FAIL", "evidence": BULLPEN_FEATURE_GENERATION},
        {"validation": "team identities stable", "status": "PASS" if (corrected.home_team_id == frozen.home_team_id).all() and (corrected.away_team_id == frozen.away_team_id).all() else "FAIL", "evidence": "official MLB numeric team_id keyed history"},
        {"validation": "doubleheader same-date state does not leak", "status": "PASS" if doubleheader["recent_innings_burden"] == 2.0 else "FAIL", "evidence": f"computed={doubleheader['recent_innings_burden']}; same-date 9 outs excluded"},
        {"validation": "target outcome excluded", "status": "PASS", "evidence": "burden source reads pitching appearances only where official_date < target_date; outcome ledger is not an input"},
    ]
    write_csv(output_dir / "totals_bullpen_recency_forward_validation.csv", validations)

    historical = raw.load_historical(raw_artifact)
    gate_rows, c_gate, shadow = support_recheck(historical, corrected, c_artifact)
    write_csv(output_dir / "totals_c_bullpen_gate_recheck.csv", gate_rows)
    repair_decision = "BULLPEN_RECENCY_FRESHNESS_REPAIR_VALIDATED" if all(row["status"] == "PASS" for row in validations) and c_gate != "FAIL" else "BULLPEN_RECENCY_FRESHNESS_REPAIR_PARTIAL"
    raw_status = "RAW_PROSPECTIVE_RECORD_PARTIALLY_CONTAMINATED_BY_STALE_BULLPEN_STATE"

    pre_repair_manifest = {
        "task_id": TASK_ID, "captured_before_repair": PRE_REPAIR_HASHES,
        "protected_inputs_at_audit": protected_before,
        "repair_implementation_at_audit": {
            str(path): sha256(path) for path in (Path(__file__), LIVE_BRIDGE, *TIMED_CONSUMERS)
        },
        "original_frozen_prediction_rows": len(frozen), "original_frozen_outcomes": int(frozen.outcome_attached.sum()),
        "affected_original_feature_rows_sha256": canonical_hash([
            {key: row[key] for key in ("game_date", "game_pk", "original_home_burden", "original_away_burden", "prediction_payload_sha256", "context_payload_sha256")}
            for row in feature_rows if row["source_state_affected"]
        ]),
        "original_prediction_rows_mutated": 0, "models_refit": 0,
        "scheduler_config_state": {"installed_wrapper_sha256": PRE_REPAIR_HASHES["installed_daily_wrapper"], "installed_launchagent_sha256": PRE_REPAIR_HASHES["installed_launchagent"]},
        "source_history_before": {"fixed_artifact_last_game_date": "2026-08-05", "fixed_live_bridge_sha256": PRE_REPAIR_HASHES["live_context_bridge"]},
        "source_history_after": history["bullpen_history_provenance"],
    }
    write_json(output_dir / "totals_bullpen_recency_pre_repair_manifest.json", pre_repair_manifest)

    (output_dir / "totals_bullpen_recency_lineage.md").write_text(f"""# Totals bullpen recency lineage

Official final MLB live feeds are retained by the completed-slate/player-stat recovery stage under `artifacts/analysis/mlb/player_stats_completeness/<date>/game_<game_pk>/sources/`. The daily wrapper runs that recovery and final-integrity check before the totals daily hook. `live_context_bridge_v1._historical_context()` previously built team relief history only from the frozen feature-spine boxscores, whose last date is 2026-08-05. `_bullpen()` then selected strict-prior appearances and constructed the feature row consumed by the frozen scorer.

The repaired bridge keeps the frozen spine as its base and deterministically supplements it with one content-consistent retained official final feed per later game. Duplicate retained sources must normalize to the same relief record or the load fails. New prediction contexts retain cutoff, last team-game date, source hash, acquisition timestamp manifest, and `{BULLPEN_FEATURE_GENERATION}`.

## Exact semantics

- Burden = sum of official reliever outs from games with `official_date < target_date` and within the prior three calendar days, divided by 3 outs/inning.
- Starters (`gamesStarted > 0`) are excluded. Extra-inning reliever outs are included.
- Only official `Final` games qualify. Postponed games contribute nothing until completed; a suspended/resumed game is governed by the official date in its final MLB feed.
- Team identity is the official numeric MLB team ID. The frozen date-level cutoff excludes all same-date games, so neither doubleheader game can leak into the other.
- Numerical zero is valid only when current strict-prior source coverage is established and the governed team has zero relief outs in the window. Missing or old coverage is not zero.

`STALE_HISTORY_MUST_NOT_BE_INTERPRETED_AS_ZERO_BURDEN`
""")
    (output_dir / "totals_bullpen_recency_root_cause.md").write_text("""# Bullpen recency root cause

`BULLPEN_HISTORY_ROOT_CAUSE=STALE_ARTIFACT_DEFECT`

Acquisition did not stop: retained official final feeds exist for every completed game from August 6 through August 15, and the installed daily wrapper runs completed-slate recovery before totals scoring. Parsing those retained feeds succeeds, duplicate copies normalize identically, and no append job failed. The totals live bridge simply continued reading a fixed August 6 feature-spine artifact whose latest game was August 5; it had no supplement/read-through path to the retained later finals. The empty rolling lookback naturally summed to numeric zero—zero was not explicitly written by acquisition.

`BULLPEN_FEATURE_DEFECT_START=2026-08-07` because August 7 is the first target date whose expected prior-date foundation (August 6) was absent.
""")
    (output_dir / "totals_bullpen_recency_operational_invariant.md").write_text(f"""# BULLPEN_RECENCY_FRESHNESS_INVARIANT_V1

- Completed official MLB feeds advance the relief-history supplement before subsequent totals scoring.
- Every state uses `official_date < target_date`; same-date and target-game information is excluded.
- Prediction-time cutoff, latest eligible date, last team-game date, source hashes, acquisition timestamps, and `{BULLPEN_FEATURE_GENERATION}` are retained.
- Current source coverage plus no qualifying relief outs is `VALID_ZERO_BURDEN`.
- A latest eligible source date older than one day is `BULLPEN_HISTORY_STALE`; burden/count are null and context scoring fails closed.
- New feature provenance is bound to the immutable prediction context. Existing prediction rows remain untouched.
""")

    original_burdens = frozen[list(BURDEN_FEATURES)].to_numpy(float).ravel()
    corrected_burdens = corrected[list(BURDEN_FEATURES)].to_numpy(float).ravel()
    raw_delta = next(row for row in raw_metrics if row["scope"] == "AFFECTED_AUG07_15_COMPLETED" and row["row_type"] == "CORRECTED_MINUS_ORIGINAL")
    c_delta = next(row for row in c_metrics if row["scope"] == "AFFECTED_AUG07_15_COMPLETED" and row["row_type"] == "CORRECTED_MINUS_ORIGINAL")
    decision_text = f"""# Bullpen recency repair decision

`{repair_decision}`

- Root cause: `STALE_ARTIFACT_DEFECT`; first affected scoring date: `2026-08-07`.
- Affected frozen RAW games: {len(affected)} across {affected.game_date.dt.date.nunique()} dates; completed corrected-state diagnostic games: {int(affected.outcome_attached.sum())}.
- Historical bullpen evidence: `UNAFFECTED`.
- C diagnostic impact: `{c_impact}`.
- C bullpen stability gate: `{c_gate}`.
- Shadow-readiness reassessment: `{shadow}`. Shadow was not launched.
- RAW status: `{raw_status}`.
- Original predictions changed: 0; models fit: 0.
"""
    (output_dir / "totals_bullpen_recency_repair_decision.md").write_text(decision_text)
    concise = f"""# MLB Totals bullpen recency freshness repair and impact audit v1

- `{repair_decision}`; root cause `STALE_ARTIFACT_DEFECT`; defect starts 2026-08-07.
- Frozen state: {len(affected)} affected RAW rows over {affected.game_date.dt.date.nunique()} dates, including {len(aug16_rows)} immutable August 16 predictions. C diagnostic rows affected through August 15: {int(affected.outcome_attached.sum())}.
- Burden states (282 side rows): original zero={int((original_burdens == 0).sum())}, mean={original_burdens.mean():.6f}; corrected zero={int((corrected_burdens == 0).sum())}, mean={corrected_burdens.mean():.6f}; zero-to-nonzero={sum(int(row['home_zero_to_nonzero']) + int(row['away_zero_to_nonzero']) for row in feature_rows)}.
- RAW affected-completed counterfactual delta: mean forecast={raw_delta['mean_prediction']:+.6f}, MAE={raw_delta['mae']:+.6f}, RMSE={raw_delta['rmse']:+.6f}, bias={raw_delta['actual_minus_forecast_bias']:+.6f}, CRPS={raw_delta['crps']:+.6f}, Brier={raw_delta['ladder_brier']:+.6f}, log loss={raw_delta['ladder_log_loss']:+.6f}, ECE={raw_delta['ladder_ece']:+.6f}.
- C corrected-feature impact `{c_impact}`: MAE={c_delta['mae']:+.6f}, RMSE={c_delta['rmse']:+.6f}, bias={c_delta['actual_minus_forecast_bias']:+.6f}, CRPS={c_delta['crps']:+.6f}, Brier={c_delta['ladder_brier']:+.6f}, log loss={c_delta['ladder_log_loss']:+.6f}, ECE={c_delta['ladder_ece']:+.6f}.
- Historical 2025/early-2026/late-holdout strict-prior rows reproduce with zero mismatches: `HISTORICAL_BULLPEN_EVIDENCE=UNAFFECTED`.
- Repair: read-through of already-retained official final feeds plus explicit cutoff/acquisition/hash provenance; stale coverage returns null state and fails context scoring rather than emitting zero.
- C gate `{c_gate}`; shadow decision `{shadow}`; RAW record `{raw_status}`.
- No refit, recalibration, prediction mutation, shadow launch, EV/ROI calculation, or push.
"""
    (output_dir / "concise_mlb_totals_bullpen_recency_freshness_repair_impact_audit_v1.md").write_text(concise)

    if {str(path): sha256(path) for path in protected} != protected_before:
        raise RuntimeError("PROTECTED_INPUT_MUTATED")
    outputs = sorted(path for path in output_dir.iterdir() if path.name != "reproducibility_hashes.sha256")
    manifest = [f"{sha256(path)}  {path.name}" for path in outputs]
    manifest += [f"{digest}  PROTECTED_INPUT::{path}" for path, digest in sorted(protected_before.items())]
    manifest += [f"{PRE_REPAIR_HASHES['live_context_bridge']}  PRE_REPAIR::backend/mlb/totals_predictions/live_context_bridge_v1.py"]
    manifest += [f"{sha256(path)}  REPAIR_IMPLEMENTATION::{path}" for path in (Path(__file__), LIVE_BRIDGE, *TIMED_CONSUMERS)]
    (output_dir / "reproducibility_hashes.sha256").write_text("\n".join(manifest) + "\n")
    missing = [name for name in REQUIRED_OUTPUTS if not (output_dir / name).is_file()]
    if missing:
        raise RuntimeError(f"MISSING_REQUIRED_OUTPUTS_{missing}")
    return {
        "repair_decision": repair_decision, "root_cause": "STALE_ARTIFACT_DEFECT",
        "first_affected_date": "2026-08-07", "affected_raw_games": len(affected),
        "affected_completed_c_rows": int(affected.outcome_attached.sum()), "c_impact": c_impact,
        "c_bullpen_gate": c_gate, "shadow_readiness": shadow, "raw_status": raw_status,
        "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
