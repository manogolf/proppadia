"""Score frozen totals challenger C from immutable RAW pregame contexts."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_count_feature_structural_repair_comparison_v1 as structural
from backend.mlb.totals_predictions.c_shadow_v1 import (
    MODEL_HASH, MODEL_NAME, SNAPSHOT_CLASS, append_prediction_with_context, append_watch_observation,
    canonical_identity, connect_ledger, counts, payload_hash, predictions_for_date,
)
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution
from backend.mlb.totals_predictions.prospective_shadow_v1 import payload_hash as raw_payload_hash


ROOT = Path(__file__).resolve().parents[3]
EXPERIMENT = "MLB_TOTALS_COUNT_CONFIDENCE_ONLY_LIVE_SHADOW_V1"
START_DATE = "2026-08-17"
RAW_MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
ARTIFACT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_feature_structural_repair_comparison_v1/2026-08-16/DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json"
ARTIFACT_SHA256 = "ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc"
FEATURE_CONTRACT_HASH = "d7551fd7798aa60ada1b96831e32bcb7748a17aabf67f53c8800f24c9f4a0927"
RAW_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
C_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_c_shadow_v1/totals_c_shadow_v1.sqlite3"
OUTPUT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_totals_c_live_shadow_v1"
SUPPORT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1/2026-08-16/totals_c_feature_support_drift.csv"
THRESHOLDS = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_artifact() -> dict[str, Any]:
    raw = ARTIFACT.read_bytes()
    artifact = json.loads(raw)
    if hashlib.sha256(raw).hexdigest() != ARTIFACT_SHA256:
        raise RuntimeError("C_ARTIFACT_SHA256_MISMATCH")
    if artifact.get("candidate_identity") != MODEL_NAME or artifact.get("canonical_model_hash") != MODEL_HASH:
        raise RuntimeError("C_MODEL_IDENTITY_MISMATCH")
    if structural.artifact_hash(artifact) != MODEL_HASH:
        raise RuntimeError("C_CANONICAL_MODEL_HASH_RECOMPUTATION_FAILED")
    feature_contract = {key: artifact[key] for key in ("feature_order", "scaler_mean", "scaler_scale", "normalization")}
    if structural.prior.canonical_hash(feature_contract) != FEATURE_CONTRACT_HASH:
        raise RuntimeError("C_FEATURE_CONTRACT_HASH_MISMATCH")
    if artifact.get("fit_count") != 1 or artifact.get("prospective_rows_used_for_fit_or_selection") != 0:
        raise RuntimeError("C_TRAINING_BINDING_MISMATCH")
    return artifact


def raw_rows(game_date: str, raw_ledger_path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(raw_ledger_path)
    rows = connection.execute("""SELECT p.canonical_identity,p.game_id,p.scheduled_start_utc,p.prediction_timestamp_utc,
      p.model_hash,p.feature_state_hash,p.schedule_source_hash,p.market_source_hash,
      p.prediction_payload_json,p.prediction_payload_sha256,c.context_payload_json,c.context_payload_sha256
      FROM totals_shadow_predictions p JOIN totals_shadow_prediction_context c USING(canonical_identity)
      WHERE p.game_date=? ORDER BY p.scheduled_start_utc,p.game_id""", (game_date,)).fetchall()
    connection.close()
    output = []
    for row in rows:
        identity, game_pk, scheduled, predicted, model_hash, feature_hash, schedule_hash, market_hash, prediction_json, prediction_sha, context_json, context_sha = row
        prediction, context = json.loads(prediction_json), json.loads(context_json)
        if model_hash != RAW_MODEL_HASH or prediction.get("model_hash") != RAW_MODEL_HASH:
            raise RuntimeError(f"RAW_CONTROL_HASH_MISMATCH_{identity}")
        if raw_payload_hash(prediction) != prediction_sha or raw_payload_hash(context) != context_sha:
            raise RuntimeError(f"RAW_SOURCE_PAYLOAD_HASH_MISMATCH_{identity}")
        if context_sha != feature_hash or prediction.get("feature_state_hash") != feature_hash:
            raise RuntimeError(f"RAW_SOURCE_FEATURE_HASH_MISMATCH_{identity}")
        if pd.Timestamp(predicted) >= pd.Timestamp(scheduled):
            raise RuntimeError(f"RAW_SOURCE_POST_START_{identity}")
        output.append({
            "raw_identity": identity, "game_pk": int(game_pk), "scheduled_start_utc": scheduled,
            "raw_prediction_timestamp_utc": predicted, "feature_state_hash": feature_hash,
            "schedule_source_hash": schedule_hash, "market_source_hash": market_hash,
            "raw_prediction_sha256": prediction_sha, "raw_context_sha256": context_sha,
            "prediction": prediction, "context": context,
        })
    return output


def probability_fields(mu: float, alpha: float, line: float | None) -> tuple[dict[str, Any], np.ndarray, int]:
    mass = distribution(mu, alpha)
    if abs(float(mass.sum()) - 1.0) > 1e-12:
        raise RuntimeError("C_PROBABILITY_MASS_NOT_NORMALIZED")
    support = np.arange(len(mass))
    result = {f"p_over_{str(value).replace('.', '_')}": float(mass[support > value].sum()) for value in THRESHOLDS}
    if line is None:
        result.update({"p_over_governed_total_line": None, "p_under_governed_total_line": None, "p_push_governed_total_line": None})
    else:
        result.update({
            "p_over_governed_total_line": float(mass[support > line].sum()),
            "p_under_governed_total_line": float(mass[support < line].sum()),
            "p_push_governed_total_line": float(mass[support == line].sum()),
        })
    median = int(np.searchsorted(np.cumsum(mass), 0.5))
    return result, mass, median


def _support_bounds() -> dict[str, dict[str, float]]:
    frame = pd.read_csv(SUPPORT)
    return {str(row.feature): {
        "mean": float(row.training_mean), "sd": float(row.training_sd),
        "minimum": float(row.training_min), "maximum": float(row.training_max),
    } for row in frame.itertuples()}


def watch_payload(game_date: str, run_tag: str, observed: str, source_rows: list[dict[str, Any]], artifact: dict[str, Any], raw_attempts: list[dict[str, Any]] | None) -> dict[str, Any]:
    features = [row["context"]["model_features"] for row in source_rows]
    contexts = [row["context"] for row in source_rows]
    bounds = _support_bounds()
    rows: list[dict[str, Any]] = []
    bullpen_states = [context.get(f"{side}_bullpen_state", {}) for context in contexts for side in ("home", "away")]
    freshness_failures = sum(state.get("freshness_status") != "CURRENT_STRICT_PRIOR_HISTORY" for state in bullpen_states)
    rows.append({"watch": "A_BULLPEN_SOURCE_FRESHNESS", "status": "FAIL" if freshness_failures else "PASS", "value": freshness_failures,
                 "evidence": "non-current bullpen side states"})
    burdens = [float(feature[f"{side}_bullpen_recent_innings_burden"]) for feature in features for side in ("home", "away")]
    zero_burdens = sum(value == 0 for value in burdens)
    rows.append({"watch": "B_ZERO_BURDEN_FREQUENCY", "status": "WATCH" if zero_burdens else "PASS", "value": zero_burdens,
                 "evidence": "valid current-source numerical zeros; watch only, never treated as stale"})
    count_violations = 0
    count_shifts = []
    for name in ("home_bullpen_likely_available_reliever_count", "away_bullpen_likely_available_reliever_count"):
        values = [float(feature[name]) for feature in features]
        count_violations += sum(value < bounds[name]["minimum"] or value > bounds[name]["maximum"] for value in values)
        count_shifts.append((float(np.mean(values)) - bounds[name]["mean"]) / bounds[name]["sd"] if values else math.nan)
    count_watch = count_violations > 0 or any(abs(value) >= 0.5 for value in count_shifts)
    rows.append({"watch": "C_LIKELY_RELIEVER_COUNT_DRIFT", "status": "WATCH" if count_watch else "PASS",
                 "value": max((abs(value) for value in count_shifts), default=math.nan),
                 "evidence": f"maximum absolute standardized center shift; training-range violations={count_violations}"})
    fallback = sum(context.get(f"{side}_starter_state", {}).get("fallback_tier") != "DIRECT_STARTER_HISTORY" for context in contexts for side in ("home", "away"))
    rows.append({"watch": "D_STARTER_FALLBACK_MIX", "status": "WATCH" if fallback else "PASS", "value": fallback,
                 "evidence": "governed non-direct starter side states"})
    league_values = [float(feature["league_total"]) for feature in features]
    league_shift = ((float(np.mean(league_values)) - bounds["league_total"]["mean"]) / bounds["league_total"]["sd"]) if league_values else math.nan
    rows.append({"watch": "E_LEAGUE_TOTAL_CENTER_DRIFT", "status": "FAIL" if abs(league_shift) >= 3 else ("WATCH" if abs(league_shift) >= 1 else "PASS"),
                 "value": league_shift, "evidence": "standardized shift from frozen development center"})
    raw_attempts = raw_attempts or []
    probable_blocks = sum(any("PROBABLE_PITCHER" in str(reason) for reason in attempt.get("rejection_reasons", [])) for attempt in raw_attempts)
    rows.append({"watch": "F_PROBABLE_PITCHER_AVAILABILITY", "status": "WATCH" if probable_blocks else "PASS", "value": probable_blocks,
                 "evidence": "RAW shared-source scoring attempts blocked by probable-pitcher state"})
    park_fallbacks = sum(context.get("park_state", {}).get("fallback_status") != "DIRECT_REGRESSED_PARK_HISTORY" for context in contexts)
    rows.append({"watch": "G_PARK_CONTEXT_FALLBACK", "status": "WATCH" if park_fallbacks else "PASS", "value": park_fallbacks,
                 "evidence": "non-direct park contexts among admitted shared rows"})
    support_violations = sum(
        float(feature[name]) < bounds[name]["minimum"] or float(feature[name]) > bounds[name]["maximum"]
        for feature in features for name in artifact["feature_order"]
    )
    rows.append({"watch": "H_FEATURE_SUPPORT_VIOLATIONS", "status": "WATCH" if support_violations else "PASS", "value": support_violations,
                 "evidence": "feature values outside frozen development min/max"})
    rows.append({"watch": "I_MODEL_HASH_INTEGRITY", "status": "PASS", "value": MODEL_HASH, "evidence": ARTIFACT_SHA256})
    status = "FAIL" if any(row["status"] == "FAIL" for row in rows) else ("WATCH" if any(row["status"] == "WATCH" for row in rows) else "PASS")
    return {
        "experiment": EXPERIMENT, "game_date": game_date, "scoring_run_tag": run_tag, "observed_at_utc": observed,
        "deployment_watch_status": status, "watch_rows": rows,
        "regime_classification": "LATE_SEASON_TRANSITION_WATCH",
        "regime_evidence": {
            "performance_used": False,
            "available_nonperformance_indicators": [row["watch"] for row in rows[:-1]],
            "unavailable_exact_indicators": ["mathematical_elimination_status", "active_roster_turnover", "lineup_churn", "replacement_player_usage"],
            "reason": "exact objective late-season classification is not yet supportable; contract requires WATCH rather than invented certainty",
        },
    }


def score_from_raw(game_date: str, scoring_mode: str, run_tag: str, raw_ledger_path: Path = RAW_LEDGER,
                   c_ledger_path: Path = C_LEDGER, observed_at_utc: str | None = None,
                   raw_attempts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if game_date < START_DATE:
        return {"status": "C_LIVE_SHADOW_NOT_STARTED", "game_date": game_date, "new_rows": 0, "rows": 0}
    if scoring_mode not in ("PRIMARY_SCORE", "SCORE_MISSING"):
        raise ValueError(f"C_SCORING_MODE_INVALID_{scoring_mode}")
    artifact = load_artifact()
    observed = observed_at_utc or now_utc()
    source_rows = raw_rows(game_date, raw_ledger_path)
    connection = connect_ledger(c_ledger_path)
    before = counts(connection)
    existing = {int(row["game_pk"]): row for row in predictions_for_date(connection, game_date)}
    attempts = []
    for source in source_rows:
        game_pk = int(source["game_pk"])
        if game_pk in existing:
            attempts.append({"game_pk": game_pk, "ledger_action": "EXISTING_IMMUTABLE", "source_raw_identity": source["raw_identity"]})
            continue
        if pd.Timestamp(observed) >= pd.Timestamp(source["scheduled_start_utc"]):
            attempts.append({"game_pk": game_pk, "ledger_action": "REJECTED_POST_START", "rejection_reason": "PREGAME_CUTOFF_FAILED"})
            continue
        raw_prediction, context = source["prediction"], source["context"]
        bullpen_states = [context.get(f"{side}_bullpen_state", {}) for side in ("home", "away")]
        if any(state.get("freshness_status") != "CURRENT_STRICT_PRIOR_HISTORY" for state in bullpen_states):
            attempts.append({"game_pk": game_pk, "ledger_action": "REJECTED_BULLPEN_HISTORY_STALE", "retry_status": "RETRYABLE_SAME_DAY"})
            continue
        features = context["model_features"]
        mu = float(structural.score(pd.DataFrame([features]), artifact)[0])
        line = raw_prediction.get("total_line")
        probabilities, mass, central_median = probability_fields(mu, float(artifact["dispersion_alpha"]), float(line) if line is not None else None)
        team_baseline = 0.5 * sum(float(features[name]) for name in ("home_offense", "away_offense", "home_prevention", "away_prevention"))
        prediction = {
            "experiment": EXPERIMENT, "game_date": game_date, "game_pk": game_pk,
            "away_team_id": raw_prediction["away_team_id"], "away_team": raw_prediction["away_team"],
            "home_team_id": raw_prediction["home_team_id"], "home_team": raw_prediction["home_team"],
            "scheduled_start_utc": source["scheduled_start_utc"], "prediction_timestamp_utc": observed,
            "source_state_timestamp_utc": source["raw_prediction_timestamp_utc"], "scoring_run_tag": run_tag,
            "scoring_mode": scoring_mode, "prediction_snapshot_class": SNAPSHOT_CLASS,
            "model_name": MODEL_NAME, "model_hash": MODEL_HASH, "artifact_sha256": ARTIFACT_SHA256,
            "feature_contract_hash": FEATURE_CONTRACT_HASH, "feature_state_hash": source["feature_state_hash"],
            "source_raw_identity": source["raw_identity"], "source_raw_prediction_sha256": source["raw_prediction_sha256"],
            "source_raw_context_sha256": source["raw_context_sha256"], "schedule_source_sha256": source["schedule_source_hash"],
            "market_source_sha256": source["market_source_hash"],
            "away_probable_starter_id": raw_prediction.get("away_probable_starter_id"),
            "away_probable_starter_name": raw_prediction.get("away_probable_starter_name"),
            "away_starter_state_status": raw_prediction.get("away_starter_state_status"),
            "away_starter_fallback_status": raw_prediction.get("away_starter_fallback_status"),
            "home_probable_starter_id": raw_prediction.get("home_probable_starter_id"),
            "home_probable_starter_name": raw_prediction.get("home_probable_starter_name"),
            "home_starter_state_status": raw_prediction.get("home_starter_state_status"),
            "home_starter_fallback_status": raw_prediction.get("home_starter_fallback_status"),
            "bullpen_history_cutoff": context["bullpen_history_provenance"]["latest_completed_game_date"],
            "away_bullpen_freshness": context["away_bullpen_state"]["freshness_status"],
            "home_bullpen_freshness": context["home_bullpen_state"]["freshness_status"],
            "away_bullpen_source_hash": context["away_bullpen_state"]["source_hash"],
            "home_bullpen_source_hash": context["home_bullpen_state"]["source_hash"],
            "park_context_status": context["park_state"]["fallback_status"],
            "park_state_hash": context["park_state"].get("state_hash"),
            "expected_total_mean": mu, "central_total_median": central_median,
            "mae_optimal_point": central_median, "dispersion_alpha": float(artifact["dispersion_alpha"]),
            "probability_distribution_0_to_30plus": mass.tolist(), "governed_total_line": line,
            **probabilities,
            "comparator_raw_expected_total_mean": float(raw_prediction["expected_total"]),
            "comparator_prior_population_baseline": float(features["league_total"]),
            "comparator_team_shrunk_baseline": team_baseline,
            "v1_intercept_policy": "DO_NOT_APPLY_RAW_INTERCEPT_TO_C", "raw_intercept_applied_to_c": False,
            "evidence_regime": "C_SHADOW_PRIMARY_2026_REGIME",
            "regime_classification": "LATE_SEASON_TRANSITION_WATCH",
            "grading_status": "UNGRADED_OUTCOME_SEPARATE_SIDECAR",
            "outcomes_accessed_during_prediction": 0, "public_status": "PRIVATE_SHADOW_ONLY_NOT_PUBLIC",
        }
        action, context_action = append_prediction_with_context(connection, prediction, context)
        attempts.append({"game_pk": game_pk, "ledger_action": action, "context_action": context_action, "source_raw_identity": source["raw_identity"]})
    watches = watch_payload(game_date, run_tag, observed, source_rows, artifact, raw_attempts)
    watch_action = append_watch_observation(connection, watches)
    after = counts(connection)
    rows = predictions_for_date(connection, game_date)
    return {
        "status": "TOTALS_C_LIVE_SHADOW_SCORE_COMPLETE", "game_date": game_date, "scoring_mode": scoring_mode,
        "scoring_run_tag": run_tag, "source_raw_rows": len(source_rows), "rows": len(rows),
        "new_rows": sum(row["ledger_action"] == "APPENDED_NEW" for row in attempts), "attempts": attempts,
        "deployment_watch_status": watches["deployment_watch_status"], "regime_classification": watches["regime_classification"],
        "watch_action": watch_action, "ledger_before": before, "ledger_after": after,
        "model_name": MODEL_NAME, "model_hash": MODEL_HASH, "artifact_sha256": ARTIFACT_SHA256,
        "raw_control_hash": RAW_MODEL_HASH, "outcomes_accessed": 0, "public_side_effects": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True); parser.add_argument("--mode", required=True, choices=("PRIMARY_SCORE", "SCORE_MISSING"))
    parser.add_argument("--run-tag", required=True); parser.add_argument("--raw-ledger-path", type=Path, default=RAW_LEDGER)
    parser.add_argument("--c-ledger-path", type=Path, default=C_LEDGER); parser.add_argument("--observed-at-utc")
    args = parser.parse_args()
    print(json.dumps(score_from_raw(args.date, args.mode, args.run_tag, args.raw_ledger_path, args.c_ledger_path, args.observed_at_utc), indent=2, default=str))


if __name__ == "__main__":
    main()
