from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_c_shadow_daily_v1 as daily
from backend.mlb.scripts import run_mlb_totals_c_shadow_v1 as scorer
from backend.mlb.totals_predictions import c_shadow_v1 as ledger
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    append_outcome as append_raw_outcome,
    append_prediction_with_context as append_raw_prediction,
    canonical_identity as raw_identity,
    connect_ledger as connect_raw,
    payload_hash as raw_hash,
)


ROOT = Path(__file__).resolve().parents[3]
MONEYLINE = ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"
LAUNCH_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_live_shadow_launch_v1/2026-08-16"


def raw_fixture(raw_path: Path, game_pk: int = 900001, *, scheduled: str = "2026-08-17T23:00:00Z", fresh: bool = True):
    artifact = scorer.load_artifact()
    features = dict(zip(artifact["feature_order"], artifact["scaler_mean"]))
    features.update({"home_starter_prior_starts": 3.0, "away_starter_prior_starts": 3.0, "park_history_depth": 100.0})
    context = {
        "model_features": features,
        "away_starter_state": {"certification_status": "STRICT_PRIOR_STARTER_STATE", "fallback_tier": "DIRECT_STARTER_HISTORY"},
        "home_starter_state": {"certification_status": "STRICT_PRIOR_STARTER_STATE", "fallback_tier": "DIRECT_STARTER_HISTORY"},
        "away_bullpen_state": {"freshness_status": "CURRENT_STRICT_PRIOR_HISTORY" if fresh else "STALE_OR_INCOMPLETE_HISTORY", "source_hash": "a" * 64},
        "home_bullpen_state": {"freshness_status": "CURRENT_STRICT_PRIOR_HISTORY" if fresh else "STALE_OR_INCOMPLETE_HISTORY", "source_hash": "b" * 64},
        "bullpen_history_provenance": {"latest_completed_game_date": "2026-08-16", "feature_generation": "BULLPEN_RECENCY_FRESHNESS_INVARIANT_V1"},
        "park_state": {"fallback_status": "DIRECT_REGRESSED_PARK_HISTORY", "state_hash": "c" * 64},
        "dynamic_league_environment": {"season_to_date_league_rpg": 9.0},
    }
    prediction = {
        "experiment": "MLB_TOTALS_PROSPECTIVE_SHADOW_V1", "game_date": "2026-08-17", "game_pk": game_pk,
        "scheduled_start_utc": scheduled, "prediction_timestamp_utc": "2026-08-17T12:30:00Z",
        "away_team_id": 1, "away_team": "Away", "home_team_id": 2, "home_team": "Home",
        "away_probable_starter_id": 10, "away_probable_starter_name": "Away Starter",
        "away_starter_state_status": "STRICT_PRIOR_STARTER_STATE", "away_starter_fallback_status": "DIRECT_STARTER_HISTORY",
        "home_probable_starter_id": 20, "home_probable_starter_name": "Home Starter",
        "home_starter_state_status": "STRICT_PRIOR_STARTER_STATE", "home_starter_fallback_status": "DIRECT_STARTER_HISTORY",
        "model_version": "DIRECT_NEGATIVE_BINOMIAL", "model_hash": scorer.RAW_MODEL_HASH,
        "expected_total": 8.4, "total_line": 8.5, "market_source_sha256": "d" * 64,
        "feature_state_hash": raw_hash(context), "schedule_source_sha256": "e" * 64,
        "prediction_snapshot_class": "DAILY_DESIGNATED_PREGAME", "context_quality_state": "TOTALS_CONTEXT_COMPLETE",
        "grading_status": "UNGRADED_OUTCOME_SEPARATE_LEDGER",
    }
    connection = connect_raw(raw_path)
    assert append_raw_prediction(connection, prediction, context) == ("APPENDED_NEW", "APPENDED_NEW")
    return prediction, context


def test_exact_frozen_subject_and_no_raw_intercept():
    artifact = scorer.load_artifact()
    assert artifact["candidate_identity"] == ledger.MODEL_NAME
    assert artifact["canonical_model_hash"] == ledger.MODEL_HASH
    assert hashlib.sha256(scorer.ARTIFACT.read_bytes()).hexdigest() == scorer.ARTIFACT_SHA256
    assert scorer.FEATURE_CONTRACT_HASH == "d7551fd7798aa60ada1b96831e32bcb7748a17aabf67f53c8800f24c9f4a0927"


def test_primary_score_uses_exact_raw_context_and_freezes_mean_median_distribution(tmp_path):
    raw_path, c_path = tmp_path / "raw.sqlite3", tmp_path / "c.sqlite3"
    raw_prediction, context = raw_fixture(raw_path)
    result = scorer.score_from_raw("2026-08-17", "PRIMARY_SCORE", "run-0530", raw_path, c_path, "2026-08-17T12:31:00Z")
    assert result["new_rows"] == result["rows"] == 1 and result["outcomes_accessed"] == 0
    connection = ledger.connect_ledger(c_path)
    row = ledger.predictions_for_date(connection, "2026-08-17")[0]
    assert row["source_raw_identity"] == raw_identity("2026-08-17", 900001)
    assert row["feature_state_hash"] == raw_hash(context)
    assert row["source_raw_prediction_sha256"] == raw_hash(raw_prediction)
    assert row["expected_total_mean"] == pytest.approx(float(__import__("math").exp(scorer.load_artifact()["intercept"])))
    assert isinstance(row["central_total_median"], int) and row["mae_optimal_point"] == row["central_total_median"]
    assert sum(row["probability_distribution_0_to_30plus"]) == pytest.approx(1.0, abs=1e-12)
    assert row["raw_intercept_applied_to_c"] is False
    assert row["v1_intercept_policy"] == "DO_NOT_APPLY_RAW_INTERCEPT_TO_C"
    assert row["outcomes_accessed_during_prediction"] == 0


def test_duplicate_identity_is_immutable_and_missing_only_adds_only_new_raw_identity(tmp_path):
    raw_path, c_path = tmp_path / "raw.sqlite3", tmp_path / "c.sqlite3"
    raw_fixture(raw_path)
    first = scorer.score_from_raw("2026-08-17", "PRIMARY_SCORE", "run-0530", raw_path, c_path, "2026-08-17T12:31:00Z")
    repeat = scorer.score_from_raw("2026-08-17", "SCORE_MISSING", "run-0830", raw_path, c_path, "2026-08-17T15:31:00Z")
    raw_fixture(raw_path, 900002)
    retry = scorer.score_from_raw("2026-08-17", "SCORE_MISSING", "run-1100", raw_path, c_path, "2026-08-17T18:01:00Z")
    assert first["new_rows"] == 1 and repeat["new_rows"] == 0 and retry["new_rows"] == 1
    assert ledger.counts(ledger.connect_ledger(c_path))["duplicate_prediction_identities"] == 0


def test_post_start_and_pre_start_date_admission_guards(tmp_path):
    raw_path, c_path = tmp_path / "raw.sqlite3", tmp_path / "c.sqlite3"
    raw_fixture(raw_path, scheduled="2026-08-17T13:00:00Z")
    result = scorer.score_from_raw("2026-08-17", "PRIMARY_SCORE", "late", raw_path, c_path, "2026-08-17T13:00:01Z")
    assert result["new_rows"] == 0 and result["attempts"][0]["rejection_reason"] == "PREGAME_CUTOFF_FAILED"
    before = scorer.score_from_raw("2026-08-16", "PRIMARY_SCORE", "before", raw_path, c_path, "2026-08-16T12:00:00Z")
    assert before == {"status": "C_LIVE_SHADOW_NOT_STARTED", "game_date": "2026-08-16", "new_rows": 0, "rows": 0}


def test_stale_bullpen_source_fails_closed_and_remains_retryable(tmp_path):
    raw_path, c_path = tmp_path / "raw.sqlite3", tmp_path / "c.sqlite3"
    raw_fixture(raw_path, fresh=False)
    result = scorer.score_from_raw("2026-08-17", "PRIMARY_SCORE", "stale", raw_path, c_path, "2026-08-17T12:31:00Z")
    assert result["new_rows"] == 0
    assert result["attempts"][0]["ledger_action"] == "REJECTED_BULLPEN_HISTORY_STALE"
    assert result["attempts"][0]["retry_status"] == "RETRYABLE_SAME_DAY"


def test_outcome_sidecar_is_separate_and_prediction_trigger_is_append_only(tmp_path):
    raw_path, c_path = tmp_path / "raw.sqlite3", tmp_path / "c.sqlite3"
    raw_prediction, _ = raw_fixture(raw_path)
    scorer.score_from_raw("2026-08-17", "PRIMARY_SCORE", "run-0530", raw_path, c_path, "2026-08-17T12:31:00Z")
    raw_connection = connect_raw(raw_path)
    raw_grade = {
        "game_date": "2026-08-17", "game_pk": 900001, "official_final_total": 9, "regulation_nine_total": 9,
        "official_source_path": "official.json", "official_source_hash": "f" * 64, "official_status": "Final",
        "model_hash": scorer.RAW_MODEL_HASH, "expected_total": raw_prediction["expected_total"],
    }
    assert append_raw_outcome(raw_connection, raw_identity("2026-08-17", 900001), raw_grade, "2026-08-18T12:00:00Z") == "APPENDED_NEW"
    c_connection = ledger.connect_ledger(c_path)
    prediction_before = ledger.predictions_for_date(c_connection, "2026-08-17")
    result = daily.grade_date("2026-08-17", c_path, raw_path)
    assert result["new_outcome_rows"] == 1 and result["prediction_rows_unchanged"] is True
    assert ledger.predictions_for_date(c_connection, "2026-08-17") == prediction_before
    assert ledger.outcomes_for_date(c_connection, "2026-08-17")[0]["official_final_total"] == 9
    with pytest.raises(sqlite3.DatabaseError, match="APPEND_ONLY_C_PREDICTION_LEDGER"):
        c_connection.execute("UPDATE totals_c_shadow_predictions SET game_pk=1")


def test_watch_and_regime_schema_defaults_to_normal_when_metadata_is_unavailable(tmp_path):
    raw_path, c_path = tmp_path / "raw.sqlite3", tmp_path / "c.sqlite3"
    raw_fixture(raw_path)
    result = scorer.score_from_raw("2026-08-17", "PRIMARY_SCORE", "run-0530", raw_path, c_path, "2026-08-17T12:31:00Z")
    assert result["regime_classification"] == "NORMAL_COMPETITIVE_REGIME"
    assert result["C_REGIME"] == "NORMAL"
    connection = ledger.connect_ledger(c_path)
    watch = connection.execute("SELECT watch_payload_json FROM totals_c_shadow_watch_observations").fetchone()[0]
    assert all(label in watch for label in ("A_BULLPEN_SOURCE_FRESHNESS", "I_MODEL_HASH_INTEGRITY", "performance_used"))
    payload = json.loads(watch)
    assert payload["regime_evidence"]["missing_metadata_triggered_watch"] is False
    assert ledger.predictions_for_date(connection, "2026-08-17")[0]["regime_classification"] == "NORMAL_COMPETITIVE_REGIME"


def test_regime_classification_requires_affirmative_nonperformance_evidence():
    missing_only = {
        "affirmative_transition_evidence": [], "affirmative_distinct_evidence": [],
        "unavailable_metadata": ["mathematical_elimination_status"],
        "ordinary_game_conditions": [],
    }
    weather_only = {
        "affirmative_transition_evidence": [], "affirmative_distinct_evidence": [],
        "unavailable_metadata": [],
        "ordinary_game_conditions": ["RAIN_DELAY", "EXTRA_INNINGS", "COMPLETED_UNUSUAL_GAME"],
    }
    transition = {
        "affirmative_transition_evidence": [{
            "evidence_type": "ACTIVE_ROSTER_TURNOVER", "affirmative": True,
            "performance_used": False, "source": "governed_roster_snapshot",
        }],
        "affirmative_distinct_evidence": [], "unavailable_metadata": [], "ordinary_game_conditions": [],
    }
    distinct = {
        "affirmative_transition_evidence": [],
        "affirmative_distinct_evidence": [{
            "evidence_type": "MATHEMATICAL_ELIMINATION_STATUS", "affirmative": True,
            "performance_used": False, "source": "governed_standings_snapshot",
        }],
        "unavailable_metadata": [], "ordinary_game_conditions": [],
    }
    assert scorer.classify_regime(missing_only) == "NORMAL_COMPETITIVE_REGIME"
    assert scorer.classify_regime(weather_only) == "NORMAL_COMPETITIVE_REGIME"
    assert scorer.classify_regime(transition) == "LATE_SEASON_TRANSITION_WATCH"
    assert scorer.classify_regime(distinct) == "LATE_SEASON_DISTINCT_REGIME"
    assert scorer.operational_regime_label("LATE_SEASON_TRANSITION_WATCH") == "TRANSITION_WATCH"
    assert scorer.operational_regime_label("LATE_SEASON_DISTINCT_REGIME") == "LATE_SEASON_DISTINCT"
    with pytest.raises(ValueError, match="PERFORMANCE_FORBIDDEN"):
        transition["affirmative_transition_evidence"][0]["performance_used"] = True
        scorer.classify_regime(transition)


def test_hook_is_syntax_valid_natural_and_private():
    hook = ROOT / "bin/mlb_totals_prospective_shadow_daily_hook.sh"
    subprocess.run(["zsh", "-n", str(hook)], check=True)
    text = hook.read_text()
    assert "run_mlb_totals_c_shadow_daily_v1" in text
    assert "--raw-lifecycle-json" in text
    assert "MLB_PUBLIC" not in text
    assert hashlib.sha256(MONEYLINE.read_bytes()).hexdigest() == "afc257a5ede1c5bc352dcb1e990b710272d472cd62d2dadfb7dafb7254b35722"


def test_launch_package_is_complete_and_all_validations_pass():
    required = {
        "totals_c_shadow_identity.json", "totals_c_shadow_contract.md", "totals_c_shadow_snapshot_policy.md",
        "totals_c_shadow_prediction_schema.json", "totals_c_shadow_outcome_contract.md",
        "totals_c_shadow_comparator_contract.md", "totals_c_shadow_metrics_contract.md",
        "totals_c_shadow_deployment_watch_contract.md", "totals_c_shadow_regime_contract.md",
        "totals_c_shadow_review_schedule.md", "totals_c_shadow_immutability_contract.md",
        "totals_c_shadow_validation.csv", "totals_c_shadow_launch_readiness.md",
        "concise_mlb_totals_c_live_shadow_launch_v1.md", "reproducibility_hashes.sha256",
    }
    assert {path.name for path in LAUNCH_OUTPUT.iterdir()} == required
    validation = pd.read_csv(LAUNCH_OUTPUT / "totals_c_shadow_validation.csv")
    assert len(validation) == 20 and set(validation.status) == {"PASS"}
    identity = json.loads((LAUNCH_OUTPUT / "totals_c_shadow_identity.json").read_text())
    assert identity["TOTALS_C_SHADOW_MODEL_FROZEN"] is True and identity["refit_at_launch"] is False
    assert identity["pre_start_live_c_rows"] == 0
    readiness = (LAUNCH_OUTPUT / "totals_c_shadow_launch_readiness.md").read_text()
    assert "TOTALS_C_SHADOW_LAUNCH_READY_WITH_WATCH" in readiness
