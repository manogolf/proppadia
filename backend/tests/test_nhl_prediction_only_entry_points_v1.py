"""Offline contract tests for general Points/Saves prediction-only entry points."""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.prediction_only import core
from backend.nhl.scripts import nhl_prediction_only_common as observer
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as mainline


SLATE = "2026-09-21"
OBSERVED = "2026-09-21T18:00:00Z"
START = "2026-09-21T23:00:00Z"


def games(start=START):
    return pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010010,
        "home_team_id": 1, "home_team": "AAA", "away_team_id": 2,
        "away_team": "BBB", "scheduled_start_time_utc": start,
        "game_type_code": 1, "game_status": "FUT",
    }])


def points_features():
    return pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010010,
        "player_id": 101, "player_name": "Fixture Skater", "team": "AAA",
        "opponent": "BBB", "scheduled_start_time_utc": START, "game_type_code": 1,
        "feature_cutoff_timestamp_utc": OBSERVED,
        "feature_history_max_timestamp_utc": "2026-04-15T23:00:00Z",
        "pregame_participation_state": "ACTIVE",
        "roster_source_timestamp_utc": "2026-09-21T17:55:00Z",
    }])


def goalie_features():
    return pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010010,
        "goalie_id": 201, "goalie_name": "Fixture Goalie", "team": "AAA",
        "opponent": "BBB", "scheduled_start_time_utc": START, "game_type_code": 1,
        "feature_cutoff_timestamp_utc": OBSERVED,
        "feature_history_max_timestamp_utc": "2026-04-15T23:00:00Z",
        "goalie_eligibility_state": "ACTIVE_ROSTER_STARTER_UNKNOWN",
        "roster_source_timestamp_utc": "2026-09-21T17:55:00Z",
        "population_contract": "COMPLETE_SCORER_ELIGIBLE", "scorer_eligible": True,
        "expected_complete_population_rows": 1,
    }])


def point_scores():
    return pd.DataFrame([
        {"player_id": 101, "game_id": 2026010010, "line": line,
         "prob_over": probability, "model": "fixture_points"}
        for line, probability in ((0.5, .7), (1.5, .5), (2.5, .3))
    ])


def ladder(decision="PASS_LADDER_COHERENCE"):
    return pd.DataFrame([{
        "game_id": 2026010010, "player_id": 101,
        "ladder_coherence_decision": decision, "ladder_coherence_reason": "FIXTURE",
        "maximum_adjacent_crossing_probability": 0.0,
        "maximum_adjacent_crossing_pp": 0.0,
    }])


class PredictionOnlyEntryPointTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="nhl_prediction_only_")
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def points_patches(self, decision="PASS_LADDER_COHERENCE"):
        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(core, "validate_points_inputs"))
        stack.enter_context(patch.object(core, "verify_points_parity", return_value={"status": "EXACT_BYTE_PARITY"}))
        stack.enter_context(patch.object(core, "verify_points_identity", return_value={"model_version": "fixture_v1"}))
        stack.enter_context(patch.object(core, "score_points", return_value=point_scores()))
        stack.enter_context(patch.object(core, "evaluate_ladder_coherence", return_value=ladder(decision)))
        return stack

    def saves_patches(self):
        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(core, "validate_saves_inputs"))
        stack.enter_context(patch.object(core, "verify_saves_identity", return_value={
            "lines": [18.5], "model_version": "fixture_saves_v1",
            "operational_amendment": {"semantic_contract": "P(saves over | named goalie starts)"},
        }))
        stack.enter_context(patch.object(core, "verify_saves_parity", return_value={"status": "EXACT_BYTE_PARITY"}))
        stack.enter_context(patch.object(core, "verify_operational_amendment", return_value={"status": "BOUNDED"}))
        stack.enter_context(patch.object(core, "score_saves", return_value=(pd.DataFrame([{
            "game_id": 2026010010, "goalie_id": 201, "line": 18.5,
            "expected_saves": 24.0, "raw_prob_over": .8, "prob_over": .8, "prob_under": .2,
        }]), pd.DataFrame())))
        return stack

    def publish_points(self, identity="fixture-run", decision="PASS_LADDER_COHERENCE"):
        with self.points_patches(decision):
            return core.publish_points(
                games=games(), players=points_features(), output_root=self.root / "points",
                season=2026, slate_date=SLATE, phase="MIDDAY",
                observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                canonical_run_identifier=identity,
            )

    def test_valid_nonempty_points_snapshot_has_no_market_surface(self):
        with patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch.dict(os.environ, {"ODDS_API_KEY": "fixture-secret"}, clear=False):
            destination, status = self.publish_points()
        self.assertEqual(status, "COMPLETE_NEW_APPEND_ONLY")
        metadata = json.loads((destination / "run_metadata.json").read_text())
        self.assertEqual(metadata["market_requests"], 0)
        self.assertFalse(metadata["bookmaker_credential_access"])
        self.assertEqual(pd.read_csv(destination / "market_qualified_population.csv").shape[0], 0)
        self.assertEqual(pd.read_csv(destination / "candidate_population.csv").shape[0], 0)

    def test_empty_slate_is_a_governed_noop(self):
        now = datetime.fromisoformat(OBSERVED.replace("Z", "+00:00"))
        with patch.object(observer, "export_lane_inputs", return_value=(games().iloc[0:0], pd.DataFrame())), \
             patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")):
            status = observer.observe(lane="POINTS", season=2026, slate_date=SLATE,
                                      requested_phase="MIDDAY", observation_timestamp=now,
                                      canonical_run_identifier="empty", dsn="fixture",
                                      output_root=self.root / "empty")
        self.assertEqual(json.loads(status.read_text())["status"], "VALID_EMPTY_SLATE")

    def test_poststart_and_retrospective_partial_slate_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "WHOLE_SLATE_PRESTART_GATE_FAILED"):
            core._validate_common(games=games("2026-09-21T17:00:00Z"), season=2026,
                                  slate_date=SLATE, phase="MIDDAY",
                                  observation_timestamp_utc=OBSERVED,
                                  input_cutoff_timestamp_utc=OBSERVED,
                                  canonical_run_identifier="poststart")
        with self.assertRaisesRegex(RuntimeError, "RETROSPECTIVE_PREDICTION_FORBIDDEN"):
            core._validate_common(games=games().assign(slate_date="2026-09-20"), season=2026,
                                  slate_date="2026-09-20", phase="MIDDAY",
                                  observation_timestamp_utc="2026-09-20T18:00:00Z",
                                  input_cutoff_timestamp_utc="2026-09-20T18:00:00Z",
                                  canonical_run_identifier="retrospective")

    def test_explicit_observer_reports_poststart_failure_not_noop(self):
        now = datetime.fromisoformat(OBSERVED.replace("Z", "+00:00"))
        with patch.object(observer, "export_lane_inputs",
                          return_value=(games("2026-09-21T17:00:00Z"), points_features())):
            status = observer.observe(lane="POINTS", season=2026, slate_date=SLATE,
                                      requested_phase="MIDDAY", observation_timestamp=now,
                                      canonical_run_identifier="poststart-observer", dsn="fixture",
                                      output_root=self.root / "poststart")
        payload = json.loads(status.read_text())
        self.assertEqual(payload["status"], "FAILED_WARN_ONLY")
        self.assertIn("WHOLE_SLATE_PRESTART_GATE_FAILED", payload["failure"])

    def test_duplicate_rerun_is_idempotent_and_conflicting_identity_fails(self):
        first, state1 = self.publish_points()
        second, state2 = self.publish_points()
        self.assertEqual(first, second)
        self.assertEqual(state1, "COMPLETE_NEW_APPEND_ONLY")
        self.assertEqual(state2, "IDEMPOTENT_EXISTING_ZERO_INSERTS")
        with self.assertRaisesRegex(RuntimeError, "CONFLICTING_PREDICTION_IDENTITY"):
            self.publish_points("different-run")

    def test_points_ladder_exclusion_is_retained_and_not_qualified(self):
        destination, _ = self.publish_points(decision="BLOCKED_MATERIAL_LADDER_INCOHERENCE")
        predictions = pd.read_csv(destination / "immutable_predictions.csv")
        self.assertFalse(predictions.prediction_eligible.any())
        self.assertEqual(len(pd.read_csv(destination / "prediction_exclusions.csv")), 1)
        self.assertFalse(predictions.market_qualified.any())

    def test_saves_snapshot_preserves_unknown_starter_and_conditional_semantics(self):
        with self.saves_patches(), patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch.dict(os.environ, {}, clear=True):
            destination, state = core.publish_saves(
                games=games(), goalies=goalie_features(), output_root=self.root / "saves",
                season=2026, slate_date=SLATE, phase="FINAL_PREGAME",
                observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                canonical_run_identifier="saves-fixture",
            )
        self.assertEqual(state, "COMPLETE_NEW_APPEND_ONLY")
        predictions = pd.read_csv(destination / "immutable_conditional_predictions.csv")
        self.assertTrue(predictions.starter_state.eq("UNKNOWN_NO_AUTHORIZED_PREGAME_STARTER_SOURCE").all())
        self.assertFalse(predictions.selected_starter.any())
        self.assertFalse(predictions.market_qualified.any())
        self.assertEqual(len(pd.read_csv(destination / "starter_selections.csv")), 0)

    def test_missing_market_directory_and_credentials_do_not_block_either_lane(self):
        with patch.dict(os.environ, {}, clear=True):
            self.publish_points()
            with self.saves_patches():
                destination, _ = core.publish_saves(
                    games=games(), goalies=goalie_features(), output_root=self.root / "saves-no-market",
                    season=2026, slate_date=SLATE, phase="MIDDAY",
                    observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                    canonical_run_identifier="saves-no-market",
                )
        self.assertTrue((destination / "RUN_COMPLETE.json").is_file())

    def test_lane_failure_isolated_and_all_predictions_precede_market_gate(self):
        order = []
        def independent(**kwargs):
            order.append(kwargs["lane"])
            if kwargs["lane"] == "POINTS":
                raise RuntimeError("POINTS_FIXTURE_FAILURE")
            return self.root / "saves-status.json"
        def sog(**kwargs):
            order.append("SOG")
            return self.root / "sog-status.json"
        with patch.object(mainline, "observe_independent_prediction_only", side_effect=independent), \
             patch.object(mainline, "observe_sog_prediction_only", side_effect=sog):
            result = mainline.observe_prediction_only_lanes(SLATE, "MIDDAY", "fixture", datetime.now(timezone.utc))
        self.assertTrue(result["POINTS"].startswith("FAILED_WARN_ONLY"))
        self.assertEqual(order, ["POINTS", "SAVES", "SOG"])

        call_order = []
        with patch("sys.argv", ["runner", "--slate-date", SLATE, "--env-file", str(self.root / "absent")]), \
             patch.object(mainline, "load_env"), \
             patch.object(mainline, "observe_prediction_only_lanes", side_effect=lambda *a: call_order.append("predictions")), \
             patch.object(mainline, "morning_capture_allowed", side_effect=lambda *a: (call_order.append("market_gate") or (False, "fixture"))), \
             patch.object(mainline, "record_morning_not_ready", return_value=self.root / "status.json"), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(mainline.main(), 0)
        self.assertEqual(call_order, ["predictions", "market_gate"])


if __name__ == "__main__":
    unittest.main()
