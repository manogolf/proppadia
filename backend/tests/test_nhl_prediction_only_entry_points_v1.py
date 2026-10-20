"""Offline contract tests for general Points/Saves prediction-only entry points."""
from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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
        class FixtureDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = datetime.fromisoformat("2026-09-21T18:01:00+00:00")
                return value.astimezone(tz) if tz else value.replace(tzinfo=None)
        self.enterContext(patch.object(core, "datetime", FixtureDateTime))
        self.enterContext(patch.object(observer, "require_slate_history_certificate", return_value={
            "status": "CERTIFIED_FINAL", "slate_date": "2026-09-20",
        }))
        self.enterContext(patch.object(observer, "require_slate_roster_observation", return_value={
            "status": "VERIFIED_CURRENT_SLATE_ROSTER", "slate_date": SLATE,
        }))

    def tearDown(self):
        self.tmp.cleanup()

    def points_patches(self, decision="PASS_LADDER_COHERENCE"):
        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(core, "validate_points_inputs"))
        stack.enter_context(patch.object(core, "verify_points_parity", return_value={"status": "EXACT_BYTE_PARITY"}))
        stack.enter_context(patch.object(core, "verify_points_identity", return_value={
            "model_version": "fixture_v1",
            "feature_contract": {"version": "FIXTURE_CONTRACT",
                                  "training_semantics_comparability": "FROZEN_CONTRACT"},
            "feature_contract_sha256": "a" * 64,
            "feature_construction": {"sha256": "b" * 64},
        }))
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
        self.assertEqual(metadata["feature_contract_version"], "FIXTURE_CONTRACT")
        self.assertEqual(metadata["feature_contract_sha256"], "a" * 64)
        self.assertEqual(pd.read_csv(destination / "market_qualified_population.csv").shape[0], 0)
        self.assertEqual(pd.read_csv(destination / "candidate_population.csv").shape[0], 0)

    @unittest.skipUnless(
        (Path(__file__).resolve().parents[2] / "backend/nhl/models/latest/points").is_dir(),
        "frozen Phoenix Points model files are local artifacts")
    def test_explicit_corrected_shadow_contract_is_published_with_model_lineage(self):
        identity_path = Path(__file__).resolve().parents[2] / (
            "backend/nhl/points_shadow/phoenix_shadow_attempt_missingness_v1.json")
        identity = json.loads(identity_path.read_text())
        identity["_identity_path"] = str(identity_path.resolve())
        with patch.object(core, "validate_points_inputs"), \
             patch.object(core, "score_points", return_value=point_scores()), \
             patch.object(core, "evaluate_ladder_coherence", return_value=ladder()):
            destination, status = core.publish_points(
                games=games(), players=points_features(), output_root=self.root / "explicit-points",
                season=2026, slate_date=SLATE, phase="FINAL_PREGAME",
                observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                canonical_run_identifier="explicit-shadow-contract",
                feature_contract_identity=identity,
            )
        metadata = json.loads((destination / "run_metadata.json").read_text())
        self.assertEqual(status, "COMPLETE_NEW_APPEND_ONLY")
        self.assertEqual(metadata["feature_contract_version"],
                         "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2_ATTEMPT_MISSINGNESS_V1")
        self.assertEqual(metadata["feature_construction_sha256"],
                         identity["feature_construction"]["sha256"])
        self.assertEqual(metadata["training_semantics_comparability"],
                         identity["feature_contract"]["training_semantics_comparability"])

    def test_empty_slate_is_a_governed_noop(self):
        now = datetime.fromisoformat(OBSERVED.replace("Z", "+00:00"))
        with patch.object(observer, "export_lane_inputs", return_value=(games().iloc[0:0], pd.DataFrame())), \
             patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")):
            status = observer.observe(lane="POINTS", season=2026, slate_date=SLATE,
                                      requested_phase="MIDDAY", observation_timestamp=now,
                                      canonical_run_identifier="empty", dsn="fixture",
                                      output_root=self.root / "empty")
        self.assertEqual(json.loads(status.read_text())["status"], "VALID_EMPTY_SLATE")

    def test_started_game_is_excluded_while_future_game_remains_eligible(self):
        slate = pd.concat([
            games("2026-09-21T17:00:00Z"),
            games("2026-09-21T21:00:00Z").assign(game_id=2026010011),
        ], ignore_index=True)
        observation, cutoff, eligible, excluded = core._validate_common(
            games=slate, season=2026, slate_date=SLATE, phase="MIDDAY",
            observation_timestamp_utc=OBSERVED,
            input_cutoff_timestamp_utc=OBSERVED, canonical_run_identifier="partial")
        self.assertEqual(eligible, [2026010011])
        self.assertEqual(excluded, [{"game_id": 2026010010, "reason": "GAME_ALREADY_STARTED"}])
        self.assertEqual(observation, OBSERVED)
        self.assertEqual(cutoff, OBSERVED)

    def test_no_eligible_games_and_retrospective_dates_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "NO_ELIGIBLE_PREGAME_GAMES"):
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
        self.assertEqual(payload["status"], "NOOP_NO_ELIGIBLE_PREGAME_GAMES")
        self.assertEqual(payload["started_excluded_games"], [
            {"game_id": 2026010010, "reason": "GAME_ALREADY_STARTED"},
        ])

    def test_staggered_slate_preserves_future_game_eligibility(self):
        now = datetime.fromisoformat("2026-10-17T18:19:54+00:00")
        slate = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": "2026-10-17", "game_id": 2026020100 + index,
            "scheduled_start_time_utc": (now - timedelta(minutes=79) if index == 0 else
                                          now + timedelta(minutes=10 * index)).isoformat(),
            "game_type_code": 2,
        } for index in range(14)])
        eligible = slate[pd.to_datetime(slate.scheduled_start_time_utc, utc=True) > pd.Timestamp(now)]
        self.assertEqual(len(eligible), 13)
        _, _, actual_eligible, actual_excluded = core._validate_common(
            games=slate, season=2026, slate_date="2026-10-17", phase="FINAL_PREGAME",
            observation_timestamp_utc=now.isoformat(), input_cutoff_timestamp_utc=now.isoformat(),
            canonical_run_identifier="oct17-game-level",
        )
        self.assertEqual(actual_eligible, eligible.game_id.astype(int).tolist())
        self.assertEqual(len(actual_excluded), 1)

    def test_observer_binds_points_card_to_explicit_corrected_shadow_contract(self):
        now = datetime.fromisoformat(OBSERVED.replace("Z", "+00:00"))
        captured = {}
        run = self.root / "captured" / "run_id=fixture"
        run.mkdir(parents=True)
        (run / "run_metadata.json").write_text(json.dumps({"prediction_rows": 3}))

        def publish(**kwargs):
            captured.update(kwargs)
            return run, "COMPLETE_NEW_APPEND_ONLY"

        with patch.object(observer, "export_lane_inputs", return_value=(games(), points_features())), \
             patch.object(observer, "publish_points", side_effect=publish):
            status = observer.observe(
                lane="POINTS", season=2026, slate_date=SLATE, requested_phase="FINAL_PREGAME",
                observation_timestamp=now, canonical_run_identifier="explicit-contract",
                dsn="fixture", output_root=self.root / "observer",
            )
        payload = json.loads(status.read_text())
        self.assertEqual(payload["status"], "CAPTURED")
        contract = captured["feature_contract_identity"]
        self.assertEqual(contract["feature_contract"]["version"],
                         "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2_ATTEMPT_MISSINGNESS_V1")
        self.assertEqual(contract["feature_construction"]["sha256"],
                         "61ac83bf6049bdfe5a12b895667fcbe6efa82f5aa2c960171bfcf3a95e7df43d")

    def test_duplicate_rerun_is_idempotent_and_conflicting_identity_fails(self):
        first, state1 = self.publish_points()
        second, state2 = self.publish_points()
        self.assertEqual(first, second)
        self.assertEqual(state1, "COMPLETE_NEW_APPEND_ONLY")
        self.assertEqual(state2, "IDEMPOTENT_EXISTING_ZERO_INSERTS")
        with self.assertRaisesRegex(RuntimeError, "CONFLICTING_PREDICTION_IDENTITY"):
            self.publish_points("different-run")

    def test_existing_card_with_different_feature_contract_fails_closed(self):
        self.publish_points()
        with self.assertRaisesRegex(RuntimeError, "CONFLICTING_PREDICTION_FEATURE_CONTRACT"):
            core._existing(self.root / "points", 2026, SLATE, "MIDDAY", "fixture-run",
                           feature_contract_sha256="c" * 64)

    def test_points_ladder_exclusion_is_retained_and_not_qualified(self):
        with self.assertRaisesRegex(RuntimeError, "NO_POINTS_ROWS_PASS_LADDER_COHERENCE"):
            self.publish_points(decision="BLOCKED_MATERIAL_LADDER_INCOHERENCE")

    def test_points_ladder_incoherence_excludes_player_without_blocking_other_players(self):
        players = pd.concat([points_features(), points_features().assign(
            player_id=102, player_name="Second Fixture Skater")], ignore_index=True)
        scores = pd.concat([point_scores(), point_scores().assign(player_id=102)], ignore_index=True)
        decisions = pd.concat([
            ladder("PASS_LADDER_COHERENCE"),
            ladder("BLOCKED_MATERIAL_LADDER_INCOHERENCE").assign(player_id=102),
        ], ignore_index=True)
        with self.points_patches():
            with patch.object(core, "score_points", return_value=scores), \
                 patch.object(core, "evaluate_ladder_coherence", return_value=decisions):
                destination, _ = core.publish_points(
                    games=games(), players=players, output_root=self.root / "mixed-ladder",
                    season=2026, slate_date=SLATE, phase="MIDDAY",
                    observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                    canonical_run_identifier="mixed-ladder",
                )
        predictions = pd.read_csv(destination / "immutable_predictions.csv")
        exclusions = pd.read_csv(destination / "prediction_exclusions.csv")
        self.assertEqual(set(predictions.player_id.astype(int)), {101})
        self.assertEqual(set(exclusions.player_id.astype(int)), {102})
        self.assertEqual(json.loads((destination / "run_metadata.json").read_text())[
            "coverage_status"], "PARTIAL_GAME_OR_PLAYER_COVERAGE")

    def test_points_partial_capture_keeps_only_future_game_and_records_started_exclusion(self):
        slate = pd.concat([
            games("2026-09-21T21:00:00Z"),
            games("2026-09-21T17:00:00Z").assign(game_id=2026010011),
        ], ignore_index=True)
        future_player = points_features()
        future_player["scheduled_start_time_utc"] = "2026-09-21T21:00:00Z"
        output = self.root / "partial-points"
        with self.points_patches():
            with patch.object(core, "score_points", return_value=point_scores()):
                destination, _ = core.publish_points(
                    games=slate, players=future_player, output_root=output, season=2026,
                    slate_date=SLATE, phase="MIDDAY", observation_timestamp_utc=OBSERVED,
                    input_cutoff_timestamp_utc=OBSERVED, canonical_run_identifier="partial-points",
                )
        metadata = json.loads((destination / "run_metadata.json").read_text())
        self.assertEqual(metadata["eligible_game_ids"], [2026010010])
        self.assertEqual(metadata["prediction_game_ids"], [2026010010])
        self.assertEqual(metadata["started_excluded_game_ids"], [2026010011])
        self.assertEqual(pd.read_csv(destination / "game_exclusions.csv").reason.tolist(),
                         ["GAME_ALREADY_STARTED"])

    def test_missing_roster_identity_game_is_explicitly_excluded(self):
        slate = pd.concat([
            games("2026-09-21T20:00:00Z"),
            games("2026-09-21T21:00:00Z").assign(game_id=2026010011),
        ], ignore_index=True)
        slate.attrs["roster_identity_scope_known"] = True
        slate.attrs["roster_identity_game_ids"] = [2026010010]
        output = self.root / "missing-roster-game"
        with self.points_patches():
            with patch.object(core, "score_points", return_value=point_scores()):
                destination, _ = core.publish_points(
                    games=slate, players=points_features(), output_root=output, season=2026,
                    slate_date=SLATE, phase="MIDDAY", observation_timestamp_utc=OBSERVED,
                    input_cutoff_timestamp_utc=OBSERVED, canonical_run_identifier="missing-roster",
                )
        exclusions = pd.read_csv(destination / "game_exclusions.csv")
        self.assertEqual(exclusions.to_dict("records"), [{
            "game_id": 2026010011,
            "reason": "NO_ACTIVE_CUTOFF_BOUNDED_ROSTER_IDENTITIES",
        }])

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

    def test_saves_partial_capture_keeps_future_game_and_excludes_started_game(self):
        slate = pd.concat([
            games("2026-09-21T21:00:00Z"),
            games("2026-09-21T17:00:00Z").assign(game_id=2026010011),
        ], ignore_index=True)
        goalies = goalie_features()
        goalies["scheduled_start_time_utc"] = "2026-09-21T21:00:00Z"
        with self.saves_patches():
            destination, _ = core.publish_saves(
                games=slate, goalies=goalies, output_root=self.root / "partial-saves",
                season=2026, slate_date=SLATE, phase="FINAL_PREGAME",
                observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                canonical_run_identifier="partial-saves",
            )
        metadata = json.loads((destination / "run_metadata.json").read_text())
        self.assertEqual(metadata["prediction_game_ids"], [2026010010])
        self.assertEqual(metadata["started_excluded_game_ids"], [2026010011])
        predictions = pd.read_csv(destination / "immutable_conditional_predictions.csv")
        self.assertTrue(predictions.starter_state.eq(
            "UNKNOWN_NO_AUTHORIZED_PREGAME_STARTER_SOURCE").all())
        self.assertFalse(predictions.selected_starter.any())

    def test_missing_goalie_history_is_explicitly_excluded_per_game(self):
        slate = pd.concat([
            games("2026-09-21T20:00:00Z"),
            games("2026-09-21T21:00:00Z").assign(game_id=2026010011),
        ], ignore_index=True)
        slate.attrs["roster_identity_scope_known"] = True
        slate.attrs["roster_identity_game_ids"] = [2026010010, 2026010011]
        goalies = goalie_features()
        goalies.attrs["input_exclusions"] = [{
            "game_id": 2026010011, "player_id": 202,
            "reason": "NO_QUALIFIED_LANE_FEATURE_HISTORY",
        }]
        with self.saves_patches():
            destination, _ = core.publish_saves(
                games=slate, goalies=goalies, output_root=self.root / "missing-goalie-history",
                season=2026, slate_date=SLATE, phase="FINAL_PREGAME",
                observation_timestamp_utc=OBSERVED, input_cutoff_timestamp_utc=OBSERVED,
                canonical_run_identifier="missing-goalie-history",
            )
        exclusions = pd.read_csv(destination / "game_exclusions.csv")
        self.assertEqual(exclusions.to_dict("records"), [{
            "game_id": 2026010011, "reason": "NO_QUALIFIED_SAVES_FEATURE_INPUTS",
        }])
        self.assertEqual(pd.read_csv(destination / "input_exclusions.csv").iloc[0].reason,
                         "NO_QUALIFIED_LANE_FEATURE_HISTORY")

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
        self.assertEqual(result["POINTS"]["status"], "FAILED_BEFORE_RECEIPT")
        self.assertIn("POINTS_FIXTURE_FAILURE", result["POINTS"]["failure"]["message"])
        self.assertEqual(order, ["POINTS", "SAVES", "SOG"])

        call_order = []
        database_accesses = []
        with patch("sys.argv", ["runner", "--slate-date", SLATE, "--env-file", str(self.root / "absent")]), \
             patch.object(mainline, "load_env"), \
             patch.object(mainline, "database_url",
                          side_effect=lambda access: database_accesses.append(access) or "fixture-dsn"), \
             patch.object(mainline, "observe_prediction_only_lanes", side_effect=lambda *a: call_order.append("predictions")), \
             patch.object(mainline, "observe", side_effect=lambda *a, **kwargs: (call_order.append("mainline_observer") or self.root / "status.json")), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(mainline.main(), 0)
        self.assertEqual(call_order, ["predictions", "mainline_observer"])
        self.assertEqual(database_accesses, ["research", "research"])


if __name__ == "__main__":
    unittest.main()
