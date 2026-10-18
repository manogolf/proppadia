from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.reconciliation_certificate import RosterObservationNotReady
from backend.nhl.scripts import nhl_prediction_only_common as common
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as mainline
from backend.nhl.scripts import run_nhl_sog_prediction_only_warn_only as sog


class PredictionObserverReconciliationGateTests(unittest.TestCase):
    def test_mainline_receipt_preserves_bounded_prior_day_child_diagnostics(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        child_error = mainline.PriorDayChildFailure(
            "authority_roster_acquisition", 5,
            stdout="{" + "x" * 3000,
            stderr="IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:0",
        )
        with tempfile.TemporaryDirectory() as temp, patch.object(
            mainline, "utc_now", return_value=now
        ), patch.object(mainline, "ensure_prior_day_official_outcomes",
                        side_effect=child_error), patch.object(
            mainline, "require_slate_history_certificate") as certificate, patch.object(
            mainline, "require_slate_roster_observation") as roster:
            path = mainline.observe(
                Path(temp), "2026-10-18", "AUTO", False, "research-dsn")
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertEqual(result["prior_day_child_failure"]["stage"],
                         "authority_roster_acquisition")
        self.assertEqual(result["prior_day_child_failure"]["exit_status"], 5)
        self.assertLessEqual(len(result["prior_day_child_failure"]["stdout_tail"]), 1200)
        self.assertIn("IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:0",
                      result["prior_day_child_failure"]["stderr_tail"])
        certificate.assert_not_called()
        roster.assert_not_called()

    def test_mainline_blocks_before_history_export_without_current_roster(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temp, patch.object(
            mainline, "utc_now", return_value=now
        ), patch.object(mainline, "ensure_prior_day_official_outcomes",
                        return_value={"status": "EXISTING_GOVERNED_PACKAGE_VALID"}), patch.object(
            mainline, "require_slate_history_certificate",
            return_value={"status": "CERTIFIED_FINAL"},
        ), patch.object(
            mainline, "require_slate_roster_observation",
            side_effect=RosterObservationNotReady("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING"),
        ), patch.object(mainline, "export_inputs") as export, patch.object(
            mainline, "fetch_markets") as markets:
            path = mainline.observe(Path(temp), "2026-10-18", "AUTO", False, "research-dsn")
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "ROSTER_NOT_READY")
        self.assertTrue(result["retry_eligible_on_later_poll"])
        self.assertFalse(result["feature_export_started"])
        export.assert_not_called()
        markets.assert_not_called()

    def test_points_and_saves_block_before_feature_export_without_certificate(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        for lane in ("POINTS", "SAVES"):
            with self.subTest(lane=lane), tempfile.TemporaryDirectory() as temp, patch.object(
                common, "require_slate_history_certificate",
                side_effect=RuntimeError("PRIOR_DAY_CERTIFICATE_MISSING"),
            ), patch.object(common, "require_slate_roster_observation") as roster, patch.object(
                common, "export_lane_inputs") as export:
                path = common.observe(
                    lane=lane, season=2026, slate_date="2026-10-18",
                    requested_phase="MIDDAY", observation_timestamp=now,
                    canonical_run_identifier="test", dsn="research-dsn",
                    output_root=Path(temp),
                )
                result = json.loads(path.read_text())
            self.assertEqual(result["status"], "FAILED_WARN_ONLY")
            self.assertIn("PRIOR_DAY_CERTIFICATE_MISSING", result["failure"])
            export.assert_not_called()
            roster.assert_not_called()

    def test_points_and_saves_block_before_history_when_current_roster_is_missing(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        for lane in ("POINTS", "SAVES"):
            with self.subTest(lane=lane), tempfile.TemporaryDirectory() as temp, patch.object(
                common, "require_slate_history_certificate", return_value={"status": "CERTIFIED_FINAL"}
            ), patch.object(common, "require_slate_roster_observation",
                            side_effect=RosterObservationNotReady("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING")), patch.object(
                common, "export_lane_inputs") as export:
                path = common.observe(
                    lane=lane, season=2026, slate_date="2026-10-18",
                    requested_phase="MIDDAY", observation_timestamp=now,
                    canonical_run_identifier="test", dsn="research-dsn",
                    output_root=Path(temp),
                )
                result = json.loads(path.read_text())
            self.assertEqual(result["status"], "ROSTER_NOT_READY")
            self.assertTrue(result["retry_eligible_on_later_poll"])
            self.assertFalse(result["feature_export_started"])
            export.assert_not_called()

    def test_sep30_style_timing_race_retries_on_later_natural_poll(self):
        first = datetime(2026, 9, 30, 14, 30, 10, tzinfo=timezone.utc)
        second = datetime(2026, 9, 30, 14, 45, 10, tzinfo=timezone.utc)
        games = pd.DataFrame({"game_id": [2026020001],
                              "scheduled_start_time_utc": ["2026-09-30T23:30:00Z"]})
        features = pd.DataFrame({"game_id": [2026020001], "player_id": [7]})
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "observer"
            run_dir = Path(temp) / "published"
            run_dir.mkdir()
            (run_dir / "run_metadata.json").write_text(json.dumps({"prediction_rows": 1}))
            with patch.object(common, "require_slate_history_certificate",
                              return_value={"status": "CERTIFIED_FINAL"}), patch.object(
                common, "require_slate_roster_observation",
                side_effect=[RosterObservationNotReady("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING"),
                             {"status": "VERIFIED_CURRENT_SLATE_ROSTER"}],
            ) as roster, patch.object(common, "export_lane_inputs", return_value=(games, features)), patch.object(
                common, "publish_saves", return_value=(run_dir, "COMPLETE_NEW_APPEND_ONLY")
            ) as publish:
                first_path = common.observe(
                    lane="SAVES", season=2026, slate_date="2026-09-30",
                    requested_phase="MIDDAY", observation_timestamp=first,
                    canonical_run_identifier="first", dsn="research-dsn", output_root=root,
                )
                second_path = common.observe(
                    lane="SAVES", season=2026, slate_date="2026-09-30",
                    requested_phase="MIDDAY", observation_timestamp=second,
                    canonical_run_identifier="second", dsn="research-dsn", output_root=root,
                )
            first_receipt = json.loads(first_path.read_text())
            second_receipt = json.loads(second_path.read_text())
        self.assertEqual(first_receipt["status"], "ROSTER_NOT_READY")
        self.assertEqual(second_receipt["status"], "CAPTURED")
        self.assertEqual(roster.call_count, 2)
        self.assertEqual(publish.call_count, 1)

    def test_invalid_roster_package_remains_blocking_not_retryable_readiness(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temp, patch.object(
            common, "require_slate_history_certificate",
            return_value={"status": "CERTIFIED_FINAL"},
        ), patch.object(
            common, "require_slate_roster_observation",
            side_effect=RuntimeError("CURRENT_SLATE_ROSTER_OBSERVATION_INVALID:HASH_MISMATCH"),
        ), patch.object(common, "export_lane_inputs") as export:
            path = common.observe(
                lane="POINTS", season=2026, slate_date="2026-10-18",
                requested_phase="MIDDAY", observation_timestamp=now,
                canonical_run_identifier="corrupt-roster", dsn="research-dsn",
                output_root=Path(temp),
            )
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertIn("HASH_MISMATCH", result["failure"])
        self.assertNotIn("retry_eligible_on_later_poll", result)
        export.assert_not_called()

    def test_sog_blocks_before_feature_export_without_certificate(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temp, patch.object(
            sog, "require_slate_history_certificate",
            side_effect=RuntimeError("PRIOR_DAY_CERTIFICATE_MISSING"),
        ), patch.object(sog, "require_slate_roster_observation") as roster, patch.object(
            sog, "export_features") as export:
            path = sog.observe(slate="2026-10-18", requested="MIDDAY", dsn="research-dsn",
                               root=Path(temp), now=now)
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertIn("PRIOR_DAY_CERTIFICATE_MISSING", result["failure"])
        export.assert_not_called()
        roster.assert_not_called()

    def test_sog_blocks_before_history_when_current_roster_is_missing(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temp, patch.object(
            sog, "require_slate_history_certificate", return_value={"status": "CERTIFIED_FINAL"}
        ), patch.object(sog, "require_slate_roster_observation",
                        side_effect=RuntimeError("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING")), patch.object(
            sog, "export_features") as export:
            path = sog.observe(slate="2026-10-18", requested="MIDDAY", dsn="research-dsn",
                               root=Path(temp), now=now)
            result = json.loads(path.read_text())
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertIn("CURRENT_SLATE_ROSTER_OBSERVATION_MISSING", result["failure"])
        export.assert_not_called()

    def test_valid_certificate_allows_points_prediction_publication(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        games = pd.DataFrame({"game_id": [2026020001],
                              "scheduled_start_time_utc": ["2026-10-18T20:00:00Z"]})
        features = pd.DataFrame({"game_id": [2026020001], "player_id": [7]})
        certificate = {"status": "CERTIFIED_FINAL", "slate_date": "2026-10-17",
                       "package_manifest_sha256": "verified-hash", "game_ids": [2026020002]}
        roster_certificate = {"status": "VERIFIED_CURRENT_SLATE_ROSTER",
                              "package_path": "/roster/package",
                              "package_manifest_sha256": "roster-hash"}
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp) / "run"
            run_dir.mkdir()
            (run_dir / "run_metadata.json").write_text(json.dumps({"prediction_rows": 1}))
            with patch.object(common, "require_slate_history_certificate", return_value=certificate), patch.object(
                common, "require_slate_roster_observation", return_value=roster_certificate), patch.object(
                common, "export_lane_inputs", return_value=(games, features)
            ), patch.object(common, "publish_points", return_value=(run_dir, "COMPLETE_NEW_APPEND_ONLY")) as publish:
                path = common.observe(
                    lane="POINTS", season=2026, slate_date="2026-10-18",
                    requested_phase="MIDDAY", observation_timestamp=now,
                    canonical_run_identifier="test", dsn="research-dsn",
                    output_root=Path(temp) / "observer",
                )
                result = json.loads(path.read_text())
        publish.assert_called_once()
        self.assertEqual(result["status"], "CAPTURED")
        self.assertEqual(result["prior_day_reconciliation_certificate"], certificate)
        self.assertEqual(result["current_slate_roster_observation"], roster_certificate)
        self.assertTrue(result["durable_publication_timestamp_utc"].endswith("Z"))

    def test_valid_certificate_allows_sog_prediction_generation(self):
        now = datetime(2026, 10, 18, 16, 0, tzinfo=timezone.utc)
        schedule = pd.DataFrame({"game_id": [2026020001],
                                 "scheduled_start_time_utc": ["2026-10-18T20:00:00Z"]})
        features = pd.DataFrame({"scheduled_start_time_utc": ["2026-10-18T20:00:00Z"]})
        empty = pd.DataFrame()
        certificate = {"status": "CERTIFIED_FINAL", "slate_date": "2026-10-17",
                       "package_manifest_sha256": "verified-hash", "game_ids": [2026020002]}
        roster_certificate = {"status": "VERIFIED_CURRENT_SLATE_ROSTER",
                              "package_path": "/roster/package",
                              "package_manifest_sha256": "roster-hash"}
        with tempfile.TemporaryDirectory() as temp, patch.object(
            sog, "require_slate_history_certificate", return_value=certificate
        ), patch.object(sog, "require_slate_roster_observation", return_value=roster_certificate
        ), patch.object(sog, "export_features", return_value=(schedule, features)), patch.object(
            sog, "build_predictions", return_value=(empty, empty, empty)
        ) as build:
            path = sog.observe(slate="2026-10-18", requested="MIDDAY", dsn="research-dsn",
                               root=Path(temp), now=now)
            result = json.loads(path.read_text())
        build.assert_called_once()
        self.assertEqual(result["status"], "CAPTURED")
        self.assertEqual(result["prior_day_reconciliation_certificate"], certificate)
        self.assertEqual(result["current_slate_roster_observation"], roster_certificate)
        self.assertTrue(result["durable_publication_timestamp_utc"].endswith("Z"))


if __name__ == "__main__":
    unittest.main()
