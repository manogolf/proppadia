import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.cross_market_shadow.core import (
    SCHEDULE_COLUMNS, build_v2_predictions, eligible_targets_at_capture, normalize_markets,
)
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture


SLATE = "2026-10-17"
NOW = datetime(2026, 10, 17, 23, 35, tzinfo=timezone.utc)


class CrossMarketObserverReadinessTest(unittest.TestCase):
    def test_october_17_bounded_warning_and_reused_learning_do_not_gate_mainline(self):
        # Retained daily status values: the warning was SOG-integrity-only;
        # prior-day learning reused its already verified package. Neither is
        # a source consumed by the mainline observer.
        daily_classification = "READY_WITH_BOUNDED_LANE_WARNING"
        reconciliation_status = "REUSED_VALID_PACKAGE"
        self.assertEqual(daily_classification, "READY_WITH_BOUNDED_LANE_WARNING")
        self.assertEqual(reconciliation_status, "REUSED_VALID_PACKAGE")

        order = []
        child_links = {
            "POINTS": {"status": "FAILED_WARN_ONLY", "receipt_path": "/points.json"},
            "SAVES": {"status": "FAILED_WARN_ONLY", "receipt_path": "/saves.json"},
            "SOG": {"status": "BOUNDED_WARNING", "receipt_path": "/sog.json"},
        }
        observed = {}
        with patch("sys.argv", ["runner", "--slate-date", SLATE,
                                "--env-file", "/absent", "--output-root", "/unused"]), \
             patch.object(capture, "load_env"), \
             patch.object(capture, "database_url", return_value="fixture-dsn"), \
             patch.object(capture, "observe_prediction_only_lanes",
                          side_effect=lambda *args: (order.append("independent_lanes")
                                                     or child_links)), \
             patch.object(capture, "observe",
                          side_effect=lambda *args, **kwargs: (order.append("mainline_observer")
                                                               or observed.update(kwargs)
                                                               or Path("status.json"))), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(capture.main(), 0)
        self.assertEqual(order, ["independent_lanes", "mainline_observer"])
        self.assertEqual(observed.get("player_prop_observers"), child_links)

    def test_lane_dispatch_links_success_skip_and_failed_readiness_receipts(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            paths = {lane: root / f"{lane.lower()}.json"
                     for lane in ("POINTS", "SAVES", "SOG")}
            for lane, status in (("POINTS", "CAPTURED"),
                                 ("SAVES", "FAILED_WARN_ONLY"),
                                 ("SOG", "NOOP_OUTSIDE_WINDOW")):
                paths[lane].write_text(json.dumps({"status": status, "large_payload": [1, 2]}))

            def independent(*, lane, **kwargs):
                return paths[lane]

            with patch.object(capture, "observe_independent_prediction_only",
                              side_effect=independent), \
                 patch.object(capture, "observe_sog_prediction_only", return_value=paths["SOG"]):
                links = capture.observe_prediction_only_lanes("2026-10-18", "AUTO",
                                                               "fixture-dsn", NOW)
        self.assertEqual(links["POINTS"], {"status": "CAPTURED",
                                           "receipt_path": str(paths["POINTS"])})
        self.assertEqual(links["SAVES"]["status"], "FAILED_WARN_ONLY")
        self.assertEqual(links["SOG"]["status"], "NOOP_OUTSIDE_WINDOW")
        self.assertNotIn("large_payload", links["POINTS"])

    def test_pre_receipt_exception_is_bounded_and_does_not_skip_other_lanes(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            saves = root / "saves.json"
            sog = root / "sog.json"
            saves.write_text(json.dumps({"status": "NOOP_OUTSIDE_WINDOW"}))
            sog.write_text(json.dumps({"status": "CAPTURED"}))
            calls = []

            def independent(*, lane, **kwargs):
                calls.append(lane)
                if lane == "POINTS":
                    raise RuntimeError("password=secret postgres://user:pass@host/db " + "x" * 800)
                return saves

            with patch.object(capture, "observe_independent_prediction_only",
                              side_effect=independent), \
                 patch.object(capture, "observe_sog_prediction_only", return_value=sog):
                links = capture.observe_prediction_only_lanes("2026-10-18", "AUTO",
                                                               "fixture-dsn", NOW)
        failed = links["POINTS"]
        self.assertEqual(failed["status"], "FAILED_BEFORE_RECEIPT")
        self.assertIsNone(failed["receipt_path"])
        self.assertLessEqual(len(failed["failure"]["message"]), 500)
        self.assertNotIn("secret", failed["failure"]["message"])
        self.assertNotIn("user:pass", failed["failure"]["message"])
        self.assertEqual(calls, ["POINTS", "SAVES"])
        self.assertEqual(links["SOG"]["status"], "CAPTURED")

    def test_parent_receipt_retains_child_links_when_mainline_readiness_fails(self):
        child_links = {
            "POINTS": {"status": "FAILED_WARN_ONLY", "receipt_path": "/points/status.json"},
            "SAVES": {"status": "FAILED_WARN_ONLY", "receipt_path": "/saves/status.json"},
            "SOG": {"status": "NOOP_OUTSIDE_WINDOW", "receipt_path": "/sog/status.json"},
        }
        with tempfile.TemporaryDirectory() as raw:
            with patch.object(capture, "utc_now", return_value=NOW), \
                 patch.object(capture, "ensure_prior_day_official_outcomes",
                              side_effect=ValueError("fixture source gate")), \
                 patch.object(capture, "export_inputs") as export, \
                 patch.object(capture, "fetch_markets") as fetch:
                status_path = capture.observe(Path(raw), SLATE, "AUTO", False,
                                              "fixture-dsn", player_prop_observers=child_links)
            parent = json.loads(status_path.read_text())
        self.assertEqual(parent["status"], "FAILED_WARN_ONLY")
        self.assertEqual(parent["player_prop_observers"]["schema_version"],
                         "NHL_PLAYER_PROP_OBSERVER_LINKS_V1")
        self.assertEqual({key: value for key, value in parent["player_prop_observers"].items()
                          if key != "schema_version"}, child_links)
        self.assertEqual(parent["live_calls"], 0)
        export.assert_not_called()
        fetch.assert_not_called()

    def test_october_17_later_game_keeps_auto_window_after_first_start(self):
        schedule = pd.DataFrame({"scheduled_start_time_utc": [
            "2026-10-17T17:00:00Z", "2026-10-18T00:00:00Z",
            "2026-10-18T01:00:00Z",
        ]})
        decision = capture.auto_capture_readiness_decision(SLATE, schedule, NOW)
        self.assertEqual(decision["decision"], "SCHEDULE_WINDOW_ELIGIBLE")
        self.assertEqual(decision["resolved_phase"], "FINAL_PREGAME")
        self.assertEqual(decision["minutes_until_first_start"], 25.0)
        self.assertEqual(decision["mainline_input_validation"], "DEFERRED_TO_OBSERVER")

    def test_real_mainline_source_failure_blocks_before_provider_request(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with patch.object(capture, "utc_now", return_value=NOW), \
                 patch.object(capture, "ensure_prior_day_official_outcomes",
                              return_value={"status": "EXISTING_GOVERNED_PACKAGE_VALID"}), \
                 patch.object(capture, "require_slate_roster_observation",
                              return_value={"status": "VERIFIED_CURRENT_SLATE_ROSTER"}), \
                 patch.object(capture, "export_inputs",
                              side_effect=ValueError("CROSS_MARKET_OFFICIAL_OUTCOME_EVIDENCE_MISSING")), \
                 patch.object(capture, "fetch_markets") as fetch:
                status_path = capture.observe(root, SLATE, "AUTO", False, "fixture-dsn")
            result = json.loads(status_path.read_text())
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertIn("CROSS_MARKET_OFFICIAL_OUTCOME_EVIDENCE_MISSING", result["failure"])
        self.assertEqual(result["live_calls"], 0)
        fetch.assert_not_called()

    def test_corrupt_or_missing_prior_outcome_source_remains_blocking(self):
        for message in ("CROSS_MARKET_OUTCOME_MANIFEST_MISSING",
                        "CROSS_MARKET_OUTCOME_MANIFEST_MISMATCH:canonical_game_outcomes.csv"):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                with patch.object(capture, "utc_now", return_value=NOW), \
                     patch.object(capture, "ensure_prior_day_official_outcomes",
                                  side_effect=ValueError(message)), \
                     patch.object(capture, "export_inputs") as export, \
                     patch.object(capture, "fetch_markets") as fetch:
                    status_path = capture.observe(root, SLATE, "AUTO", False, "fixture-dsn")
                result = json.loads(status_path.read_text())
                self.assertEqual(result["status"], "FAILED_WARN_ONLY")
                self.assertEqual(result["live_calls"], 0)
                export.assert_not_called()
                fetch.assert_not_called()

    def test_duplicate_final_capture_stays_suppressed(self):
        self.assertEqual(capture.prior_capture_suppression(
            "FINAL_PREGAME", existing=True, prior_claim=False,
            prior_paid=False, force=False), "NOOP_ALREADY_CAPTURED")

    def test_staggered_schedule_filters_started_game_at_response_timestamp(self):
        schedule = pd.DataFrame({
            "game_id": [849001, 849002, 849003],
            "scheduled_start_time_utc": [
                "2026-10-17T17:00:00Z", "2026-10-17T19:30:00Z",
                "2026-10-17T20:00:00Z",
            ],
        })
        eligible, excluded = eligible_targets_at_capture(
            schedule, "2026-10-17T19:35:00Z")
        self.assertEqual(eligible.game_id.tolist(), [849003])
        self.assertEqual(excluded, [849001, 849002])

    def test_odds_updated_at_start_is_rejected_even_when_capture_is_earlier(self):
        schedule = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": SLATE, "game_id": 849003,
            "game_date": SLATE, "scheduled_start_time_utc": "2026-10-18T00:00:00Z",
            "home_team_id": 6, "home_team": "BOS", "away_team_id": 8,
            "away_team": "MTL", "game_status": "SCHEDULED", "game_type_code": 2,
        }])
        event = {
            "id": "boston-montreal", "home_team": "Boston Bruins",
            "away_team": "Montréal Canadiens", "commence_time": "2026-10-18T00:00:00Z",
            "bookmakers": [{"key": "fixture", "title": "Fixture", "last_update": "2026-10-18T00:00:00Z",
                            "markets": [{"key": "h2h", "outcomes": [
                                {"name": "Boston Bruins", "price": -110},
                                {"name": "Montréal Canadiens", "price": 100},
                            ]}]}],
        }
        quotes, _, _ = normalize_markets({
            "capture_timestamp_utc": "2026-10-17T23:35:00Z",
            "provider_response": [event],
        }, schedule)
        self.assertTrue(quotes.qualification_status.eq("POST_START_INVALID").all())

    def test_prediction_created_at_or_after_start_is_not_pregame_eligible(self):
        schedule = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": SLATE, "game_id": 849003,
            "game_date": SLATE, "scheduled_start_time_utc": "2026-10-18T00:00:00Z",
            "home_team_id": 6, "home_team": "BOS", "away_team_id": 8,
            "away_team": "MTL", "game_status": "SCHEDULED", "game_type_code": 2,
        }])
        empty_history = pd.DataFrame(columns=list(SCHEDULE_COLUMNS) + ["game_type_code"] + [
            "final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots",
        ])
        predictions, _ = build_v2_predictions(
            schedule, empty_history, "2026-10-18T00:00:00Z")
        self.assertEqual(predictions.iloc[0].prediction_status, "POST_START_REJECTED")
        self.assertFalse(bool(predictions.iloc[0].regular_season_evaluation_eligible))

    def test_et_date_is_authoritative_and_missing_schedule_blocks(self):
        before_et_midnight = datetime(2026, 10, 18, 4, 30, tzinfo=timezone.utc)
        self.assertEqual(before_et_midnight.astimezone(ZoneInfo("America/Los_Angeles"))
                         .date().isoformat(), "2026-10-17")
        self.assertEqual(capture.resolve_slate_date("today", before_et_midnight),
                         "2026-10-18")
        result = capture.auto_capture_readiness_decision(
            SLATE, pd.DataFrame(columns=["scheduled_start_time_utc"]), NOW)
        self.assertEqual(result["decision"], "BLOCKED")
        self.assertEqual(result["gate_reason"], "NO_CANONICAL_2026_GAMES")


if __name__ == "__main__":
    unittest.main()
