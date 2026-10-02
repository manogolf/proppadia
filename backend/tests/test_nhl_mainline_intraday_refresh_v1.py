import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.cross_market_shadow.core import quota_estimate, run_capture, verify_manifest
from backend.nhl.cross_market_shadow.core import normalize_game_types, SCHEDULE_COLUMNS
from backend.nhl.scripts.activate_nhl_2026_mainline_cross_market_prospective_shadow_v1 import fixture_files
from backend.nhl.scripts.run_nhl_mainline_cross_market_capture_warn_only import (
    phase_for,
    prior_capture_suppression,
)
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as capture


class MainlineIntradayRefreshTest(unittest.TestCase):
    def schedule(self, start="2026-09-29T23:00:00Z"):
        return pd.DataFrame([{
            "game_id": 2026010001,
            "scheduled_start_time_utc": start,
        }])

    def test_refresh_allowed_hours_before_start_and_auto_remains_scheduled(self):
        now = datetime(2026, 9, 29, 15, 0, tzinfo=timezone.utc)
        self.assertEqual(phase_for(self.schedule(), now, "REFRESH", False),
                         ("REFRESH", "EXPLICIT_PHASE"))
        auto_phase, auto_reason = phase_for(self.schedule(), now, "AUTO", False)
        self.assertEqual(auto_phase, "MIDDAY")
        self.assertIn("AUTO_ALLOWED_NONBLOCKING", auto_reason)

    def test_auto_inside_final_pregame_window_remains_enabled(self):
        now = datetime(2026, 9, 29, 21, 55, tzinfo=timezone.utc)
        phase, reason = phase_for(self.schedule(), now, "AUTO", False)
        self.assertEqual(phase, "FINAL_PREGAME")
        self.assertIn("FIRST_START_IN_", reason)

    def test_refresh_never_suppressed_by_prior_snapshots_or_failed_claims(self):
        for _ in range(3):
            self.assertIsNone(prior_capture_suppression(
                "REFRESH", existing=True, prior_claim=True, prior_paid=True, force=False,
            ))

    def test_scheduler_phases_retain_duplicate_and_failed_attempt_guards(self):
        self.assertEqual(prior_capture_suppression(
            "MIDDAY", existing=True, prior_claim=False, prior_paid=False, force=False,
        ), "NOOP_ALREADY_CAPTURED")
        self.assertEqual(prior_capture_suppression(
            "FINAL_PREGAME", existing=False, prior_claim=True, prior_paid=False,
            force=False,
        ), "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")
        self.assertIsNone(prior_capture_suppression(
            "FINAL_PREGAME", existing=True, prior_claim=True, prior_paid=True, force=True,
        ))

    def test_repeated_refreshes_get_distinct_immutable_states_and_preserve_prior(self):
        with tempfile.TemporaryDirectory(prefix="nhl_intraday_refresh_") as raw:
            work = Path(raw)
            inputs = fixture_files(work)
            root = work / "captures"
            first = run_capture(
                inputs["schedule"], inputs["history"], inputs["odds"], root,
                "2026-09-19", "2026-09-19T18:00:00Z", "REFRESH", canary_mode=True,
            )
            first_manifest = (first / "SHA256SUMS").read_bytes()
            second = run_capture(
                inputs["schedule"], inputs["history"], inputs["odds"], root,
                "2026-09-19", "2026-09-19T18:01:00Z", "REFRESH", canary_mode=True,
            )
            third = run_capture(
                inputs["schedule"], inputs["history"], inputs["odds"], root,
                "2026-09-19", "2026-09-19T18:02:00Z", "REFRESH", canary_mode=True,
            )
            self.assertEqual(len({first, second, third}), 3)
            self.assertEqual((first / "SHA256SUMS").read_bytes(), first_manifest)
            for package in (first, second, third):
                verify_manifest(package)
                state = json.loads((package / "daily_execution_status.json").read_text())
                self.assertEqual(state["scheduled_games"], 1)
                self.assertEqual(state["wagers_selected"], 0)
                self.assertEqual(state["production_writes"], 0)

    def test_request_and_credit_budget_remains_bounded(self):
        estimate = quota_estimate()
        self.assertEqual(estimate["http_request_count"], 1)
        self.assertTrue(estimate["within_request_bound"])
        self.assertTrue(estimate["within_credit_bound"])

    def test_empty_strict_prior_history_retains_v2_required_schema(self):
        schedule = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": "2026-09-29",
            "game_id": 2026020001, "game_date": "2026-09-29",
            "scheduled_start_time_utc": "2026-09-29T23:00:00Z",
            "home_team_id": 10, "home_team": "MTL", "away_team_id": 20,
            "away_team": "OTT", "game_status": "SCHEDULED",
            "game_type_code": 2,
        }])
        empty_history = pd.DataFrame(columns=list(SCHEDULE_COLUMNS) + [
            "game_type_code", "final_home_shots", "final_away_shots",
            "final_home_skater_rows", "final_home_distinct_skater_players",
            "final_away_skater_rows", "final_away_distinct_skater_players",
        ])
        with tempfile.TemporaryDirectory(prefix="nhl_empty_history_schema_") as raw:
            with patch.object(capture.psycopg, "connect") as connect, \
                    patch.object(capture.pd, "read_sql_query", return_value=empty_history):
                connect.return_value.__enter__.return_value = object()
                _, history_path, _ = capture.export_inputs(
                    "unused-test-dsn", "2026-09-29", Path(raw) / "inputs",
                    canonical_schedule=schedule,
                )
            history = normalize_game_types(pd.read_csv(history_path))
        required = set(SCHEDULE_COLUMNS) | {
            "final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots",
        }
        self.assertFalse(required - set(history.columns))
        self.assertTrue(history.empty)

    def test_failed_refresh_transport_does_not_block_later_fresh_attempt(self):
        with tempfile.TemporaryDirectory(prefix="nhl_refresh_retry_") as raw:
            root = Path(raw) / "root"
            schedule = self.schedule()
            fixed = datetime(2026, 9, 29, 15, 0, tzinfo=timezone.utc)

            def export(_dsn, _slate, directory, **_kwargs):
                directory.mkdir(parents=True, exist_ok=True)
                schedule_path, history_path = directory / "schedule.csv", directory / "history.csv"
                schedule.to_csv(schedule_path, index=False)
                history_path.write_text("fixture\n")
                return schedule_path, history_path, schedule

            calls = []

            def fetch(_key, output):
                calls.append(output)
                if len(calls) == 1:
                    raise ConnectionError("synthetic transport failure")
                output.write_text(json.dumps({
                    "capture_timestamp_utc": fixed.isoformat(),
                    "quota": {"credits_consumed": 4},
                }))
                return output

            with patch.object(capture, "utc_now", return_value=fixed), \
                    patch.object(capture, "export_inputs", side_effect=export), \
                    patch.object(capture, "fetch_markets", side_effect=fetch), \
                    patch.object(capture, "run_capture", return_value=root / "new_snapshot"), \
                    patch.dict("os.environ", {"ODDS_API_KEY": "test-key"}):
                first = capture.observe(root, "2026-09-29", "REFRESH", False, "test-dsn")
                second = capture.observe(root, "2026-09-29", "REFRESH", False, "test-dsn")

            first_status = json.loads(first.read_text())
            second_status = json.loads(second.read_text())
            self.assertEqual(first_status["status"], "FAILED_WARN_ONLY")
            self.assertEqual(second_status["status"], "CAPTURED")
            self.assertEqual(second_status["live_credits_consumed"], 4)
            self.assertEqual(len(calls), 2)
            claims = list((root / "paid_attempt_claims" / "2026-09-29").glob("*.claim.json"))
            self.assertEqual(len(claims), 2)


if __name__ == "__main__":
    unittest.main()
