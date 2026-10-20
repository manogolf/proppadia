"""Per-game closing odds scheduler: window, skips, retries, binding report."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from backend.nhl.daily_capture import CanonicalGame
from backend.nhl.scripts import run_nhl_closing_prop_odds as scheduler
from backend.nhl.tests.test_closing_odds_phase import provider_for

UTC = timezone.utc
NJD = CanonicalGame(2026020300, "2026-10-21T23:00:00Z", "NJD", "NYR",
                    ("NJD", "New Jersey Devils"), ("NYR", "New York Rangers"))
BOS = CanonicalGame(2026020301, "2026-10-22T02:00:00Z", "BOS", "TOR",
                    ("BOS", "Boston Bruins"), ("TOR", "Toronto Maple Leafs"))


class ClosingSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        patcher = patch.object(scheduler, "load_canonical_slate", return_value=[NJD, BOS])
        patcher.start(); self.addCleanup(patcher.stop)

    def tearDown(self):
        self.temp.cleanup()

    def run_at(self, now, provider=None, dry_run=False):
        return scheduler.run(now=now, observation_root=self.root, slate_root=self.root / "slates",
                             dry_run=dry_run, provider=provider, authorized=provider is not None)

    def observations(self):
        day = self.root / "season=2026/slate_date=2026-10-21"
        return sorted(p for p in day.glob("observation=*")) if day.is_dir() else []

    def test_only_games_10_to_30_minutes_out_are_captured(self):
        now = datetime(2026, 10, 21, 22, 40, tzinfo=UTC)  # NJD in 20 min; BOS in 3h20
        records = self.run_at(now, provider_for("New Jersey Devils", "New York Rangers", "e1"))
        self.assertEqual([(r["game_id"], r["decision"]) for r in records], [(2026020300, "CAPTURE_ATTEMPTED")])
        self.assertTrue(records[0]["game_bound"])
        summary = json.loads((self.observations()[0] / "observation_summary.json").read_text())
        self.assertEqual(summary["phase"], "CLOSING")
        self.assertIs(summary["strictly_prestart"], True)

    def test_a_captured_game_is_not_requested_again(self):
        now = datetime(2026, 10, 21, 22, 40, tzinfo=UTC)
        self.run_at(now, provider_for("New Jersey Devils", "New York Rangers", "e1"))
        provider = provider_for("New Jersey Devils", "New York Rangers", "e1")
        records = self.run_at(now + timedelta(minutes=5), provider)
        self.assertEqual(records[0]["decision"], "SKIPPED_ALREADY_CAPTURED")
        self.assertEqual(provider.calls, 0)
        self.assertEqual(len(self.observations()), 1)

    def test_nothing_is_requested_inside_10_minutes_or_after_the_start(self):
        for minutes_before in (10, 5, 0, -3):
            now = datetime(2026, 10, 21, 23, 0, tzinfo=UTC) - timedelta(minutes=minutes_before)
            provider = provider_for("New Jersey Devils", "New York Rangers", "e1")
            records = self.run_at(now, provider)
            self.assertEqual(provider.calls, 0, minutes_before)
            self.assertEqual([r["decision"] for r in records if r["game_id"] == 2026020300],
                             ["MISSED_NO_CLOSING_CAPTURE"])
        self.assertEqual(self.observations(), [])

    def test_an_unbound_event_is_reported_and_retried_in_the_window(self):
        now = datetime(2026, 10, 21, 22, 35, tzinfo=UTC)
        records = self.run_at(now, provider_for("Boston Bruins", "Toronto Maple Leafs", "wrong"))
        self.assertEqual(records[0]["decision"], "CAPTURE_ATTEMPTED")
        self.assertIs(records[0]["game_bound"], False)
        self.assertIn("wrong", records[0]["unbound_provider_events"])
        records = self.run_at(now + timedelta(minutes=5), provider_for("New Jersey Devils", "New York Rangers", "e1"))
        self.assertIs(records[0]["game_bound"], True)  # retried and bound

    def test_attempts_are_capped(self):
        start = datetime(2026, 10, 21, 22, 31, tzinfo=UTC)
        for step in range(scheduler.MAX_ATTEMPTS + 1):
            records = self.run_at(start + timedelta(minutes=5 * step),
                                  provider_for("Boston Bruins", "Toronto Maple Leafs", "wrong"))
        self.assertEqual(records[0]["decision"], "SKIPPED_ATTEMPT_LIMIT")

    def test_dry_run_requests_nothing_and_writes_nothing(self):
        records = self.run_at(datetime(2026, 10, 21, 22, 40, tzinfo=UTC), dry_run=True)
        self.assertEqual(records[0]["decision"], "WOULD_CAPTURE")
        self.assertFalse((self.root / "season=2026").exists())

    def test_each_run_is_logged_beside_the_snapshots(self):
        self.run_at(datetime(2026, 10, 21, 22, 40, tzinfo=UTC),
                    provider_for("New Jersey Devils", "New York Rangers", "e1"))
        log = self.root / "season=2026/slate_date=2026-10-21/closing_capture_log.jsonl"
        self.assertEqual(json.loads(log.read_text().splitlines()[0])["game_id"], 2026020300)


if __name__ == "__main__":
    unittest.main()
