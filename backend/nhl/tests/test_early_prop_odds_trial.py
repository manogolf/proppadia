"""06:00 PT odds-only trial: one capture per slate, kept out of daily readers, self-stopping."""
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from backend.nhl.daily_capture import CanonicalGame, is_closing_observation, is_side_capture_observation
from backend.nhl.scripts import run_nhl_early_prop_odds_trial as trial
from backend.nhl.tests.test_closing_odds_phase import provider_for

UTC = timezone.utc
NJD = CanonicalGame(2026020300, "2026-10-23T23:00:00Z", "NJD", "NYR",
                    ("NJD", "New Jersey Devils"), ("NYR", "New York Rangers"))
SIX_AM_PT = datetime(2026, 10, 23, 13, 0, tzinfo=UTC)


class EarlyTrialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        patcher = patch.object(trial, "load_canonical_slate", return_value=[NJD])
        patcher.start(); self.addCleanup(patcher.stop)
        self.fetched = []

    def tearDown(self):
        self.temp.cleanup()

    def fake_fetch(self, slate_date, schedule_root):
        self.fetched.append(Path(schedule_root))
        day = Path(schedule_root) / slate_date
        day.mkdir(parents=True, exist_ok=True)
        return day / "raw_schedule_response.json", day / "slate_health.json"

    def run_at(self, now, provider=None, dry_run=False):
        return trial.run(now=now, observation_root=self.root, dry_run=dry_run,
                         provider=provider, authorized=provider is not None,
                         fetch_schedule=self.fake_fetch)

    def observations(self):
        return sorted((self.root / "season=2026/slate_date=2026-10-23").glob("observation=*"))

    def test_captures_the_slate_once_as_early_trial(self):
        provider = provider_for("New Jersey Devils", "New York Rangers", "e1", start=NJD.start_time_utc)
        record = self.run_at(SIX_AM_PT, provider)
        self.assertEqual(record["decision"], "CAPTURE_ATTEMPTED")
        self.assertEqual(record["classification"], "CAPTURED_NONEMPTY")
        [observation] = self.observations()
        summary = json.loads((observation / "observation_summary.json").read_text())
        self.assertEqual(summary["phase"], "EARLY_TRIAL")
        # The schedule is kept beside the day's odds, never in the shared slate folder.
        self.assertEqual(self.fetched, [self.root / "season=2026/slate_date=2026-10-23/early_trial_schedule"])
        again = self.run_at(SIX_AM_PT.replace(minute=5),
                            provider_for("New Jersey Devils", "New York Rangers", "e1", start=NJD.start_time_utc))
        self.assertEqual(again["decision"], "SKIPPED_ALREADY_CAPTURED")
        self.assertEqual(len(self.observations()), 1)
        log = (self.root / "season=2026/slate_date=2026-10-23/early_trial_capture_log.jsonl").read_text()
        self.assertEqual(len(log.splitlines()), 1)  # the skip is reported, not logged twice

    def test_daily_readers_skip_trial_snapshots(self):
        self.run_at(SIX_AM_PT, provider_for("New Jersey Devils", "New York Rangers", "e1", start=NJD.start_time_utc))
        [observation] = self.observations()
        self.assertTrue(is_side_capture_observation(observation))
        self.assertFalse(is_closing_observation(observation))

    def test_outside_the_trial_week_nothing_is_fetched_or_requested(self):
        for day in (datetime(2026, 10, 22, 13, 0, tzinfo=UTC), datetime(2026, 10, 30, 13, 0, tzinfo=UTC)):
            provider = provider_for("New Jersey Devils", "New York Rangers", "e1", start=NJD.start_time_utc)
            record = self.run_at(day, provider)
            self.assertEqual(record["decision"], "TRIAL_INACTIVE")
            self.assertEqual(provider.calls, 0)
        self.assertEqual(self.fetched, [])

    def test_dry_run_and_started_games_make_no_request(self):
        provider = provider_for("New Jersey Devils", "New York Rangers", "e1", start=NJD.start_time_utc)
        self.assertEqual(self.run_at(SIX_AM_PT, provider, dry_run=True)["decision"], "WOULD_CAPTURE")
        late = datetime(2026, 10, 23, 23, 30, tzinfo=UTC)
        self.assertEqual(self.run_at(late, provider, dry_run=True)["decision"], "NO_PREGAME_GAMES")
        self.assertEqual(provider.calls, 0)
        self.assertEqual(self.observations(), [])

    def test_a_late_wake_does_not_record_a_06_00_snapshot(self):
        provider = provider_for("New Jersey Devils", "New York Rangers", "e1", start=NJD.start_time_utc)
        nine_am_pt = datetime(2026, 10, 23, 16, 0, tzinfo=UTC)
        self.assertEqual(self.run_at(nine_am_pt, provider)["decision"], "OUTSIDE_CAPTURE_WINDOW")
        self.assertEqual(provider.calls, 0)
        self.assertEqual(self.fetched, [])


if __name__ == "__main__":
    unittest.main()
