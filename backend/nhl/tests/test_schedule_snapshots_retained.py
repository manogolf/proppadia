"""Every schedule fetch is kept; a failed fetch never replaces a good schedule (P2.24 F)."""
import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.reconciliation_certificate import schedule_hash_matches


def _load_importer():
    env = {"NHL_DB_ACCESS_MODE": "legacy",
           "SUPABASE_DB_URL": "postgresql://placeholder@db.invalid/postgres"}
    with patch.dict(os.environ, env):
        sys.modules.pop("backend.nhl.scripts.import_schedule_today", None)
        return importlib.import_module("backend.nhl.scripts.import_schedule_today")


class ScheduleSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.importer = _load_importer()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.importer.HEALTH_ROOT = self.root

    def tearDown(self):
        self.temp.cleanup()

    def _fetch(self, stamp, games, status, ready, error=None):
        import hashlib
        raw = {"gameWeek": [{"date": "2026-10-19", "games": games}]}
        compact = (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.importer.LAST_FETCH_EVIDENCE = {
            "source_url": "https://api-web.nhle.com/v1/schedule/2026-10-19",
            "fetch_timestamp_utc": stamp, "raw_source": raw,
            "raw_source_sha256": hashlib.sha256(compact).hexdigest(), "raw_game_count": len(games),
        } if ready else {"fetch_timestamp_utc": stamp}
        return self.importer._write_slate_health("2026-10-19", games, status, ready, error)

    def test_failed_later_fetch_is_kept_but_does_not_replace_the_good_schedule(self):
        game = {"id": 1, "startTimeUTC": "2026-10-19T23:00:00Z", "gameType": 2}
        self._fetch("2026-10-19T13:00:00Z", [game], "READY", True)
        self._fetch("2026-10-19T23:20:00Z", [], "FAILED", False, "timeout")
        day = self.root / "2026-10-19"
        current = json.loads((day / "slate_health.json").read_text())
        self.assertEqual(current["completion_status"], "READY")
        self.assertTrue(schedule_hash_matches(current["raw_source_hash"], day / "raw_schedule_response.json"))
        snapshots = sorted(p.name for p in day.glob("slate_health.fetch=*.json"))
        self.assertEqual(len(snapshots), 2)
        self.assertFalse((day / "snapshots").exists())  # no new directory
        failed = json.loads((day / snapshots[1]).read_text())
        self.assertEqual(failed["completion_status"], "FAILED")

    def test_each_successful_fetch_is_a_snapshot_and_updates_the_current_schedule(self):
        early = {"id": 1, "startTimeUTC": "2026-10-19T23:00:00Z", "gameType": 2}
        moved = {"id": 1, "startTimeUTC": "2026-10-19T23:30:00Z", "gameType": 2}
        self._fetch("2026-10-19T13:00:00Z", [early], "READY", True)
        self._fetch("2026-10-19T19:00:00Z", [moved], "READY", True)
        day = self.root / "2026-10-19"
        current = json.loads((day / "raw_schedule_response.json").read_text())
        self.assertEqual(current["gameWeek"][0]["games"][0]["startTimeUTC"], "2026-10-19T23:30:00Z")
        first = sorted(day.glob("raw_schedule_response.fetch=*.json"))[0]
        kept = json.loads(first.read_text())
        self.assertEqual(kept["gameWeek"][0]["games"][0]["startTimeUTC"], "2026-10-19T23:00:00Z")


if __name__ == "__main__":
    unittest.main()
