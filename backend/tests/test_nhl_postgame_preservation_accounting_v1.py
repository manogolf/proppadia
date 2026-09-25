import datetime as dt
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("SUPABASE_DB_URL", "postgresql://offline.invalid/fixture")

from backend.nhl.scripts import import_schedule_today as importer
from backend.nhl.scripts.run_nhl_postgame_reconciliation import (
    _run,
    schedule_artifact_environment,
)


class FakeCursor:
    columns = ["game_id", "game_date", "start_time_utc", "season", "game_type",
               "home_team_code", "away_team_code", "home_team_id", "away_team_id", "status"]

    def __init__(self, rows):
        self.rows = {row["game_id"]: dict(row) for row in rows}
        self.description = None
        self.rowcount = -1
        self._fetch = []

    def execute(self, sql, params):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT game_id, game_date"):
            self.description = [(name,) for name in self.columns]
            self._fetch = [tuple(self.rows[gid][name] for name in self.columns)
                           for gid in sorted(set(params[0]) & self.rows.keys())]
            self.rowcount = len(self._fetch)
        elif normalized.startswith("UPDATE nhl.games"):
            keys = ["game_date", "start_time_utc", "season", "game_type", "home_team_code",
                    "away_team_code", "home_team_id", "away_team_id", "status"]
            row = self.rows[params[-1]]
            row.update(dict(zip(keys, params[:-1])))
            self.rowcount = 1
        elif normalized.startswith("INSERT INTO nhl.games"):
            gid, game_date, start, season, game_type, home_code, away_code, home_id, away_id, status = params
            if gid in self.rows:
                raise AssertionError("plain INSERT must fail on a concurrent duplicate")
            self.rows[gid] = dict(zip(self.columns,
                                      [gid, game_date, start, season, game_type, home_code,
                                       away_code, home_id, away_id, status]))
            self.rowcount = 1
        else:
            raise AssertionError(f"unexpected SQL in offline fake: {normalized}")

    def fetchall(self):
        return self._fetch


class NHLPostgamePreservationAccountingTest(unittest.TestCase):
    def test_run_scoped_schedule_output_preserves_shared_health_file(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shared = root / "shared"
            date = "2026-09-24"
            shared_health = shared / date / "slate_health.json"
            shared_health.parent.mkdir(parents=True)
            shared_health.write_text('{"preserved": true}\n')
            before_hash = hashlib.sha256(shared_health.read_bytes()).hexdigest()

            env = schedule_artifact_environment(root / "request_runs" / date / "run-unique")
            with patch.object(importer, "HEALTH_ROOT", Path(env["NHL_SLATE_HEALTH_ROOT"])), \
                 patch.object(importer, "LAST_FETCH_EVIDENCE", {
                     "raw_source": {"gameWeek": []}, "fetch_timestamp_utc": "2026-09-25T00:00:00Z",
                     "raw_source_sha256": "fixture", "raw_game_count": 0,
                 }):
                output = importer._write_slate_health(date, [], "READY", True)

            self.assertTrue(output.is_file())
            self.assertNotEqual(output, shared_health)
            self.assertEqual(hashlib.sha256(shared_health.read_bytes()).hexdigest(), before_hash)
            self.assertTrue((output.parent / "raw_schedule_response.json").is_file())

    def test_child_run_forwards_only_explicit_run_scoped_artifact_overrides(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "run-unique"
            env_values = schedule_artifact_environment(root)
            observed = {}

            def fake_run(command, *, cwd, env, check):
                observed.update(env)

            with patch("backend.nhl.scripts.run_nhl_postgame_reconciliation.subprocess.run",
                       side_effect=fake_run):
                _run(["python", "import_schedule_today.py"], "2026-09-24", "offline-dsn",
                     extra_env=env_values)
            self.assertEqual(Path(observed["NHL_SLATE_HEALTH_ROOT"]), root / "schedule_source")
            self.assertEqual(Path(observed["NHL_DB_ACTION_ACCOUNTING_PATH"]),
                             root / "schedule_database_actions.json")

    def test_schedule_game_action_ledger_distinguishes_insert_update_and_noop(self):
        start = dt.datetime(2026, 9, 24, 23, 0, tzinfo=dt.timezone.utc)
        unchanged = {"game_id": 1, "game_date": dt.date(2026, 9, 24), "start_time_utc": start,
                     "season": 2026, "game_type": 2, "home_team_code": "A", "away_team_code": "B",
                     "home_team_id": 10, "away_team_id": 20, "status": "SCHEDULED"}
        changed = {**unchanged, "game_id": 2, "status": "SCHEDULED"}
        cursor = FakeCursor([unchanged, changed])
        payload = [
            {"game_id": 1, "game_date": "2026-09-24", "start_time_utc": "2026-09-24T23:00:00Z",
             "season": 2026, "game_type": 2, "home_team_id": 10, "away_team_id": 20, "status": "SCHEDULED"},
            {"game_id": 2, "game_date": "2026-09-24", "start_time_utc": "2026-09-24T23:00:00Z",
             "season": 2026, "game_type": 2, "home_team_id": 10, "away_team_id": 20, "status": "FINAL"},
            {"game_id": 3, "game_date": "2026-09-24", "start_time_utc": "2026-09-24T23:00:00Z",
             "season": 2026, "game_type": 2, "home_team_id": 10, "away_team_id": 20, "status": "SCHEDULED"},
        ]
        result = importer._apply_accounted_game_rows(cursor, payload, {10: "A", 20: "B"})
        self.assertEqual(result, {"status": "MEASURED_TRANSACTIONAL", "inserted": 1,
                                  "updated": 1, "unchanged_noop": 1, "deleted": 0,
                                  "rows_considered": 3})
        self.assertEqual(cursor.rows[2]["status"], "FINAL")
        self.assertIn(3, cursor.rows)


if __name__ == "__main__":
    unittest.main()
