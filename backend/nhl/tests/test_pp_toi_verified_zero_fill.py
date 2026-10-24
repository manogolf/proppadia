"""Verified-zero PP-TOI fill (P2.31): migration contract and guarded daily call."""
from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from backend.nhl.scripts import fill_pp_toi_minutes_for_date as fill

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill.sql"
ROLLBACK = ROOT / "backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill_rollback.sql"
FIXTURE = ROOT / "backend/nhl/sql/tests/nhl_pp_toi_zero_fill_fixture_test.sql"


class FakeCursor:
    def __init__(self, zero_available: bool, zero_count: int = 3):
        self.zero_available = zero_available
        self.zero_count = zero_count
        self.calls: list[tuple[str, tuple]] = []
        self._next = None

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if "to_regprocedure" in sql:
            self._next = (self.zero_available,)
        elif "fill_pp_toi_zero_from_shift_overlap" in sql:
            self._next = (self.zero_count,)
        else:
            self._next = None

    def fetchone(self):
        return self._next

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


def run_main(cursor: FakeCursor, games=(2026020101, 2026020102)):
    conn = FakeConn(cursor)
    out, err = StringIO(), StringIO()
    with patch.object(fill.psycopg, "connect", return_value=conn), \
         patch.object(fill, "database_url", return_value="postgresql://fixture"), \
         patch.object(fill, "get_games_for_date", return_value=list(games)), \
         patch.object(fill, "skip_voided_games", side_effect=lambda ids: ids), \
         patch.object(fill, "extract_pp_toi_minutes_from_db", return_value={1: 2.0}), \
         patch.object(fill, "update_game", return_value=(20, 1, 1)), \
         patch.object(fill, "assert_no_bad_team_games", return_value=0), \
         patch.object(fill, "write_pp_toi_conflict_diagnostic", return_value={"conflict_count": 0}), \
         patch.object(fill.time, "sleep"), \
         patch.object(sys, "argv", ["fill", "--date", "2026-10-20", "--commit"]), \
         redirect_stdout(out), redirect_stderr(err):
        fill.main()
    line = next(l for l in out.getvalue().splitlines() if l.startswith("NHL_CHILD_SUMMARY_JSON="))
    return json.loads(line.split("=", 1)[1]), conn, err.getvalue()


class GuardedDailyCallTests(unittest.TestCase):
    def test_without_the_migration_the_daily_fill_is_unchanged(self):
        cursor = FakeCursor(zero_available=False)
        summary, _, err = run_main(cursor)
        self.assertFalse(summary["zero_fill_function_available"])
        self.assertEqual(summary["verified_zero_fills"], 0)
        self.assertEqual(summary["filled_nulls"], 2)
        self.assertFalse(any("fill_pp_toi_zero_from_shift_overlap(%s)" in sql for sql, _ in cursor.calls))
        self.assertIn("not installed", err)

    def test_with_the_migration_each_game_gets_a_verified_zero_fill(self):
        cursor = FakeCursor(zero_available=True, zero_count=3)
        summary, conn, _ = run_main(cursor)
        self.assertTrue(summary["zero_fill_function_available"])
        self.assertEqual(summary["verified_zero_fills"], 6)
        calls = [params for sql, params in cursor.calls if "fill_pp_toi_zero_from_shift_overlap(%s)" in sql]
        self.assertEqual(calls, [(2026020101,), (2026020102,)])
        self.assertGreaterEqual(conn.commits, 2)

    def test_zero_fill_uses_the_fixed_function_and_never_updates_directly(self):
        cursor = FakeCursor(zero_available=True, zero_count=4)
        self.assertEqual(fill.fill_verified_zero(cursor, 2026020101), 4)
        sql, params = cursor.calls[-1]
        self.assertIn("nhl.fill_pp_toi_zero_from_shift_overlap", sql)
        self.assertNotIn("UPDATE", sql.upper())
        self.assertEqual(params, (2026020101,))

    def test_parent_receipt_admits_the_new_counts(self):
        source = (ROOT / "backend/nhl/daily_orchestration.py").read_text()
        allowed = source.split("allowed = {", 1)[1].split("}", 1)[0]
        self.assertIn('"verified_zero_fills"', allowed)
        self.assertIn('"zero_fill_function_available"', allowed)


class MigrationContractTests(unittest.TestCase):
    def test_function_is_fixed_security_definer_null_only(self):
        sql = MIGRATION.read_text().lower()
        self.assertIn("preparation migration only", sql)
        self.assertEqual(sql.count("create or replace function"), 1)
        self.assertIn("security definer", sql)
        self.assertIn("set search_path = pg_catalog, nhl", sql)
        self.assertNotRegex(sql, r"\bexecute\s+(format|sql)\b")
        self.assertIn("set pp_toi_minutes = 0.00", sql)
        for guard in ("raw.pp_toi_minutes is null", "raw.toi_minutes > 0",
                      "coalesce(raw.pp_sog, 0) = 0", "coverage.team_count = 1",
                      "abs(coverage.shift_seconds - raw.toi_minutes * 60) <= 60",
                      "coalesce(overlap.pp_seconds, 0) = 0", "select distinct game_id, player_id, team_id"):
            self.assertIn(guard, sql)
        self.assertIn("nhl_pp_zero_fill_requires_safe_promotion_migration", sql)

    def test_execution_is_restricted_to_ingest(self):
        sql = MIGRATION.read_text().lower()
        self.assertIn("revoke all on function nhl.fill_pp_toi_zero_from_shift_overlap(bigint)\n  from public, anon, authenticated, service_role", sql)
        self.assertEqual(sql.count("grant execute on function"), 1)
        self.assertIn("to proppadia_nhl_ingest", sql)

    def test_rollback_only_removes_the_entry_point(self):
        sql = ROLLBACK.read_text().lower()
        self.assertIn("drop function nhl.fill_pp_toi_zero_from_shift_overlap(bigint)", sql)
        self.assertNotIn("update nhl.skater_game_logs_raw", sql)
        self.assertNotIn("grant update", sql)

    def test_fixture_covers_every_guard(self):
        sql = FIXTURE.read_text().lower()
        for expected in (
            "fixture_verified_zero_not_written", "fixture_opponent_zero_not_written",
            "fixture_zero_written_despite_pp_overlap", "fixture_zero_written_without_shifts",
            "fixture_zero_written_with_incomplete_shifts", "fixture_zero_fill_overwrote_populated_source",
            "fixture_zero_written_despite_pp_sog", "fixture_zero_written_without_manpower_segments",
            "fixture_zero_fill_replay_not_idempotent", "fixture_positive_fill_after_zero_failed",
            "fixture_direct_pp_toi_update_was_allowed", "set local role proppadia_nhl_daily_login",
        ):
            self.assertIn(expected, sql)


if __name__ == "__main__":
    unittest.main()
