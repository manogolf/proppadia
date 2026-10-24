"""Verified-zero PP-TOI fill (P2.31): migration contract and guarded daily call."""
from __future__ import annotations

import json
import os
import sys
import unittest
import uuid
from decimal import Decimal
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import psycopg

from backend.nhl.scripts import fill_pp_toi_minutes_for_date as fill

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill.sql"
ROLLBACK = ROOT / "backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill_rollback.sql"
FIXTURE = ROOT / "backend/nhl/sql/tests/nhl_pp_toi_zero_fill_fixture_test.sql"
EDITOR = ROOT / "backend/nhl/sql/repairs/operator_sql_editor_pp_toi_verified_zero_fill.sql"
if (ROOT / "backend/.env").is_file():
    from dotenv import load_dotenv
    load_dotenv(ROOT / "backend/.env", override=False)
DSN = os.environ.get("NHL_PP_TOI_TEST_DSN")


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

    def test_operator_editor_block_applies_the_migration_unchanged(self):
        def body(sql: str) -> str:
            return sql[sql.index("BEGIN;\n") + len("BEGIN;\n"):sql.rindex("COMMIT;")]
        migration_body = body(MIGRATION.read_text())
        editor = EDITOR.read_text()
        self.assertIn(migration_body, editor)
        self.assertIn("RUN_AS_POSTGRES_IN_SQL_EDITOR", editor)
        self.assertEqual(editor.count("BEGIN;"), 1)
        self.assertEqual(editor.count("COMMIT;"), 1)
        self.assertNotIn("update nhl.skater_game_logs_raw", editor.lower().split("as $$", 1)[0])

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


@unittest.skipUnless(DSN, "set NHL_PP_TOI_TEST_DSN for the rollback-only PostgreSQL exercise")
class ZeroFillFunctionDatabaseTests(unittest.TestCase):
    """Runs the migration's exact function body in a throwaway schema; always rolled back."""

    def setUp(self):
        self.schema = "pp_zero_" + uuid.uuid4().hex[:12]
        self.conn = psycopg.connect(DSN)
        self.conn.execute("BEGIN")
        q = f'"{self.schema}"'
        self.conn.execute(f"CREATE SCHEMA {q}")
        self.conn.execute(f"""CREATE TABLE {q}.skater_game_logs_raw(game_id bigint, player_id bigint, team_id bigint,
            toi_minutes numeric(5,2), pp_toi_minutes numeric(5,2), pp_sog integer, PRIMARY KEY(player_id, game_id))""")
        self.conn.execute(f"CREATE TABLE {q}.game_manpower_segments(game_id bigint, period int, start_sec int, end_sec int, pp_team_id bigint)")
        self.conn.execute(f"CREATE TABLE {q}.shiftcharts_raw(game_id bigint, player_id bigint, team_id bigint, period int, start_sec int, end_sec int)")
        sql = MIGRATION.read_text()
        function = sql[sql.index("CREATE OR REPLACE FUNCTION"):sql.index("$$;", sql.index("CREATE OR REPLACE FUNCTION")) + 3]
        function = function.replace("nhl.", f"{q}.").replace("SET search_path = pg_catalog, nhl", f"SET search_path = pg_catalog, {q}")
        self.conn.execute(function)
        self.conn.execute(f"INSERT INTO {q}.game_manpower_segments VALUES (101,1,0,120,18)")
        rows = [(101, 10, 18, 10, None, 0), (101, 11, 18, 10, None, 0), (101, 12, 18, 10, None, 0),
                (101, 13, 18, 10, None, 0), (101, 14, 18, 10, Decimal("1.25"), 0), (101, 15, 18, 10, None, 1),
                (101, 16, 19, 10, None, 0), (102, 20, 18, 10, None, 0)]
        shifts = [(101, 10, 18, 1, 300, 900), (101, 11, 18, 1, 0, 600), (101, 13, 18, 1, 300, 600),
                  (101, 14, 18, 1, 300, 900), (101, 15, 18, 1, 300, 900), (101, 16, 19, 1, 0, 600),
                  (101, 16, 19, 1, 0, 600), (102, 20, 18, 1, 300, 900)]
        with self.conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {q}.skater_game_logs_raw VALUES (%s,%s,%s,%s,%s,%s)", rows)
            cur.executemany(f"INSERT INTO {q}.shiftcharts_raw VALUES (%s,%s,%s,%s,%s,%s)", shifts)

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def fill(self, game):
        return self.conn.execute(f'SELECT "{self.schema}".fill_pp_toi_zero_from_shift_overlap(%s)', (game,)).fetchone()[0]

    def values(self):
        return {p: v for p, v in self.conn.execute(f'SELECT player_id, pp_toi_minutes FROM "{self.schema}".skater_game_logs_raw')}

    def test_zero_written_only_with_complete_evidence(self):
        self.assertEqual(self.fill(101), 2)
        v = self.values()
        self.assertEqual(v[10], Decimal("0.00"))    # complete shifts, no PP overlap
        self.assertEqual(v[16], Decimal("0.00"))    # opponent with no PP windows; duplicate shift counted once
        self.assertIsNone(v[11])                     # overlaps the PP window
        self.assertIsNone(v[12])                     # no shift rows
        self.assertIsNone(v[13])                     # shifts cover 5 of 10 minutes
        self.assertEqual(v[14], Decimal("1.25"))     # populated value untouched
        self.assertIsNone(v[15])                     # recorded a PP shot on goal
        self.assertEqual(self.fill(102), 0)          # game without manpower segments
        self.assertIsNone(self.values()[20])
        self.assertEqual(self.fill(101), 0)          # replay is idempotent


if __name__ == "__main__":
    unittest.main()
