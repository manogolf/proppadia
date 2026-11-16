"""Official PP TOI fill from the NHL stats API (P2.45): migration contract and daily call."""
from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from backend.nhl.scripts import fill_pp_toi_minutes_for_date as fill

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "backend/nhl/sql/migrations/20261115_pp_toi_official_fill.sql"
ROLLBACK = ROOT / "backend/nhl/sql/migrations/20261115_pp_toi_official_fill_rollback.sql"
EDITOR = ROOT / "backend/nhl/sql/repairs/operator_sql_editor_pp_toi_official_fill.sql"

OFFICIAL = [{"player_id": 1, "pp_seconds": 120, "toi_seconds": 960},
            {"player_id": 2, "pp_seconds": 0, "toi_seconds": 900}]


class FakeCursor:
    def __init__(self, official_available: bool, zero_available: bool = True):
        self.official_available = official_available
        self.zero_available = zero_available
        self.calls: list[tuple[str, tuple]] = []
        self._next = None

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if "to_regprocedure" in sql:
            official = bool(params) and "from_official" in str(params[0])
            self._next = (self.official_available if official else self.zero_available,)
        elif "fill_pp_toi_minutes_from_official" in sql:
            self._next = (len(json.loads(params[1])),)
        elif "fill_pp_toi_zero_from_shift_overlap" in sql:
            self._next = (3,)
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

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


def _invariant_patch():
    """Keep an outer patch of the invariant when a test installs one."""
    from contextlib import nullcontext
    if isinstance(fill.assert_no_bad_team_games, unittest.mock.Mock):
        return nullcontext()
    return patch.object(fill, "assert_no_bad_team_games", return_value=0)


def run_main(cursor, official_rows, games=(2026020101, 2026020102)):
    out, err = StringIO(), StringIO()
    overlap = patch.object(fill, "extract_pp_toi_minutes_from_db", return_value={1: 2.0})
    with patch.object(fill.psycopg, "connect", return_value=FakeConn(cursor)), \
         patch.object(fill, "database_url", return_value="postgresql://fixture"), \
         patch.object(fill, "get_games_for_date", return_value=list(games)), \
         patch.object(fill, "skip_voided_games", side_effect=lambda ids: ids), \
         patch.object(fill, "fetch_official_pp", side_effect=official_rows) as fetch, \
         overlap as extract, \
         patch.object(fill, "update_game", return_value=(20, 1, 1)), \
         _invariant_patch(), \
         patch.object(fill, "write_pp_toi_conflict_diagnostic", return_value={"conflict_count": 0}), \
         patch.object(fill.time, "sleep"), \
         patch.object(sys, "argv", ["fill", "--date", "2026-11-15", "--commit"]), \
         redirect_stdout(out), redirect_stderr(err):
        fill.main()
    line = next(l for l in out.getvalue().splitlines() if l.startswith("NHL_CHILD_SUMMARY_JSON="))
    return json.loads(line.split("=", 1)[1]), fetch, extract, err.getvalue()


class DailyCallTests(unittest.TestCase):
    def test_official_source_fills_each_game_and_skips_shift_overlap(self):
        cursor = FakeCursor(official_available=True)
        summary, fetch, extract, _ = run_main(cursor, [OFFICIAL, OFFICIAL])
        self.assertTrue(summary["official_fill_function_available"])
        self.assertEqual((summary["official_fills"], summary["official_source_games"], summary["shift_overlap_games"]),
                         (4, 2, 0))
        self.assertEqual(summary["filled_nulls"], 0)
        self.assertEqual(summary["verified_zero_fills"], 0)
        self.assertEqual(extract.call_count, 0)
        calls = [p for sql, p in cursor.calls if "fill_pp_toi_minutes_from_official" in sql]
        self.assertEqual([c[0] for c in calls], [2026020101, 2026020102])
        self.assertEqual(json.loads(calls[0][1]), OFFICIAL)

    def test_a_game_not_yet_in_the_stats_api_is_left_null_not_shift_overlap(self):
        # 2026-11-15 VAN-VGK: the postgame run asked 40 minutes after the horn, got no rows,
        # and the shift-overlap fallback locked in values the official fill could never replace.
        cursor = FakeCursor(official_available=True)
        with patch.object(fill, "assert_no_bad_team_games", return_value=0) as invariant:
            summary, _, extract, err = run_main(cursor, [OFFICIAL, []])
        self.assertEqual((summary["official_source_games"], summary["shift_overlap_games"],
                          summary["official_pending_games"]), (1, 0, 1))
        self.assertEqual((summary["filled_nulls"], summary["verified_zero_fills"]), (0, 0))
        self.assertEqual(extract.call_count, 0)
        self.assertFalse([c for c in cursor.calls if "zero_from_shift_overlap(" in c[0] and "SELECT nhl." in c[0]])
        self.assertIn("left NULL for a later run", err)
        self.assertEqual(invariant.call_args.args[2], [2026020102])

    def test_a_fetch_failure_is_left_null_not_shift_overlap(self):
        cursor = FakeCursor(official_available=True)
        summary, _, extract, err = run_main(cursor, [RuntimeError("boom"), OFFICIAL])
        self.assertEqual((summary["official_source_games"], summary["shift_overlap_games"],
                          summary["official_pending_games"]), (1, 0, 1))
        self.assertEqual(extract.call_count, 0)
        self.assertIn("stats API PP fetch failed", err)

    def test_the_team_invariant_skips_pending_games(self):
        cursor = FakeCursor(official_available=True)
        cursor.fetchone = lambda: (0,)
        fill.assert_no_bad_team_games(cursor, "2026-11-15", [2026020329])
        sql, params = cursor.calls[-1]
        self.assertIn("NOT (g.game_id = ANY(%s::bigint[]))", sql)
        self.assertEqual(params, ("2026-11-15", [2026020329], "2026-11-15"))

    def test_without_the_migration_the_stats_api_is_not_called(self):
        cursor = FakeCursor(official_available=False)
        summary, fetch, extract, err = run_main(cursor, [OFFICIAL, OFFICIAL])
        self.assertFalse(summary["official_fill_function_available"])
        self.assertEqual(fetch.call_count, 0)
        self.assertEqual(summary["shift_overlap_games"], 2)
        self.assertIn("official PP fill function not installed", err)

    def test_fill_calls_the_fixed_function_and_never_updates_directly(self):
        cursor = FakeCursor(official_available=True)
        self.assertEqual(fill.fill_from_official(cursor, 2026020101, OFFICIAL), 2)
        sql, params = cursor.calls[-1]
        self.assertIn("nhl.fill_pp_toi_minutes_from_official", sql)
        self.assertNotIn("UPDATE", sql.upper())

    def test_fetch_uses_the_governed_client_with_a_game_identity(self):
        response = unittest.mock.Mock()
        response.json.return_value = {"data": [{"gameId": 2026020101, "playerId": 1, "ppTimeOnIce": 30,
                                                "timeOnIce": 600}], "total": 1}
        with patch.object(fill, "official_get", return_value=response) as get:
            rows = fill.fetch_official_pp(2026020101)
        self.assertEqual(rows, [{"player_id": 1, "pp_seconds": 30, "toi_seconds": 600}])
        kwargs = get.call_args.kwargs
        self.assertEqual(kwargs["endpoint_family"], "SKATER_TIME_ON_ICE")
        self.assertEqual(kwargs["identity"]["game_id"], 2026020101)
        self.assertEqual(kwargs["params"]["cayenneExp"], "gameId=2026020101")

    def test_fetch_rejects_a_truncated_or_wrong_game_response(self):
        for payload in ({"data": [], "total": 3},
                        {"data": [{"gameId": 9, "playerId": 1, "ppTimeOnIce": 0, "timeOnIce": 1}], "total": 1}):
            response = unittest.mock.Mock()
            response.json.return_value = payload
            with patch.object(fill, "official_get", return_value=response), self.assertRaises(RuntimeError):
                fill.fetch_official_pp(2026020101)

    def test_parent_receipt_admits_the_new_counts(self):
        source = (ROOT / "backend/nhl/daily_orchestration.py").read_text()
        allowed = source.split("allowed = {", 1)[1].split("}", 1)[0]
        for key in ("official_fill_function_available", "official_fills", "official_source_games",
                    "shift_overlap_games", "official_pending_games"):
            self.assertIn(f'"{key}"', allowed)


class MigrationContractTests(unittest.TestCase):
    def test_function_is_fixed_security_definer_null_only_with_toi_identity_check(self):
        sql = MIGRATION.read_text().lower()
        self.assertIn("preparation migration only", sql)
        self.assertEqual(sql.count("create or replace function"), 1)
        self.assertIn("security definer", sql)
        self.assertIn("set search_path = pg_catalog, nhl", sql)
        self.assertNotRegex(sql, r"\bexecute\s+(format|sql)\b")
        for guard in ("raw.pp_toi_minutes is null", "r.pp_seconds >= 0 and r.pp_seconds <= r.toi_seconds",
                      "abs(raw.toi_minutes - official.toi_seconds / 60.0) <= 0.05", "raw.game_id = p_game_id",
                      "round(official.pp_seconds / 60.0, 2)"):
            self.assertIn(guard, sql)

    def test_execution_is_restricted_to_ingest(self):
        sql = MIGRATION.read_text().lower()
        self.assertIn("revoke all on function nhl.fill_pp_toi_minutes_from_official(bigint, jsonb)\n"
                      "  from public, anon, authenticated, service_role", sql)
        self.assertEqual(sql.count("grant execute on function"), 1)
        self.assertIn("to proppadia_nhl_ingest", sql)

    def test_rollback_only_removes_the_entry_point(self):
        sql = ROLLBACK.read_text().lower()
        self.assertIn("drop function nhl.fill_pp_toi_minutes_from_official(bigint, jsonb)", sql)
        self.assertNotIn("update nhl.skater_game_logs_raw", sql)

    def test_operator_editor_block_applies_the_migration_unchanged(self):
        def body(sql: str) -> str:
            return sql[sql.index("BEGIN;\n") + len("BEGIN;\n"):sql.rindex("COMMIT;")]
        editor = EDITOR.read_text()
        self.assertIn(body(MIGRATION.read_text()), editor)
        self.assertIn("RUN_AS_POSTGRES_IN_SQL_EDITOR", editor)
        self.assertEqual((editor.count("BEGIN;"), editor.count("COMMIT;")), (1, 1))


if __name__ == "__main__":
    unittest.main()
