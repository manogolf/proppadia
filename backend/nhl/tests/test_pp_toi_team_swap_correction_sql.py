"""The prepared PP-TOI team-swap correction (P2.31) is executor-valid and all-or-nothing.

The database cases run the exact repair and rollback statements against a
uniquely named schema on the configured test DSN, inside one transaction that
is always rolled back.
"""
import os
import re
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg

from backend.nhl.source_correction_access import validate_correction_sql


ROOT = Path(__file__).resolve().parents[3]
if (ROOT / "backend/.env").is_file():
    from dotenv import load_dotenv
    load_dotenv(ROOT / "backend/.env", override=False)
DSN = os.environ.get("NHL_PP_TOI_TEST_DSN")
REPAIRS = ROOT / "backend/nhl/sql/repairs"
REPAIR = REPAIRS / "repair_nhl_pp_toi_team_swap_2025.sql"
ROLLBACK = REPAIRS / "rollback_nhl_pp_toi_team_swap_2025.sql"
VERIFY = REPAIRS / "verify_nhl_pp_toi_team_swap_2025.sql"
EDITOR = REPAIRS / "operator_sql_editor_pp_toi_team_swap_2025.sql"
CORRECTION_ROLE = "proppadia_nhl_source_correction"
ROW = re.compile(r"\((\d+)(?:::bigint)?, (\d+)(?:::bigint)?, (\d+)(?:::bigint)?, "
                 r"([0-9.]+)(?:::numeric)?, ([0-9.]+)(?:::numeric)?\)")


def correction_rows():
    """(game_id, player_id, raw_team_id, original, corrected) from the repair's VALUES."""
    head = REPAIR.read_text().split("AND (SELECT", 1)[0]
    return [(int(g), int(p), int(t), Decimal(a), Decimal(b)) for g, p, t, a, b in ROW.findall(head)]


class PreparedCorrectionShapeTests(unittest.TestCase):
    def test_both_directions_pass_the_governed_executor_validator(self):
        for path in (REPAIR, ROLLBACK):
            with self.subTest(path=path.name):
                validate_correction_sql(path.read_text())

    def test_sql_editor_block_runs_the_reviewed_update_verbatim(self):
        repair = REPAIR.read_text()
        statement = repair[repair.index("UPDATE nhl.skater_game_logs_raw"):].rstrip()
        editor = EDITOR.read_text()
        self.assertEqual(editor.count(statement), 1)
        order = ["BEGIN;", f"GRANT {CORRECTION_ROLE} TO postgres;", f"SET LOCAL ROLE {CORRECTION_ROLE};",
                 statement, "RESET ROLE;", f"REVOKE {CORRECTION_ROLE} FROM postgres;", "COMMIT;"]
        positions = [editor.index(step) for step in order]
        self.assertEqual(positions, sorted(positions))

    def test_scope_is_exactly_119_distinct_keys_in_5_games(self):
        rows = correction_rows()
        self.assertEqual(len(rows), 119)
        self.assertEqual(len({(g, p) for g, p, *_ in rows}), 119)
        self.assertEqual({g for g, *_ in rows}, {2025020493, 2025020605, 2025020619, 2025020822, 2025020831})
        self.assertTrue(all(original != corrected for *_, original, corrected in rows))


@unittest.skipUnless(DSN, "set NHL_PP_TOI_TEST_DSN for the rollback-only PostgreSQL exercise")
class PreparedCorrectionDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.schema = "pp_toi_swap_" + uuid.uuid4().hex[:12]
        self.conn = psycopg.connect(DSN)
        self.conn.execute("BEGIN")
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'''CREATE TABLE "{self.schema}".skater_game_logs_raw(
            game_id bigint, player_id bigint, team_id bigint, pp_toi_minutes numeric(5,2),
            PRIMARY KEY(player_id, game_id))''')
        with self.conn.cursor() as cursor:
            cursor.executemany(
                f'INSERT INTO "{self.schema}".skater_game_logs_raw VALUES (%s,%s,%s,%s)',
                [(g, p, t, original) for g, p, t, original, _ in correction_rows()]
                + [(2025020822, 1, 23, Decimal("1.11"))])  # an untouched neighbour

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def run_file(self, path):
        return self.conn.execute(path.read_text().replace("nhl.", f'"{self.schema}".'))

    def values(self):
        return dict(((g, p), v) for g, p, v in self.conn.execute(
            f'SELECT game_id, player_id, pp_toi_minutes FROM "{self.schema}".skater_game_logs_raw'))

    def test_repair_then_rollback_change_exactly_the_119_values(self):
        rows = correction_rows()
        self.assertEqual(self.run_file(REPAIR).rowcount, 119)
        after = self.values()
        self.assertTrue(all(after[(g, p)] == corrected for g, p, _, _, corrected in rows))
        self.assertEqual(after[(2025020822, 1)], Decimal("1.11"))
        self.assertEqual(self.run_file(ROLLBACK).rowcount, 119)
        restored = self.values()
        self.assertTrue(all(restored[(g, p)] == original for g, p, _, original, _ in rows))

    def test_one_changed_row_refuses_the_whole_correction(self):
        g, p, *_ = correction_rows()[5]
        self.conn.execute(
            f'UPDATE "{self.schema}".skater_game_logs_raw SET pp_toi_minutes = 9.99 '
            "WHERE game_id = %s AND player_id = %s", (g, p))
        before = self.values()
        self.conn.execute("SAVEPOINT guard")
        with self.assertRaisesRegex(psycopg.Error, "PP_TOI_TEAM_SWAP_GUARD_FAILED:matching_rows=118"):
            self.run_file(REPAIR)
        self.conn.execute("ROLLBACK TO SAVEPOINT guard")
        self.assertEqual(self.values(), before)

    def test_sql_editor_block_grants_switches_corrects_and_revokes(self):
        # Exercise the block's own transaction body under a stand-in correction
        # role on the test project; the outer transaction rolls everything back.
        exists = self.conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (CORRECTION_ROLE,)).fetchone()
        if exists:
            self.skipTest("test project already has the correction role")
        self.conn.execute(f"CREATE ROLE {CORRECTION_ROLE} NOLOGIN")
        self.conn.execute(f'GRANT USAGE ON SCHEMA "{self.schema}" TO {CORRECTION_ROLE}')
        self.conn.execute(f'GRANT SELECT ON "{self.schema}".skater_game_logs_raw TO {CORRECTION_ROLE}')
        self.conn.execute(
            f'GRANT UPDATE (pp_toi_minutes) ON "{self.schema}".skater_game_logs_raw TO {CORRECTION_ROLE}')
        session = self.conn.execute("SELECT session_user").fetchone()[0]
        body = EDITOR.read_text().split("BEGIN;", 1)[1].split("COMMIT;", 1)[0]
        body = body.replace("nhl.", f'"{self.schema}".').replace("TO postgres", f'TO "{session}"')
        body = body.replace("FROM postgres", f'FROM "{session}"').replace("'postgres'", f"'{session}'")
        self.conn.execute(body)
        rows = correction_rows()
        after = self.values()
        self.assertTrue(all(after[(g, p)] == corrected for g, p, _, _, corrected in rows))
        identity = self.conn.execute(
            "SELECT current_user, pg_has_role(session_user, %s, 'MEMBER')", (CORRECTION_ROLE,)).fetchone()
        self.assertEqual(identity, (session, False))

    def test_receipt_query_counts_both_states(self):
        query = VERIFY.read_text().replace("nhl.", f'"{self.schema}".')
        body = query.split("BEGIN READ ONLY;", 1)[1].rsplit("COMMIT;", 1)[0]
        self.assertEqual(self.conn.execute(body).fetchone()[2:6], (119, 0, 0, 0))
        self.run_file(REPAIR)
        self.assertEqual(self.conn.execute(body).fetchone()[2:6], (0, 119, 0, 0))


if __name__ == "__main__":
    unittest.main()
