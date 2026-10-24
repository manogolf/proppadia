"""The prepared season-2025 strength-split SOG correction (P2.31) is all-or-nothing.

Database cases run the exact repair and rollback against a uniquely named schema
on the rollback-only test DSN, inside one transaction that is always rolled back.
"""
import os
import re
import unittest
import uuid
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[3]
if (ROOT / "backend/.env").is_file():
    from dotenv import load_dotenv
    load_dotenv(ROOT / "backend/.env", override=False)
DSN = os.environ.get("NHL_PP_TOI_TEST_DSN")
REPAIRS = ROOT / "backend/nhl/sql/repairs"
REPAIR = REPAIRS / "repair_nhl_strength_sog_2025.sql"
ROLLBACK = REPAIRS / "rollback_nhl_strength_sog_2025.sql"
VERIFY = REPAIRS / "verify_nhl_strength_sog_2025.sql"
ROW = re.compile(r"\((\d+)(?:::bigint)?, (\d+)(?:::bigint)?, (\d+)(?:::bigint)?, (\d+)(?:::int)?, (\d+)(?:::int)?, "
                 r"(\d+)(?:::int)?, (\d+)(?:::int)?, (\d+)(?:::int)?, (\d+)(?:::int)?\)")
N = 2860


def rows():
    body = REPAIR.read_text().split("UPDATE nhl.skater_game_logs_raw", 1)[0]
    return [tuple(int(x) for x in m) for m in ROW.findall(body)]


class ShapeTests(unittest.TestCase):
    def test_scope_and_values(self):
        r = rows()
        self.assertEqual(len(r), N)
        self.assertEqual(len({(g, p) for g, p, *_ in r}), N)
        self.assertTrue(all(str(g).startswith("2025") for g, *_ in r))
        self.assertTrue(all(min(v) >= 0 for *_, fe, fp, fs, te, tp, ts in r for v in [(fe, fp, fs, te, tp, ts)]))
        self.assertTrue(all((fe, fp, fs) != (te, tp, ts) for _, _, _, fe, fp, fs, te, tp, ts in r))

    def test_corrected_split_adds_up_to_total_shots(self):
        proposal = ROOT / "artifacts/analysis/nhl/strength_sog_correction_2026-10-24/correction_proposal_2025.csv"
        if not proposal.is_file():
            self.skipTest("evidence package is local and git-ignored")
        import csv
        sog = {(int(r["game_id"]), int(r["player_id"])): float(r["sog"]) for r in csv.DictReader(proposal.open())}
        self.assertTrue(all(te + tp + ts == sog[(g, p)] for g, p, _, _, _, _, te, tp, ts in rows()))

    def test_only_strength_columns_change(self):
        for path in (REPAIR, ROLLBACK):
            sql = path.read_text().lower()
            self.assertIn("set ev_sog = c.", sql)
            self.assertNotIn("pp_toi_minutes =", sql)
            self.assertNotIn("shot_attempts =", sql)


@unittest.skipUnless(DSN, "set NHL_PP_TOI_TEST_DSN for the rollback-only PostgreSQL exercise")
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.schema = "strength_sog_" + uuid.uuid4().hex[:12]
        self.conn = psycopg.connect(DSN)
        self.conn.execute("BEGIN")
        q = f'"{self.schema}"'
        self.conn.execute(f"CREATE SCHEMA {q}")
        self.conn.execute(f"""CREATE TABLE {q}.skater_game_logs_raw(game_id bigint, player_id bigint, team_id bigint,
            ev_sog integer, pp_sog integer, sh_sog integer, PRIMARY KEY(player_id, game_id))""")
        with self.conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {q}.skater_game_logs_raw VALUES (%s,%s,%s,%s,%s,%s)",
                            [(g, p, t, fe, fp, fs) for g, p, t, fe, fp, fs, *_ in rows()] + [(2025020822, 1, 23, 1, 2, 3)])

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def run_file(self, path):
        return self.conn.execute(path.read_text().replace("nhl.", f'"{self.schema}".'))

    def values(self):
        return {(g, p): (e, pp, s) for g, p, e, pp, s in self.conn.execute(
            f'SELECT game_id, player_id, ev_sog, pp_sog, sh_sog FROM "{self.schema}".skater_game_logs_raw')}

    def test_repair_then_rollback(self):
        self.assertEqual(self.run_file(REPAIR).rowcount, N)
        after = self.values()
        self.assertTrue(all(after[(g, p)] == (te, tp, ts) for g, p, _, _, _, _, te, tp, ts in rows()))
        self.assertEqual(after[(2025020822, 1)], (1, 2, 3))
        self.assertEqual(self.run_file(ROLLBACK).rowcount, N)
        self.assertTrue(all(self.values()[(g, p)] == (fe, fp, fs) for g, p, _, fe, fp, fs, *_ in rows()))

    def test_one_changed_row_refuses_everything(self):
        g, p, *_ = rows()[10]
        self.conn.execute(f'UPDATE "{self.schema}".skater_game_logs_raw SET pp_sog = 99 WHERE game_id=%s AND player_id=%s', (g, p))
        before = self.values()
        self.conn.execute("SAVEPOINT guard")
        with self.assertRaisesRegex(psycopg.Error, f"STRENGTH_SOG_2025_GUARD_FAILED:matching_rows={N - 1}"):
            self.run_file(REPAIR)
        self.conn.execute("ROLLBACK TO SAVEPOINT guard")
        self.assertEqual(self.values(), before)

    def test_receipt_counts_both_states(self):
        body = VERIFY.read_text().replace("nhl.", f'"{self.schema}".')
        body = body.split("BEGIN READ ONLY;", 1)[1].rsplit("COMMIT;", 1)[0]
        self.assertEqual(self.conn.execute(body).fetchone()[2:6], (N, 0, 0, 0))
        self.run_file(REPAIR)
        self.assertEqual(self.conn.execute(body).fetchone()[2:6], (0, N, 0, 0))


if __name__ == "__main__":
    unittest.main()
