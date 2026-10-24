"""Generated strength-split SOG correction SQL is all-or-nothing (P2.31)."""
import os
import re
import tempfile
import unittest
import uuid
from pathlib import Path

import pandas as pd
import psycopg

from backend.nhl.scripts.build_nhl_strength_sog_correction_sql import build

ROOT = Path(__file__).resolve().parents[3]
if (ROOT / "backend/.env").is_file():
    from dotenv import load_dotenv
    load_dotenv(ROOT / "backend/.env", override=False)
DSN = os.environ.get("NHL_PP_TOI_TEST_DSN")
EVIDENCE = ROOT / "artifacts/analysis/nhl/strength_sog_correction_2026-10-24"
ROW = re.compile(r"\((\d+)(?:::bigint)?, (\d+)(?:::bigint)?, (\d+)(?:::bigint)?, (\d+)(?:::int)?, (\d+)(?:::int)?, "
                 r"(\d+)(?:::int)?, (\d+)(?:::int)?, (\d+)(?:::int)?, (\d+)(?:::int)?\)")
SYNTH = pd.DataFrame([
    {"game_id": 2023020001, "player_id": 1, "team_id": 14, "ev": 2, "pp": 0, "sh": 1, "pbp_ev": 2, "pbp_pp": 1, "pbp_sh": 0},
    {"game_id": 2023020001, "player_id": 2, "team_id": 18, "ev": 1, "pp": 1, "sh": 0, "pbp_ev": 2, "pbp_pp": 0, "pbp_sh": 0},
    {"game_id": 2024020005, "player_id": 3, "team_id": 7, "ev": 0, "pp": 0, "sh": 2, "pbp_ev": 0, "pbp_pp": 2, "pbp_sh": 0},
])


def parsed(path):
    return [tuple(int(x) for x in m) for m in ROW.findall(path.read_text().split("UPDATE nhl.skater_game_logs_raw", 1)[0])]


class GeneratorTests(unittest.TestCase):
    def test_season_split_and_files(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as tmp:
            proposal = Path(tmp) / "proposal.csv"
            SYNTH.to_csv(proposal, index=False)
            out = build(proposal, "2023", Path(tmp))
            self.assertEqual(out["rows"], 2)
            repair = Path(tmp) / "repair_nhl_strength_sog_2023.sql"
            self.assertEqual([r[:2] for r in parsed(repair)], [(2023020001, 1), (2023020001, 2)])
            self.assertIn("STRENGTH_SOG_2023_GUARD_FAILED", repair.read_text())
            self.assertIn("STRENGTH_SOG_2023_ROLLBACK_GUARD_FAILED", (Path(tmp) / "rollback_nhl_strength_sog_2023.sql").read_text())
            self.assertIn("BEGIN READ ONLY;", (Path(tmp) / "verify_nhl_strength_sog_2023.sql").read_text())


@unittest.skipUnless(DSN, "set NHL_PP_TOI_TEST_DSN for the rollback-only PostgreSQL exercise")
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.schema = "strength_gen_" + uuid.uuid4().hex[:12]
        self.conn = psycopg.connect(DSN)
        self.conn.execute("BEGIN")
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn.execute(f'''CREATE TABLE "{self.schema}".skater_game_logs_raw(game_id bigint, player_id bigint,
            team_id bigint, ev_sog integer, pp_sog integer, sh_sog integer, PRIMARY KEY(player_id, game_id))''')

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def load(self, rows):
        with self.conn.cursor() as cur:
            cur.executemany(f'INSERT INTO "{self.schema}".skater_game_logs_raw VALUES (%s,%s,%s,%s,%s,%s)',
                            [(g, p, t, fe, fp, fs) for g, p, t, fe, fp, fs, *_ in rows])

    def run_file(self, path):
        return self.conn.execute(path.read_text().replace("nhl.", f'"{self.schema}".'))

    def exercise(self, repair, rollback, rows, label):
        self.load(rows)
        self.assertEqual(self.run_file(repair).rowcount, len(rows))
        got = {(g, p): v for g, p, *v in self.conn.execute(
            f'SELECT game_id, player_id, ev_sog, pp_sog, sh_sog FROM "{self.schema}".skater_game_logs_raw')}
        self.assertTrue(all(got[(g, p)] == [te, tp, ts] for g, p, _, _, _, _, te, tp, ts in rows))
        self.assertEqual(self.run_file(rollback).rowcount, len(rows))
        g, p, *_ = rows[0]
        self.conn.execute(f'UPDATE "{self.schema}".skater_game_logs_raw SET pp_sog = 99 WHERE game_id=%s AND player_id=%s', (g, p))
        self.conn.execute("SAVEPOINT guard")
        with self.assertRaisesRegex(psycopg.Error, f"{label}_GUARD_FAILED:matching_rows={len(rows) - 1}"):
            self.run_file(repair)
        self.conn.execute("ROLLBACK TO SAVEPOINT guard")

    def test_synthetic_season(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as tmp:
            proposal = Path(tmp) / "proposal.csv"
            SYNTH.to_csv(proposal, index=False)
            build(proposal, "2024", Path(tmp))
            repair = Path(tmp) / "repair_nhl_strength_sog_2024.sql"
            self.exercise(repair, Path(tmp) / "rollback_nhl_strength_sog_2024.sql", parsed(repair), "STRENGTH_SOG_2024")

    def test_generated_evidence_files_when_present(self):
        for season in ("2023", "2024"):
            repair = EVIDENCE / f"repair_nhl_strength_sog_{season}.sql"
            if not repair.is_file():
                self.skipTest("generated evidence files are local and git-ignored")
            with self.subTest(season=season):
                self.conn.execute("SAVEPOINT season")
                self.exercise(repair, EVIDENCE / f"rollback_nhl_strength_sog_{season}.sql", parsed(repair), f"STRENGTH_SOG_{season}")
                self.conn.execute("ROLLBACK TO SAVEPOINT season")


if __name__ == "__main__":
    unittest.main()
