"""Season-2025 shift-chart payload correction (P2.31): only known patterns change, all-or-nothing."""
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path

import pandas as pd
import psycopg

from backend.nhl.scripts.build_nhl_shiftcharts_rollback_sql import build as build_rollback

ROOT = Path(__file__).resolve().parents[3]
if (ROOT / "backend/.env").is_file():
    from dotenv import load_dotenv
    load_dotenv(ROOT / "backend/.env", override=False)
DSN = os.environ.get("NHL_PP_TOI_TEST_DSN")
REPAIR = ROOT / "backend/nhl/sql/repairs/repair_nhl_shiftcharts_2025_from_payload.sql"
VERIFY = ROOT / "backend/nhl/sql/repairs/verify_nhl_shiftcharts_2025_from_payload.sql"


def payload(game, team, start, end, duration=None, type_code=517):
    return json.dumps({"gameId": game, "teamId": team, "startTime": start, "endTime": end,
                       "duration": duration, "typeCode": type_code})


# shift_id, game_id, team_id, period, start_sec, end_sec, duration_sec, raw_json
ROWS = [
    (1, 2025020001, 14, 2, 1500, 1545, 45, payload(2025020001, 14, "05:00", "05:45", "00:45")),   # whole-game seconds
    (2, 2025020001, 14, 1, None, None, None, payload(2025020001, 14, "01:00", "01:40", "00:40")),  # missing seconds/duration
    (3, 2025020001, None, 1, 100, 140, 40, payload(2025020001, 18, "01:40", "02:20", "00:40")),    # missing team
    (4, 2025020001, 18, 1, 200, 230, 30, payload(2025020001, 18, "03:20", "03:50", "00:30")),      # already correct
    (5, 2025020001, 18, 1, 777, 800, 23, payload(2025020001, 18, "05:00", "05:23", "00:23")),      # other conflict: untouched
    (6, 2025020001, 18, 3, 2900, 2900, None, payload(2025020001, 18, "08:20", "08:20", None, 505)),  # goal event, whole-game
    (7, 2026020001, 14, 2, 1500, 1545, 45, payload(2026020001, 14, "05:00", "05:45", "00:45")),   # other season: untouched
]


class ShapeTests(unittest.TestCase):
    def test_repair_is_guarded_and_payload_driven(self):
        sql = REPAIR.read_text()
        self.assertIn(":expected_rows", sql)
        self.assertIn("SHIFTCHARTS_2025_GUARD_FAILED", sql)
        self.assertIn("start_sec - ps IN (0, 1200, 2400, 3600, 4800)", sql)
        self.assertIn("(raw_json->>'gameId')::bigint = game_id", sql)
        self.assertNotIn("DELETE", sql.upper())


@unittest.skipUnless(DSN, "set NHL_PP_TOI_TEST_DSN for the rollback-only PostgreSQL exercise")
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.schema = "shiftfix_" + uuid.uuid4().hex[:12]
        self.conn = psycopg.connect(DSN)
        self.conn.execute("BEGIN")
        q = f'"{self.schema}"'
        self.conn.execute(f"CREATE SCHEMA {q}")
        self.conn.execute(f"""CREATE TABLE {q}.shiftcharts_raw(shift_id bigint PRIMARY KEY, game_id bigint, team_id bigint,
            period int, start_sec int, end_sec int, duration_sec int, raw_json jsonb)""")
        with self.conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {q}.shiftcharts_raw VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)", ROWS)

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def sql(self, path, expected=None):
        text = path.read_text().replace("nhl.", f'"{self.schema}".')
        return text.replace(":expected_rows", str(expected)) if expected is not None else text

    def state(self):
        return {r[0]: r[1:] for r in self.conn.execute(
            f'SELECT shift_id, team_id, start_sec, end_sec, duration_sec FROM "{self.schema}".shiftcharts_raw')}

    def test_only_known_patterns_change(self):
        self.assertEqual(self.conn.execute(self.sql(REPAIR, 4)).rowcount, 4)
        s = self.state()
        self.assertEqual(s[1], (14, 300, 345, 45))
        self.assertEqual(s[2], (14, 60, 100, 40))
        self.assertEqual(s[3], (18, 100, 140, 40))
        self.assertEqual(s[4], (18, 200, 230, 30))
        self.assertEqual(s[5], (18, 777, 800, 23))
        self.assertEqual(s[6], (18, 500, 500, None))
        self.assertEqual(s[7], (14, 1500, 1545, 45))
        body = self.sql(VERIFY).split("BEGIN READ ONLY;", 1)[1].rsplit("COMMIT;", 1)[0]
        self.assertEqual(self.conn.execute(body).fetchone()[2:], (0, 0, 0, 0, 1))

    def test_wrong_count_refuses_everything(self):
        before = self.state()
        self.conn.execute("SAVEPOINT guard")
        with self.assertRaisesRegex(psycopg.Error, "SHIFTCHARTS_2025_GUARD_FAILED:matching_rows=4"):
            self.conn.execute(self.sql(REPAIR, 5))
        self.conn.execute("ROLLBACK TO SAVEPOINT guard")
        self.assertEqual(self.state(), before)

    def test_generated_rollback_restores_originals(self):
        before = self.state()
        snap = pd.DataFrame([
            {"shift_id": sid, "start_sec": st, "end_sec": en, "duration_sec": du, "team_id": tm} for sid, _, tm, _, st, en, du, _ in ROWS if sid in (1, 2, 3, 6)])
        self.conn.execute(self.sql(REPAIR, 4))
        after = self.state()
        snap["new_team"], snap["new_start"], snap["new_end"], snap["new_duration"] = zip(*[after[s] for s in snap.shift_id])
        with tempfile.TemporaryDirectory(dir=ROOT / "tmp") as tmp:
            snap_path, out = Path(tmp) / "snap.csv", Path(tmp) / "rollback.sql"
            snap.to_csv(snap_path, index=False)
            build_rollback(snap_path, out)
            self.assertEqual(self.conn.execute(self.sql(out)).rowcount, 4)
        self.assertEqual(self.state(), before)


if __name__ == "__main__":
    unittest.main()
