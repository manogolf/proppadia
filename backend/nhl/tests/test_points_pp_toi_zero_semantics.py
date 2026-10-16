from __future__ import annotations

import importlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from backend.nhl.pp_toi_source import (
    pp_toi_conflict,
    preserve_existing_pp_toi_conflict_assignment,
    write_pp_toi_conflict_diagnostic,
)

with patch.dict(os.environ, {
    "SUPABASE_DB_URL": "postgresql://test",
    "SLATE_DATE": "2026-01-01",
}):
    importer = importlib.import_module("backend.nhl.scripts.seed_skater_logs_for_date")


class PpToiZeroSemanticsTests(unittest.TestCase):
    def test_parser_preserves_explicit_numeric_and_text_zero_and_missing_as_none(self):
        self.assertEqual(importer.extract_pp_toi_minutes({"ppToi": 0}), 0.0)
        self.assertEqual(importer.extract_pp_toi_minutes({"ppToi": "0:00"}), 0.0)
        self.assertEqual(importer.extract_pp_toi_minutes({"powerPlayToi": "2:30"}), 2.5)
        self.assertIsNone(importer.extract_pp_toi_minutes({}))

    def test_actual_stage_upsert_preserves_zero_and_replaces_missing_snapshot_with_null(self):
        class CaptureCursor:
            def __init__(self):
                self.sql = []
                self.rows = []

            def execute(self, sql):
                self.sql.append(sql)

            def fetchall(self):
                return [(name,) for name in (
                    "player_id", "game_id", "game_date", "team_id", "shots_on_goal",
                    "shot_attempts", "toi_minutes", "pp_toi_minutes",
                )]

            def executemany(self, sql, rows):
                self.sql.append(sql)
                self.rows = list(rows)

        cursor = CaptureCursor()
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        db = sqlite3.connect(":memory:")
        db.execute("ATTACH DATABASE ':memory:' AS nhl")
        db.execute("""CREATE TABLE nhl.import_skater_logs_stage (
            player_id INTEGER, game_id INTEGER, game_date TEXT, team_id INTEGER,
            shots_on_goal INTEGER, shot_attempts INTEGER, toi_minutes REAL,
            pp_toi_minutes REAL, PRIMARY KEY(player_id, game_id))""")

        def ingest(pp_toi):
            importer.upsert_rows(connection, [(
                7, 2023020001, "2023-10-10", 1, 2, 3, 12.0, pp_toi, None,
            )])
            sql = cursor.sql[-1].replace("%s", "?")
            db.execute(sql, cursor.rows[0])
            db.commit()

        ingest(1.25)
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.import_skater_logs_stage").fetchone()[0], 1.25)
        ingest(0.0)
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.import_skater_logs_stage").fetchone()[0], 0.0)
        ingest(None)
        self.assertIsNone(db.execute("SELECT pp_toi_minutes FROM nhl.import_skater_logs_stage").fetchone()[0])
        self.assertIn("pp_toi_minutes  = EXCLUDED.pp_toi_minutes", cursor.sql[-1])
        self.assertNotIn("NULLIF(EXCLUDED.pp_toi_minutes", cursor.sql[-1])

    def test_raw_daily_and_postgame_upsert_preserves_any_existing_value(self):
        assignment = preserve_existing_pp_toi_conflict_assignment("nhl.skater_game_logs_raw")
        db = sqlite3.connect(":memory:")
        db.execute("ATTACH DATABASE ':memory:' AS nhl")
        db.execute("CREATE TABLE nhl.skater_game_logs_raw (player_id INTEGER PRIMARY KEY, pp_toi_minutes REAL)")
        sql = f"""INSERT INTO nhl.skater_game_logs_raw(player_id,pp_toi_minutes) VALUES (?,?)
            ON CONFLICT(player_id) DO UPDATE SET {assignment}"""
        db.execute(sql, (1, 4.0))
        db.execute(sql, (1, 0.0))
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.skater_game_logs_raw WHERE player_id=1").fetchone()[0], 4.0)
        db.execute(sql, (1, None))
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.skater_game_logs_raw WHERE player_id=1").fetchone()[0], 4.0)
        db.execute(sql, (2, None))
        self.assertIsNone(db.execute("SELECT pp_toi_minutes FROM nhl.skater_game_logs_raw WHERE player_id=2").fetchone()[0])
        db.execute(sql, (1, 2.5))
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.skater_game_logs_raw WHERE player_id=1").fetchone()[0], 4.0)
        db.execute(sql, (3, 0.0))
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.skater_game_logs_raw WHERE player_id=3").fetchone()[0], 0.0)
        db.execute(sql, (4, 2.25))
        self.assertEqual(db.execute("SELECT pp_toi_minutes FROM nhl.skater_game_logs_raw WHERE player_id=4").fetchone()[0], 2.25)

        root = Path(__file__).resolve().parents[1]
        daily = (root / "cli.py").read_text()
        postgame = (root / "scripts" / "run_nhl_postgame_reconciliation.py").read_text()
        self.assertIn("preserve_existing_pp_toi_conflict_assignment(\"nhl.skater_game_logs_raw\")", daily)
        self.assertIn("preserve_existing_pp_toi_conflict_assignment(\"nhl.skater_game_logs_raw\")", postgame)
        self.assertNotIn("NULLIF(pp_toi_minutes, 0) AS pp_toi_minutes", daily)
        self.assertNotIn("NULLIF(EXCLUDED.pp_toi_minutes, 0)", daily)
        self.assertNotIn("nullif(s.pp_toi_minutes,0)", postgame)
        self.assertNotIn("coalesce(excluded.pp_toi_minutes,nhl.skater_game_logs_raw.pp_toi_minutes)", postgame)

    def test_fill_sql_only_fills_null_and_reports_disagreements(self):
        from backend.nhl.scripts import fill_pp_toi_minutes_for_date as fill

        db = sqlite3.connect(":memory:")
        db.execute("ATTACH DATABASE ':memory:' AS nhl")
        db.execute("CREATE TABLE nhl.skater_game_logs_raw (game_id INTEGER, player_id INTEGER, team_id INTEGER, pp_toi_minutes REAL, PRIMARY KEY(game_id,player_id))")
        db.executemany("INSERT INTO nhl.skater_game_logs_raw VALUES (?,?,?,?)", [
            (2023020001, 8474151, 18, 0.4),
            (2023020001, 8474564, 27, 0.0),
            (2023020001, 8470001, 18, None),
        ])

        class Cursor:
            def execute(self, sql, params):
                sql = sql.replace("::bigint", "").replace("%s", "?")
                self.result = db.execute(sql, params)

            def fetchall(self):
                return self.result.fetchall()

            def executemany(self, sql, rows):
                changed = 0
                for row in rows:
                    changed += db.execute(sql.replace("nhl.skater_game_logs_raw", "nhl.skater_game_logs_raw").replace("%s", "?"), row).rowcount
                self.rowcount = changed

        cursor = Cursor()
        self.assertEqual(fill.update_game(cursor, 2023020001, {
            8474151: 0.6, 8474564: 0.2, 8470001: 0.3,
        }), (3, 3, 1))
        values = dict(db.execute("SELECT player_id,pp_toi_minutes FROM nhl.skater_game_logs_raw"))
        self.assertEqual(values[8474151], 0.4)
        self.assertEqual(values[8474564], 0.0)
        self.assertEqual(values[8470001], 0.3)
        conflict = pp_toi_conflict(
            game_id=2023020001, player_id=8474564, team_id=27,
            existing_value=0.0, incoming_value=0.2,
            source_kind="RECONSTRUCTED_SHIFT_OVERLAP",
            source_reference="fixture shifts",
        )
        self.assertEqual(conflict["status"], "PRESERVED_EXISTING_VALUE_CONFLICT_REQUIRES_SOURCE_REVIEW")
        self.assertEqual(conflict["team_id"], 27)
        self.assertIsNone(pp_toi_conflict(
            game_id=2023020001, player_id=8470001, team_id=18,
            existing_value=None, incoming_value=0.0, source_kind="fixture",
            source_reference="fixture",
        ))
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {"NHL_PP_TOI_CONFLICT_ROOT": root}):
                artifact = write_pp_toi_conflict_diagnostic(
                    run_id="fixture_run", slate_date="2023-10-10",
                    stage="promotion", conflicts=[conflict],
                    source_reference="official fixture rows",
                    source_evidence_sha256="abc123",
                )
            payload = json.loads(Path(artifact["path"]).read_text())
            self.assertEqual(payload["run_id"], "fixture_run")
            self.assertEqual(payload["conflicts"][0]["game_id"], 2023020001)
            self.assertEqual(payload["conflicts"][0]["player_id"], 8474564)
            self.assertEqual(payload["conflicts"][0]["team_id"], 27)
            self.assertEqual(artifact["conflict_count"], 1)

    def test_postgame_stage_sync_does_not_retain_stale_pp_toi_on_unknown_snapshot(self):
        from backend.nhl.postgame_reconcile import staging_sync

        source = Path(staging_sync.__file__).read_text()
        self.assertIn("pp_toi_minutes=EXCLUDED.pp_toi_minutes", source)
        self.assertNotIn("pp_toi_minutes=COALESCE(EXCLUDED.pp_toi_minutes", source)


if __name__ == "__main__":
    unittest.main()
