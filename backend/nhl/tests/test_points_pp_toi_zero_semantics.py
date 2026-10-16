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

    def test_raw_promoters_leave_protected_fields_out_of_conflict_update(self):
        root = Path(__file__).resolve().parents[1]
        daily = (root / "cli.py").read_text()
        postgame = (root / "scripts" / "run_nhl_postgame_reconciliation.py").read_text()
        self.assertIn("promote_skater_game_log_protected_fields", daily)
        self.assertIn("promote_skater_game_log_protected_fields", postgame)
        self.assertNotIn("preserve_existing_pp_toi_conflict_assignment(\"nhl.skater_game_logs_raw\")", daily)
        self.assertNotIn("preserve_existing_pp_toi_conflict_assignment(\"nhl.skater_game_logs_raw\")", postgame)
        self.assertNotIn("NULLIF(pp_toi_minutes, 0) AS pp_toi_minutes", daily)
        self.assertNotIn("NULLIF(EXCLUDED.pp_toi_minutes, 0)", daily)
        self.assertNotIn("nullif(s.pp_toi_minutes,0)", postgame)
        self.assertNotIn("pp_toi_minutes=excluded.pp_toi_minutes", postgame)

    def test_fill_writer_uses_database_overlap_function(self):
        from backend.nhl.scripts import fill_pp_toi_minutes_for_date as fill

        class Cursor:
            def execute(self, sql, params):
                self.sql = sql
                self.params = params

            def fetchone(self):
                return (1,)

            def fetchall(self):
                return [(8474151,), (8474564,), (8470001,)]

        cursor = Cursor()
        self.assertEqual(fill.update_game(cursor, 2023020001, {
            8474151: 0.6, 8474564: 0.2, 8470001: 0.3,
        }), (3, 3, 1))
        self.assertIn('fill_pp_toi_minutes_from_shift_overlap', cursor.sql)
        self.assertEqual(cursor.params, (2023020001,))
        self.assertNotIn("UPDATE", cursor.sql.upper())
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

    def test_pp_toi_conflict_comparison_uses_persisted_numeric_scale(self):
        self.assertIsNone(pp_toi_conflict(
            game_id=2026020113, player_id=8476462, team_id=1,
            existing_value=1.92, incoming_value=115 / 60,
            source_kind="RECONSTRUCTED_SHIFT_OVERLAP", source_reference="official shifts",
        ))
        conflict = pp_toi_conflict(
            game_id=2026020111, player_id=8480995, team_id=14,
            existing_value=2.18, incoming_value=1.60,
            source_kind="RECONSTRUCTED_SHIFT_OVERLAP", source_reference="official shifts",
        )
        self.assertIsNotNone(conflict)
        self.assertEqual(conflict["existing_pp_toi_minutes"], 2.18)

    def test_manpower_segments_close_at_period_boundaries_and_ignore_shootout(self):
        from backend.nhl.scripts.backfill_game_manpower_segments import segments_for_team_advantage_abs

        plays = [
            {"periodDescriptor": {"number": 4, "periodType": "OT"}, "timeInPeriod": "04:01", "typeDescKey": "faceoff", "situationCode": "1431"},
            {"periodDescriptor": {"number": 4, "periodType": "OT"}, "timeInPeriod": "05:00", "typeDescKey": "period-end", "situationCode": "1431"},
            {"periodDescriptor": {"number": 5, "periodType": "SO"}, "timeInPeriod": "20:00", "typeDescKey": "game-end", "situationCode": "1431"},
        ]
        self.assertEqual(
            segments_for_team_advantage_abs(plays, "AWAY_ADV"),
            [((3 * 1200) + 241, (3 * 1200) + 300)],
        )

    def test_manpower_segments_do_not_bridge_a_real_one_second_gap(self):
        from backend.nhl.scripts.backfill_game_manpower_segments import segments_for_team_advantage_abs

        plays = [
            {"periodDescriptor": {"number": 1, "periodType": "REG"}, "timeInPeriod": "10:00", "typeDescKey": "faceoff", "situationCode": "1541"},
            {"periodDescriptor": {"number": 1, "periodType": "REG"}, "timeInPeriod": "10:10", "typeDescKey": "penalty", "situationCode": "1551"},
            {"periodDescriptor": {"number": 1, "periodType": "REG"}, "timeInPeriod": "10:11", "typeDescKey": "faceoff", "situationCode": "1541"},
            {"periodDescriptor": {"number": 1, "periodType": "REG"}, "timeInPeriod": "10:20", "typeDescKey": "faceoff", "situationCode": "1551"},
            {"periodDescriptor": {"number": 1, "periodType": "REG"}, "timeInPeriod": "20:00", "typeDescKey": "period-end", "situationCode": "1551"},
        ]
        self.assertEqual(
            segments_for_team_advantage_abs(plays, "AWAY_ADV"),
            [(600, 610), (611, 620)],
        )

    def test_active_pp_extraction_deduplicates_identical_official_shift_intervals(self):
        from backend.nhl.scripts.fill_pp_toi_minutes_for_date import extract_pp_toi_minutes_from_db

        class Cursor:
            sql = ""
            params = ()

            def execute(self, sql, params):
                self.sql = sql
                self.params = params

            def fetchall(self):
                return [(8480995, 1.6)]

        cursor = Cursor()
        result = extract_pp_toi_minutes_from_db(cursor, 2026020111)
        self.assertEqual(result, {8480995: 1.6})
        self.assertIn("SELECT DISTINCT game_id, player_id, team_id, period, start_sec, end_sec", cursor.sql)
        self.assertEqual(cursor.params, (2026020111, 2026020111, 2026020111))

    def test_oct16_prepared_correction_sql_is_scoped_guarded_and_idempotent(self):
        from pathlib import Path

        root = Path(__file__).resolve().parents[3]
        forward = (root / "backend/nhl/sql/repairs/repair_nhl_oct16_official_source_conflicts.sql").read_text()
        rollback = (root / "backend/nhl/sql/repairs/rollback_nhl_oct16_official_source_conflicts.sql").read_text()
        forward_rows = forward.split("INSERT INTO oct16_source_candidates VALUES", 1)[1].split(";", 1)[0]
        rollback_rows = rollback.split("INSERT INTO oct16_source_candidates VALUES", 1)[1].split(";", 1)[0]

        self.assertEqual(forward_rows.count("('shot_attempts',"), 8)
        self.assertEqual(forward_rows.count("('pp_toi_minutes',"), 2)
        self.assertEqual(rollback_rows.count("('shot_attempts',"), 8)
        self.assertEqual(rollback_rows.count("('pp_toi_minutes',"), 2)
        self.assertIn("expected_shots_on_goal", forward)
        self.assertIn("nhl.team_external_ids", forward)
        self.assertIn("UNRESOLVED_PP_PROTECTED_VALUE_CHANGED", forward)
        self.assertIn("IS NOT DISTINCT FROM c.original_value", forward)
        self.assertIn("IS NOT DISTINCT FROM c.replacement_value", forward)
        self.assertIn("UPDATE_ROW_COUNT_EXCEEDED_SCOPE", forward)
        self.assertIn("ROLLBACK_UNEXPECTED_CURRENT_VALUE", rollback)
        self.assertIn("UNRESOLVED_PP_PROTECTED_VALUE_CHANGED", rollback)
    def test_postgame_stage_sync_does_not_retain_stale_pp_toi_on_unknown_snapshot(self):
        from backend.nhl.postgame_reconcile import staging_sync

        source = Path(staging_sync.__file__).read_text()
        self.assertIn("pp_toi_minutes=EXCLUDED.pp_toi_minutes", source)
        self.assertNotIn("pp_toi_minutes=COALESCE(EXCLUDED.pp_toi_minutes", source)


if __name__ == "__main__":
    unittest.main()
