from __future__ import annotations

import unittest
import importlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from backend.nhl.points_attempts_source import (
    capture_stage_attempt_conflicts,
    compare_reconciled_attempts,
    derive_attempts_from_final_pbp,
    replace_staged_attempts_conflict_assignment,
)
from backend.nhl.scripts import build_nhl_points_architecture_bakeoff as bakeoff
from backend.nhl.scripts import export_nhl_points_hgb_features as hgb_exporter
with patch.dict(os.environ, {"SUPABASE_DB_URL": "postgresql://test", "SLATE_DATE": "2026-01-01"}):
    skater_importer = importlib.import_module("backend.nhl.scripts.seed_skater_logs_for_date")


def source_payloads():
    boxscore = {
        "id": 2026020001,
        "homeTeam": {"id": 1},
        "awayTeam": {"id": 2},
        "playerByGameStats": {
            "homeTeam": {"forwards": [
                {"playerId": 10, "sog": 1},
                {"playerId": 11, "sog": 0},
            ], "defense": []},
            "awayTeam": {"forwards": [
                {"playerId": 20, "sog": 1},
            ], "defense": []},
        },
    }
    def play(event_id, kind, player_id=None, team_id=1):
        details = {"eventOwnerTeamId": team_id}
        if player_id is not None:
            details["scoringPlayerId" if kind == "goal" else "shootingPlayerId"] = player_id
        return {"eventId": event_id, "typeDescKey": kind, "details": details}

    pbp = {
        "id": 2026020001,
        "gameState": "OFF",
        "homeTeam": {"id": 1},
        "awayTeam": {"id": 2},
        "plays": [
            play(1, "goal", 10),
            play(2, "missed-shot", 10),
            play(3, "blocked-shot", 10),
            play(4, "shot-on-goal", 20, 2),
        ],
    }
    return boxscore, pbp


class PointsAttemptsSourceTests(unittest.TestCase):
    def test_stage_conflict_capture_emits_exact_identity_and_values(self):
        from backend.nhl import points_attempts_source

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, sql, params):
                self.sql = sql
                self.params = params

            def fetchall(self):
                return [(2026020001, 8470001, 7, 99)]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def cursor(self):
                return Cursor()

        with patch.object(points_attempts_source.psycopg, "connect", return_value=Connection()):
            result = capture_stage_attempt_conflicts(
                "postgresql://fixture", slate_date="2026-10-15", stage="unit")
        self.assertEqual(result["conflict_count"], 1)
        self.assertEqual(result["conflicts"][0], {
            "game_id": 2026020001, "player_id": 8470001,
            "existing_shot_attempts": 7, "incoming_shot_attempts": 99,
            "status": "CONFLICT_PRESERVED_EXISTING_VALUE",
        })

    def test_final_pbp_preserves_positive_and_verified_zero_counts(self):
        boxscore, pbp = source_payloads()
        self.assertEqual(derive_attempts_from_final_pbp(boxscore, pbp, game_id=2026020001),
                         {10: 3, 11: 0, 20: 1})

    def test_reconciled_source_comparison_distinguishes_match_conflict_and_unknown(self):
        match = compare_reconciled_attempts(
            game_id=2025020001, player_id=8473419,
            raw_attempts=7, official_attempts=7, raw_row_present=True)
        zero = compare_reconciled_attempts(
            game_id=2025020001, player_id=8473422,
            raw_attempts=0, official_attempts=0, raw_row_present=True)
        conflict = compare_reconciled_attempts(
            game_id=2025020001, player_id=8473419,
            raw_attempts=4, official_attempts=7, raw_row_present=True)
        null_fill = compare_reconciled_attempts(
            game_id=2025020001, player_id=8473507,
            raw_attempts=None, official_attempts=3, raw_row_present=True)
        unknown = compare_reconciled_attempts(
            game_id=2025020001, player_id=8473507,
            raw_attempts=3, official_attempts=None, raw_row_present=True)
        self.assertEqual(match["status"], "MATCHES_RECONCILED_OFFICIAL_SOURCE")
        self.assertEqual(zero["status"], "MATCHES_RECONCILED_OFFICIAL_SOURCE")
        self.assertEqual(conflict["status"], "CONFLICT_PRESERVE_RAW_PENDING_CORRECTION")
        self.assertEqual((conflict["raw_attempts"], conflict["official_attempts"]), (4, 7))
        self.assertEqual(null_fill["status"], "NULL_FILL_CANDIDATE")
        self.assertEqual(unknown["status"], "OFFICIAL_SOURCE_UNKNOWN")

    def test_conflict_diagnostic_is_durable_and_run_bound(self):
        conflict = compare_reconciled_attempts(
            game_id=2025020001, player_id=8473419,
            raw_attempts=4, official_attempts=7, raw_row_present=True)
        conflict["pbp_response_sha256"] = "a" * 64
        summary = {
            "schema_version": "NHL_SHOT_ATTEMPTS_SOURCE_SUMMARY_V1",
            "conflicts": [conflict],
            "pbp_response_sha256_by_game": {"2025020001": "a" * 64},
        }
        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict(os.environ, {"NHL_DAILY_RUN_ID": "nhldaily_test_run"}):
                artifact = skater_importer._persist_attempt_diagnostic(
                    summary, diagnostic_root=Path(temporary))
                persisted = json.loads(Path(artifact["path"]).read_text())
                digest = __import__("hashlib").sha256(
                    Path(artifact["path"]).read_bytes()).hexdigest()
        self.assertEqual(persisted["parent_run_id"], "nhldaily_test_run")
        self.assertEqual(persisted["conflicts"][0]["game_id"], 2025020001)
        self.assertEqual(persisted["conflicts"][0]["raw_attempts"], 4)
        self.assertEqual(persisted["conflicts"][0]["official_attempts"], 7)
        self.assertEqual(artifact["sha256"], digest)

    def test_daily_source_summary_distinguishes_explicit_pbp_and_unknown(self):
        boxscore, _pbp = source_payloads()
        boxscore["playerByGameStats"]["homeTeam"]["forwards"][0]["shotAttempts"] = 0
        boxscore["playerByGameStats"]["homeTeam"]["forwards"][1]["shotAttempts"] = None
        counts = skater_importer.summarize_shot_attempt_sources(
            boxscore, {10: 3, 11: 0, 20: 2})
        self.assertEqual(counts, {
            "boxscore_explicit": 0, "validated_pbp": 3, "unknown": 0,
            "observed_positive": 2, "observed_zero": 1,
        })
        counts = skater_importer.summarize_shot_attempt_sources(boxscore, None)
        self.assertEqual(counts["unknown"], 2)
        self.assertEqual(counts["observed_zero"], 1)

    def test_reconciled_pbp_precedes_boxscore_and_unknown_stays_unknown(self):
        boxscore, _pbp = source_payloads()
        boxscore["playerByGameStats"]["homeTeam"]["forwards"][0]["shotAttempts"] = 99
        parsed = list(skater_importer._iter_skaters_from_box(boxscore, {10: 3, 11: 0, 20: 1}))
        self.assertEqual(parsed[0]["attempts"], 3)
        self.assertEqual(parsed[0]["attempts_source"], "VALIDATED_FINAL_PBP")
        summary = skater_importer.summarize_shot_attempt_sources(
            boxscore, {10: 3, 11: 0, 20: 1})
        self.assertEqual(summary["validated_pbp"], 3)
        unknown = list(skater_importer._iter_skaters_from_box(boxscore, None))
        self.assertEqual(unknown[1]["attempts"], None)
        self.assertEqual(unknown[1]["attempts_source"], "UNKNOWN")

    def test_partial_or_nonfinal_pbp_is_unknown_not_zero(self):
        boxscore, pbp = source_payloads()
        pbp["plays"] = pbp["plays"][:-1]
        with self.assertRaisesRegex(ValueError, "PLAYER_SOG_RECONCILIATION_FAILED"):
            derive_attempts_from_final_pbp(boxscore, pbp, game_id=2026020001)
        _boxscore, pbp = source_payloads()
        pbp["gameState"] = "LIVE"
        with self.assertRaisesRegex(ValueError, "GAME_NOT_FINAL"):
            derive_attempts_from_final_pbp(boxscore, pbp, game_id=2026020001)

    def test_hgb_history_keeps_missing_attempt_rows_out_of_rate_denominator(self):
        outcomes = pd.DataFrame([
            {"game_id": gid, "player_id": 10, "game_date": date,
             "canonical_season": 2026, "goals": 0, "assists": 0,
             "official_sog": 1, "outcome_source_timestamp_utc": f"{date}T23:00:00Z",
             "team_id": 1}
            for gid, date in ((2026020001, "2026-01-01"), (2026020002, "2026-01-05"))
        ])
        logs = pd.DataFrame([
            {"game_id": 2026020001, "player_id": 10, "team_id": 1,
             "home_team_id": 1, "away_team_id": 2, "is_home": True,
             "toi_minutes": 10.0, "pp_toi_minutes": 0.0,
             # Representative corrected source value in the retained 2025
             # recovery evidence (4 -> official-PBP 7).
             "shots_on_goal": 1, "shot_attempts": 7},
            {"game_id": 2026020002, "player_id": 10, "team_id": 1,
             "home_team_id": 1, "away_team_id": 2, "is_home": True,
             "toi_minutes": 10.0, "pp_toi_minutes": 0.0,
             "shots_on_goal": 1, "shot_attempts": None},
        ])
        history = hgb_exporter.normalize_history(logs, outcomes)
        self.assertTrue(pd.isna(history.loc[history.game_id.eq(2026020002), "shot_attempts"]).all())
        slate = hgb_exporter.normalize_slate(pd.DataFrame([{
            "game_id": 2026020003, "player_id": 10, "game_date": "2026-01-10",
            "game_start_utc": "2026-01-10T23:00:00Z", "is_home": 1,
            "home_team_id": 1, "away_team_id": 2,
        }]), 2026)
        feature = hgb_exporter.build_features(history, slate)
        self.assertAlmostEqual(float(feature.iloc[0].attempts_d10_per60), 42.0)

    def test_phoenix_export_keeps_source_attempts_nullable_until_feature_boundary(self):
        sql = (Path(__file__).resolve().parents[1] / "sql/export_points.sql").read_text()
        self.assertIn("l.shot_attempts::float AS shot_attempts", sql)
        self.assertNotIn("COALESCE(l.shot_attempts, 0)", sql)
        self.assertIn("shot_attempts IS NOT NULL", sql)
        self.assertIn("Frozen model input default", sql)

    def test_stage_refresh_replaces_stale_attempts_with_unknown(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE stage (player_id INTEGER PRIMARY KEY, shot_attempts INTEGER)")
        connection.execute("INSERT INTO stage VALUES (1, 14)")
        connection.execute(
            "INSERT INTO stage VALUES (1, NULL) ON CONFLICT(player_id) DO UPDATE SET "
            + replace_staged_attempts_conflict_assignment(),
        )
        self.assertIsNone(connection.execute(
            "SELECT shot_attempts FROM stage WHERE player_id=1"
        ).fetchone()[0])

        importer = skater_importer

        class Cursor:
            def __init__(self):
                self.executed = []

            def execute(self, sql):
                self.executed.append(sql)

            def fetchall(self):
                return [("player_id",), ("game_id",), ("game_date",), ("team_id",),
                        ("shots_on_goal",), ("shot_attempts",), ("toi_minutes",),
                        ("pp_toi_minutes",)]

            def executemany(self, sql, rows):
                self.executed.append(sql)
                self.rows = list(rows)

        cursor = Cursor()
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        importer.upsert_rows(connection, [(1, 2026020001, "2026-01-01", 1, 1, None,
                                          10.0, None, None)])
        stage_sql = cursor.executed[-1]
        self.assertIn(replace_staged_attempts_conflict_assignment(), stage_sql)
        self.assertNotIn("COALESCE(EXCLUDED.shot_attempts", stage_sql)

    def test_raw_promoters_delegate_protected_null_fills_to_database_function(self):
        cli_source = (Path(__file__).resolve().parents[1] / "cli.py").read_text()
        postgame_source = (Path(__file__).resolve().parents[1] / "scripts" /
                           "run_nhl_postgame_reconciliation.py").read_text()
        self.assertNotIn("shot_attempts = EXCLUDED.shot_attempts", cli_source)
        self.assertNotIn("shot_attempts=excluded.shot_attempts", postgame_source)
        self.assertNotIn("pp_toi_minutes = EXCLUDED.pp_toi_minutes", cli_source)
        self.assertNotIn("pp_toi_minutes=excluded.pp_toi_minutes", postgame_source)
        self.assertIn("promote_skater_game_log_protected_fields", cli_source)
        self.assertIn("promote_skater_game_log_protected_fields", postgame_source)
        self.assertIn("NHL_SHOT_ATTEMPTS_PROMOTION_SUMMARY_V1", cli_source)

    def test_hgb_training_builder_excludes_missing_attempts_but_keeps_zero(self):
        rows = []
        for idx, (date, attempts) in enumerate((
            ("2023-10-01", 4), ("2023-10-02", None),
            ("2023-10-03", 0), ("2023-10-04", 1),
        ), start=1):
            rows.append({
                "season": 2023, "game_date": date,
                "start_time_utc": f"{date}T23:00:00Z",
                "player_id": 10, "game_id": 2023020000 + idx,
                "team_id": 1, "is_home": 1,
                "realized_goals": 0, "realized_assists": 0,
                "realized_points": 0, "toi_minutes": 10.0,
                "pp_toi_minutes": 0.0, "shots_on_goal": 1,
                "shot_attempts": attempts,
            })
        frame = bakeoff.build_frame(pd.DataFrame(rows))
        contract = bakeoff.ARMS[0]
        third = frame.loc[frame.game_id.eq(2023020003) & frame.history_contract.eq(contract)].iloc[0]
        fourth = frame.loc[frame.game_id.eq(2023020004) & frame.history_contract.eq(contract)].iloc[0]
        self.assertAlmostEqual(float(third.attempts_d10_per60), 24.0)
        self.assertAlmostEqual(float(fourth.attempts_d10_per60), 12.0)
        self.assertTrue(pd.isna(frame.loc[frame.game_id.eq(2023020001) & frame.history_contract.eq(contract), "attempts_d10_per60"].iloc[0]))

        sql = (Path(__file__).resolve().parents[1] / "sql/export_points_architecture_bakeoff.sql").read_text()
        self.assertIn("l.shot_attempts::float AS shot_attempts", sql)
        self.assertNotIn("COALESCE(l.shot_attempts, 0)", sql)


if __name__ == "__main__":
    unittest.main()
