from __future__ import annotations

import unittest
import importlib
import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from backend.nhl.points_attempts_source import (
    derive_attempts_from_final_pbp,
    preserve_existing_attempts_conflict_assignment,
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
    def test_final_pbp_preserves_positive_and_verified_zero_counts(self):
        boxscore, pbp = source_payloads()
        self.assertEqual(derive_attempts_from_final_pbp(boxscore, pbp, game_id=2026020001),
                         {10: 3, 11: 0, 20: 1})

    def test_daily_source_summary_distinguishes_explicit_pbp_and_unknown(self):
        boxscore, _pbp = source_payloads()
        boxscore["playerByGameStats"]["homeTeam"]["forwards"][0]["shotAttempts"] = 0
        boxscore["playerByGameStats"]["homeTeam"]["forwards"][1]["shotAttempts"] = None
        counts = skater_importer.summarize_shot_attempt_sources(
            boxscore, {10: 3, 11: 0, 20: 2})
        self.assertEqual(counts, {
            "boxscore_explicit": 1, "validated_pbp": 2, "unknown": 0,
            "observed_positive": 1, "observed_zero": 2,
        })
        counts = skater_importer.summarize_shot_attempt_sources(boxscore, None)
        self.assertEqual(counts["unknown"], 2)
        self.assertEqual(counts["observed_zero"], 1)

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

    def test_raw_promotion_conflict_preserves_populated_values_and_fills_nulls(self):
        assignment = preserve_existing_attempts_conflict_assignment("raw")
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE raw (player_id INTEGER PRIMARY KEY, shot_attempts INTEGER)")

        def promote(player_id, old_value, incoming):
            connection.execute("INSERT INTO raw VALUES (?, ?)", (player_id, old_value))
            connection.execute(
                "INSERT INTO raw VALUES (?, ?) ON CONFLICT(player_id) DO UPDATE SET "
                + assignment,
                (player_id, incoming),
            )
            actual = connection.execute(
                "SELECT shot_attempts FROM raw WHERE player_id = ?", (player_id,)
            ).fetchone()[0]
            self.assertEqual(actual, old_value if old_value is not None else incoming)

        promote(1, 7, None)   # unavailable rerun cannot clear a verified count
        promote(2, 0, 8)      # a verified zero cannot be replaced
        promote(3, 12, 99)    # an unverified different count cannot replace it
        promote(4, None, 0)   # a verified zero can fill an unknown raw value
        promote(5, None, 6)   # a verified positive can fill an unknown raw value

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

    def test_both_raw_promoters_use_preserve_existing_assignment(self):
        expected = preserve_existing_attempts_conflict_assignment()
        cli_source = (Path(__file__).resolve().parents[1] / "cli.py").read_text()
        postgame_source = (Path(__file__).resolve().parents[1] / "scripts" /
                           "run_nhl_postgame_reconciliation.py").read_text()
        self.assertIn("{preserve_existing_attempts_conflict_assignment()}", cli_source)
        self.assertIn("{preserve_existing_attempts_conflict_assignment()}", postgame_source)
        self.assertNotIn("shot_attempts  = COALESCE(EXCLUDED.shot_attempts", cli_source)
        self.assertNotIn("shot_attempts=coalesce(excluded.shot_attempts", postgame_source)
        self.assertIn("COALESCE(nhl.skater_game_logs_raw.shot_attempts, EXCLUDED.shot_attempts)", expected)

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
