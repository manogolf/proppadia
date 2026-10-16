from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.points_attempts_source import derive_attempts_from_final_pbp
from backend.nhl.scripts import build_nhl_points_architecture_bakeoff as bakeoff
from backend.nhl.scripts import export_nhl_points_hgb_features as hgb_exporter


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
             "shots_on_goal": 1, "shot_attempts": 4},
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
        self.assertAlmostEqual(float(feature.iloc[0].attempts_d10_per60), 24.0)

    def test_phoenix_export_keeps_source_attempts_nullable_until_feature_boundary(self):
        sql = (Path(__file__).resolve().parents[1] / "sql/export_points.sql").read_text()
        self.assertIn("l.shot_attempts::float AS shot_attempts", sql)
        self.assertNotIn("COALESCE(l.shot_attempts, 0)", sql)
        self.assertIn("shot_attempts IS NOT NULL", sql)
        self.assertIn("Frozen model input default", sql)

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
