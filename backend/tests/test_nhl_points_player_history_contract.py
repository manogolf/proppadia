from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import unittest


SQL_PATH = Path(__file__).resolve().parents[1] / "nhl" / "sql" / "export_points.sql"


def strict_prior_player_window(rows, *, player_id: int, target_date: date, limit: int):
    eligible = [
        row for row in rows
        if row["player_id"] == player_id
        and row["game_type"] == 2
        and row["game_date"] < target_date
    ]
    return sorted(
        eligible,
        key=lambda row: (row["game_date"], row["start_time"], row["game_id"]),
        reverse=True,
    )[:limit]


def season_to_date(rows, *, player_id: int, season: int, target_date: date):
    return [
        row for row in rows
        if row["player_id"] == player_id
        and row["season"] == season
        and row["game_type"] == 2
        and row["game_date"] < target_date
    ]


def history_fixture():
    rows = []
    for offset in range(10):
        rows.append({
            "player_id": 77,
            "game_id": 2025021000 + offset,
            "season": 2025,
            "game_type": 2,
            "game_date": date(2025, 4, 1) + timedelta(days=offset),
            "start_time": datetime(2025, 4, 1) + timedelta(days=offset),
            "sog": offset + 1,
        })
    for offset in range(3):
        rows.append({
            "player_id": 77,
            "game_id": 2026020001 + offset,
            "season": 2026,
            "game_type": 2,
            "game_date": date(2026, 10, 1) + timedelta(days=offset),
            "start_time": datetime(2026, 10, 1) + timedelta(days=offset),
            "sog": 20 + offset,
        })
    rows.extend([
        {"player_id": 77, "game_id": 2026010001, "season": 2026,
         "game_type": 1, "game_date": date(2026, 9, 30),
         "start_time": datetime(2026, 9, 30), "sog": 99},
        {"player_id": 77, "game_id": 2026020004, "season": 2026,
         "game_type": 2, "game_date": date(2026, 10, 4),
         "start_time": datetime(2026, 10, 4), "sog": 100},
        {"player_id": 77, "game_id": 2026020005, "season": 2026,
         "game_type": 2, "game_date": date(2026, 10, 5),
         "start_time": datetime(2026, 10, 5), "sog": 101},
    ])
    return rows


class PointsPlayerHistoryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = SQL_PATH.read_text(encoding="utf-8")
        cls.player_query = cls.sql.split("player_window_rows AS (", 1)[1].split(
            "player_window_features AS (", 1
        )[0]
        cls.season_query = cls.sql.split("player_season_to_date AS (", 1)[1].split(
            "-- Team-side contract", 1
        )[0]
        cls.team_query = cls.sql.split("team_logs AS (", 1)[1].split("  )\n\n  SELECT", 1)[0]

    def test_sql_uses_strict_prior_regular_season_latest_ten_without_date_cutoff(self):
        self.assertIn("JOIN LATERAL", self.player_query)
        self.assertRegex(self.player_query, r"g2\.game_date\s*<\s*b\.game_date")
        self.assertIn("substring(g2.game_id::text, 5, 2) = '02'", self.player_query)
        self.assertIn("LIMIT 10", self.player_query)
        self.assertIn("ORDER BY g2.game_date DESC, g2.start_time_utc DESC NULLS LAST", self.player_query)
        self.assertNotIn("INTERVAL '120 days'", self.player_query)
        self.assertNotRegex(self.player_query, r"PARTITION BY[^)]*season")

    def test_last_five_and_last_ten_take_prior_season_games(self):
        rows = history_fixture()
        target = date(2026, 10, 4)
        last5 = strict_prior_player_window(rows, player_id=77, target_date=target, limit=5)
        last10 = strict_prior_player_window(rows, player_id=77, target_date=target, limit=10)
        self.assertEqual([r["game_id"] for r in last5], [2026020003, 2026020002, 2026020001, 2025021009, 2025021008])
        self.assertEqual([r["game_id"] for r in last10[:3]], [2026020003, 2026020002, 2026020001])
        self.assertEqual(len([r for r in last10 if r["season"] == 2025]), 7)

    def test_old_games_naturally_displace_after_ten_current_season_games(self):
        rows = history_fixture()
        for offset in range(10):
            rows.append({
                "player_id": 77,
                "game_id": 2026020100 + offset,
                "season": 2026,
                "game_type": 2,
                "game_date": date(2026, 10, 10) + timedelta(days=offset),
                "start_time": datetime(2026, 10, 10) + timedelta(days=offset),
                "sog": 1,
            })
        window = strict_prior_player_window(
            rows, player_id=77, target_date=date(2026, 10, 20), limit=10)
        self.assertEqual(len(window), 10)
        self.assertTrue(all(row["season"] == 2026 for row in window))

    def test_strict_prior_and_preseason_exclusions(self):
        rows = history_fixture()
        window = strict_prior_player_window(
            rows, player_id=77, target_date=date(2026, 10, 4), limit=10)
        ids = {row["game_id"] for row in window}
        self.assertNotIn(2026010001, ids)
        self.assertNotIn(2026020004, ids)  # same target date
        self.assertNotIn(2026020005, ids)  # future date
        self.assertTrue(all(row["game_date"] < date(2026, 10, 4) for row in window))

    def test_season_to_date_uses_canonical_target_season_and_strict_prior(self):
        rows = history_fixture()
        current = season_to_date(rows, player_id=77, season=2026, target_date=date(2026, 10, 4))
        self.assertEqual([row["game_id"] for row in current], [2026020001, 2026020002, 2026020003])
        self.assertRegex(self.season_query, r"gh\.season::int\s*=\s*b\.canonical_season")
        self.assertRegex(self.season_query, r"gh\.game_date\s*<\s*b\.game_date")
        self.assertIn("substring(gh.game_id::text, 5, 2) = '02'", self.season_query)

    def test_team_history_contract_remains_120_day_bounded(self):
        self.assertIn("g.game_date >= (DATE :'slate_date' - INTERVAL '120 days')", self.team_query)
        self.assertIn("l.team_id IN (SELECT team_id FROM slate_teams)", self.team_query)
        self.assertIn("team_logs", self.sql)
        self.assertIn("-- Mixed contract: player numerator is cross-season last-10", self.sql)

    def test_output_schema_order_and_grain_remain_frozen(self):
        projection = self.sql.split("  SELECT\n    b.player_id,", 1)[1].split("  FROM base b", 1)[0]
        expected = [
            "is_home", "d5_sog_per60", "d10_sog_per60", "attempts_d10_per60",
            "team_d10_sf_per_game", "last10_team_sog_share",
            "num_shotwasongoal_last5", "num_shotwasongoal_last10",
            "num_shotwasongoal_season_to_date", "num_event_shot_last5",
            "num_event_shot_last10", "num_event_shot_season_to_date",
            "team_num_event_shot_for_last10", "team_num_shotwasongoal_for_last10",
            "hot_last5_flag",
        ]
        positions = [projection.index(f"AS {name}") for name in expected]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("ORDER BY b.player_id, b.game_id", self.sql)
        self.assertIn("PARTITION BY b.player_id, b.game_id", self.player_query)

    def test_versioned_feature_contract_is_bound_to_scoring_receipts(self):
        from backend.nhl.points_shadow.core import verify_feature_contract_identity

        contract = verify_feature_contract_identity()
        self.assertEqual(contract["version"], "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2")
        self.assertEqual(len(contract["sha256"]), 64)
        wrapper = (SQL_PATH.parents[1] / "scripts" / "score_nhl_points_with_lineage.py").read_text()
        self.assertIn('"feature_contract_version": feature_contract["version"]', wrapper)
        self.assertIn('"feature_contract_sha256": feature_contract["sha256"]', wrapper)
        self.assertIn('("feature_construction", Path(__file__).resolve().parents[1] / "sql" / "export_points.sql")', wrapper)


if __name__ == "__main__":
    unittest.main()
