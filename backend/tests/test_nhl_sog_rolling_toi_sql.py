from pathlib import Path
import unittest


SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "nhl"
    / "sql"
    / "fill_sog_toi_features_for_slate.sql"
)


class NHLRollingTOISQLContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = SQL_PATH.read_text(encoding="utf-8")
        cls.history = cls.sql.split("hist AS (", 1)[1].split("final AS (", 1)[0]

    def test_rolling_history_crosses_season_boundary_but_is_strictly_prior(self):
        self.assertRegex(self.history, r"g\.game_date\s*<\s*p\.slate_date")
        self.assertRegex(
            self.history,
            r"PARTITION BY h\.player_id\s+ORDER BY h\.game_date DESC, h\.game_id DESC",
        )
        self.assertNotRegex(self.history, r"\bseason\b")

    def test_only_rows_with_real_total_toi_enter_rolling_window(self):
        self.assertRegex(self.history, r"WHERE h\.toi_min IS NOT NULL")
        self.assertIn("AVG(CASE WHEN rn_desc <= 5  THEN toi_min END)", self.history)
        self.assertIn("AVG(CASE WHEN rn_desc <= 10 THEN toi_min END)", self.history)
        self.assertIn("AVG(CASE WHEN rn_desc <= 20 THEN toi_min END)", self.history)

    def test_update_matches_feature_rows_by_player_and_slate_date(self):
        update = self.sql.split("UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t", 1)[1]
        self.assertIn("t.game_date = p.slate_date", update)
        self.assertIn("f.player_id = t.player_id::bigint", update)
        self.assertNotIn("f.season", update)


if __name__ == "__main__":
    unittest.main()
