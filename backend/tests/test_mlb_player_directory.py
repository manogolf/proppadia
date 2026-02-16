import unittest
from unittest.mock import patch

from backend.domains.mlb import player_directory as directory


class TestMlbPlayerDirectory(unittest.TestCase):
    @patch("backend.domains.mlb.player_directory.fetch_player_profile_rows")
    @patch("backend.domains.mlb.player_directory.lookup_player")
    def test_player_profile_shape_with_lookup_hit(self, mock_lookup, mock_rows):
        mock_lookup.return_value = {
            "player_id": 660271,
            "player_name": "Shohei Ohtani",
            "team_abbr": "LAD",
            "team_id": 119,
        }
        mock_rows.return_value = {
            "streaks": [{"prop_type": "hits"}],
            "recent_props": [{"prop_type": "hits"}],
            "stat_derived": [{"prop_type": "hits"}],
            "training_summary": [{"prop_type": "hits", "count": 3}],
        }

        out = directory.player_profile(660271)

        self.assertEqual((out.get("player_info") or {}).get("player_id"), 660271)
        self.assertEqual((out.get("player_info") or {}).get("team"), "LAD")
        self.assertIn("recent_props", out)
        self.assertIn("training_summary", out)
        self.assertEqual(out.get("season_stats"), {})
        self.assertEqual(out.get("career_stats"), {})

    @patch("backend.domains.mlb.player_directory.fetch_player_profile_rows")
    @patch("backend.domains.mlb.player_directory.lookup_player")
    def test_player_profile_falls_back_when_lookup_missing(self, mock_lookup, mock_rows):
        mock_lookup.return_value = None
        mock_rows.return_value = {
            "streaks": [],
            "recent_props": [],
            "stat_derived": [],
            "training_summary": [],
        }

        out = directory.player_profile(12345)

        self.assertEqual((out.get("player_info") or {}).get("player_id"), 12345)
        self.assertIsNone((out.get("player_info") or {}).get("player_name"))
        self.assertIsNone((out.get("player_info") or {}).get("team"))


if __name__ == "__main__":
    unittest.main()
