import unittest

from backend.domains.mlb.player_directory import _decorate as dir_decorate
from backend.domains.mlb.player_resolver import _decorate as resolver_decorate


class TestMlbTeamNormalization(unittest.TestCase):
    def test_resolver_decorate_numeric_team_to_abbr(self):
        row = {"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}
        out = resolver_decorate(row, source="player_ids")
        self.assertIsNotNone(out)
        self.assertEqual(out["team_abbr"], "LAD")
        self.assertEqual(out["team_id"], 119)

    def test_resolver_decorate_abbr_team(self):
        row = {"player_id": "592450", "player_name": "Aaron Judge", "team": "NYY"}
        out = resolver_decorate(row, source="player_ids")
        self.assertIsNotNone(out)
        self.assertEqual(out["team_abbr"], "NYY")
        self.assertEqual(out["team_id"], 147)

    def test_directory_decorate_numeric_team_to_abbr(self):
        row = {"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}
        out = dir_decorate(row, source="player_ids")
        self.assertIsNotNone(out)
        self.assertEqual(out["team_abbr"], "LAD")
        self.assertEqual(out["team_id"], 119)

    def test_directory_decorate_abbr_team(self):
        row = {"player_id": "592450", "player_name": "Aaron Judge", "team": "NYY"}
        out = dir_decorate(row, source="player_ids")
        self.assertIsNotNone(out)
        self.assertEqual(out["team_abbr"], "NYY")
        self.assertEqual(out["team_id"], 147)

    def test_resolver_decorate_alias_abbr_normalization(self):
        row = {"player_id": "111111", "player_name": "Test Player", "team": "AZ"}
        out = resolver_decorate(row, source="player_ids")
        self.assertIsNotNone(out)
        self.assertEqual(out["team_abbr"], "ARI")
        self.assertEqual(out["team_id"], 109)

    def test_directory_decorate_athletics_alias_normalization(self):
        row = {"player_id": "222222", "player_name": "Test Player", "team": "ATH"}
        out = dir_decorate(row, source="player_ids")
        self.assertIsNotNone(out)
        self.assertEqual(out["team_abbr"], "OAK")
        self.assertEqual(out["team_id"], 133)


if __name__ == "__main__":
    unittest.main()
