import unittest
from unittest.mock import patch

from backend.app.services.mlb import player_service as svc


class TestMlbPlayerService(unittest.TestCase):
    @patch("backend.app.services.mlb.player_service.resolve_player_candidate")
    def test_resolve_player_delegates_to_resolver(self, mock_resolve):
        mock_resolve.return_value = {"player_id": 660271}
        out = svc.resolve_player(player_id=660271, name="Shohei Ohtani", team_abbr="LAD")
        self.assertEqual(out, {"player_id": 660271})
        mock_resolve.assert_called_once_with(player_id=660271, name="Shohei Ohtani", team_abbr="LAD")

    @patch("backend.app.services.mlb.player_service.lookup_player_directory")
    def test_lookup_player_delegates(self, mock_lookup):
        mock_lookup.return_value = {"player_id": 660271}
        out = svc.lookup_player(player_id=660271)
        self.assertEqual(out, {"player_id": 660271})
        mock_lookup.assert_called_once_with(player_id=660271)

    @patch("backend.app.services.mlb.player_service.search_players_directory")
    def test_search_players_delegates(self, mock_search):
        mock_search.return_value = [{"player_id": 660271}]
        out = svc.search_players(q="Ohtani", limit=5)
        self.assertEqual(out, [{"player_id": 660271}])
        mock_search.assert_called_once_with(q="Ohtani", limit=5)

    @patch("backend.app.services.mlb.player_service.list_players_directory")
    def test_list_players_delegates(self, mock_list):
        mock_list.return_value = [{"player_id": 660271}]
        out = svc.list_players(limit=25)
        self.assertEqual(out, [{"player_id": 660271}])
        mock_list.assert_called_once_with(limit=25)

    @patch("backend.app.services.mlb.player_service.list_players_mlb_directory")
    def test_list_players_mlb_delegates(self, mock_list_mlb):
        mock_list_mlb.return_value = [{"player_id": 660271}]
        out = svc.list_players_mlb(limit=25)
        self.assertEqual(out, [{"player_id": 660271}])
        mock_list_mlb.assert_called_once_with(limit=25)

    @patch("backend.app.services.mlb.player_service.player_profile_directory")
    def test_player_profile_delegates(self, mock_profile):
        mock_profile.return_value = {"player_info": {"player_id": 660271}}
        out = svc.player_profile(player_id=660271)
        self.assertEqual(out, {"player_info": {"player_id": 660271}})
        mock_profile.assert_called_once_with(player_id=660271)


if __name__ == "__main__":
    unittest.main()
