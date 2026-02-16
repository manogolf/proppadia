import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.api_server import app


class TestMlbPlayersEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("backend.app.routers.mlb.list_players_mlb")
    def test_mlb_players_list_ok(self, mock_list):
        mock_list.return_value = [
            {
                "player_id": 660271,
                "player_name": "Shohei Ohtani",
                "team": "LAD",
                "last_prop_date": "2025-08-15",
            }
        ]
        resp = self.client.get("/api/mlb/players?limit=5")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(len(body), 1)
        self.assertEqual(body[0].get("player_name"), "Shohei Ohtani")
        mock_list.assert_called_once_with(limit=5)

    @patch("backend.app.routers.mlb.list_players")
    @patch("backend.app.routers.mlb.list_players_mlb")
    def test_players_alias_uses_mlb_scoped_list(self, mock_list_mlb, mock_list_legacy):
        mock_list_mlb.return_value = [
            {
                "player_id": 660271,
                "player_name": "Shohei Ohtani",
                "team": "LAD",
                "last_prop_date": "2025-08-15",
            }
        ]
        resp = self.client.get("/api/players?limit=5")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(len(body), 1)
        self.assertEqual(body[0].get("player_name"), "Shohei Ohtani")
        mock_list_mlb.assert_called_once_with(limit=5)
        mock_list_legacy.assert_not_called()

    @patch("backend.app.routers.mlb.search_players")
    def test_players_search_ok(self, mock_search):
        mock_search.return_value = [
            {
                "player_id": 660271,
                "player_name": "Shohei Ohtani",
                "team_abbr": "LAD",
                "team_id": 119,
                "source": "model_training_props",
            }
        ]
        resp = self.client.get("/api/players/search?q=Ohtani&limit=5")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("count"), 1)
        self.assertEqual(body.get("rows", [{}])[0].get("player_id"), 660271)
        mock_search.assert_called_once_with(q="Ohtani", limit=5)

    @patch("backend.app.routers.mlb.lookup_player")
    def test_players_lookup_found(self, mock_lookup):
        mock_lookup.return_value = {
            "player_id": 660271,
            "player_name": "Shohei Ohtani",
            "team_abbr": "LAD",
            "team_id": 119,
            "source": "player_ids",
        }
        resp = self.client.get("/api/players/lookup?player_id=660271")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("found"))
        self.assertEqual(body.get("player_id"), 660271)
        self.assertEqual(body.get("team_abbr"), "LAD")
        mock_lookup.assert_called_once_with(player_id=660271)

    @patch("backend.app.routers.mlb.lookup_player")
    def test_players_lookup_not_found(self, mock_lookup):
        mock_lookup.return_value = None
        resp = self.client.get("/api/players/lookup?player_id=999999")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertFalse(body.get("found"))
        self.assertEqual(body.get("player_id"), 999999)
        mock_lookup.assert_called_once_with(player_id=999999)

    @patch("backend.app.routers.mlb.player_profile")
    def test_player_profile_ok(self, mock_profile):
        mock_profile.return_value = {
            "player_info": {
                "player_id": 660271,
                "player_name": "Shohei Ohtani",
                "team": "LAD",
                "team_id": 119,
            },
            "streaks": [],
            "recent_props": [],
            "stat_derived": [],
            "training_summary": [],
            "season_stats": {},
            "career_stats": {},
        }
        resp = self.client.get("/api/player-profile/660271")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual((body.get("player_info") or {}).get("player_name"), "Shohei Ohtani")
        self.assertIn("recent_props", body)
        self.assertIn("training_summary", body)
        mock_profile.assert_called_once_with(player_id=660271)

    def test_players_resolve_requires_identifier(self):
        resp = self.client.get("/api/players/resolve")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Provide player_id or name/player_name", str(resp.json().get("detail")))

    @patch("backend.app.routers.mlb.resolve_player")
    def test_players_resolve_found(self, mock_resolve):
        mock_resolve.return_value = {
            "player_id": 660271,
            "player_name": "Shohei Ohtani",
            "team_abbr": "LAD",
            "team_id": 119,
            "source": "player_ids",
            "matched_on": "player_id",
        }
        resp = self.client.get("/api/players/resolve?player_id=660271")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("found"))
        self.assertEqual(body.get("player_id"), 660271)
        self.assertEqual(body.get("team_abbr"), "LAD")
        mock_resolve.assert_called_once_with(player_id=660271, name=None, team_abbr=None)

    @patch("backend.app.routers.mlb.resolve_player")
    def test_players_resolve_accepts_player_name_alias(self, mock_resolve):
        mock_resolve.return_value = {
            "player_id": 660271,
            "player_name": "Shohei Ohtani",
            "team_abbr": "LAD",
            "team_id": 119,
            "source": "player_ids",
            "matched_on": "fuzzy_name",
        }
        resp = self.client.get("/api/players/resolve?player_name=Shohei+Ohtani&team_abbr=LAD")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("found"))
        self.assertEqual(body.get("player_id"), 660271)
        mock_resolve.assert_called_once_with(player_id=None, name="Shohei Ohtani", team_abbr="LAD")

    @patch("backend.app.routers.mlb.resolve_player")
    def test_players_resolve_prefers_name_over_player_name_alias(self, mock_resolve):
        mock_resolve.return_value = {
            "player_id": 660271,
            "player_name": "Shohei Ohtani",
            "team_abbr": "LAD",
            "team_id": 119,
            "source": "player_ids",
            "matched_on": "fuzzy_name",
        }
        resp = self.client.get(
            "/api/players/resolve?name=Primary+Name&player_name=Alias+Name&team_abbr=LAD"
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("found"))
        mock_resolve.assert_called_once_with(player_id=None, name="Primary Name", team_abbr="LAD")

    @patch("backend.app.routers.mlb.resolve_player")
    def test_players_resolve_not_found(self, mock_resolve):
        mock_resolve.return_value = None
        resp = self.client.get("/api/players/resolve?name=Unknown+Player&team_abbr=LAD")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertFalse(body.get("found"))
        self.assertEqual(body.get("player_name"), "Unknown Player")
        self.assertEqual(body.get("team_abbr"), "LAD")
        mock_resolve.assert_called_once_with(player_id=None, name="Unknown Player", team_abbr="LAD")

    @patch("backend.app.routers.mlb.resolve_player")
    def test_players_resolve_not_found_normalizes_team_alias(self, mock_resolve):
        mock_resolve.return_value = None
        resp = self.client.get("/api/players/resolve?name=Unknown+Player&team_abbr=az")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertFalse(body.get("found"))
        self.assertEqual(body.get("team_abbr"), "ARI")
        mock_resolve.assert_called_once_with(player_id=None, name="Unknown Player", team_abbr="az")

    @patch("backend.app.routers.mlb.resolve_player")
    def test_players_resolve_not_found_normalizes_ath_alias(self, mock_resolve):
        mock_resolve.return_value = None
        resp = self.client.get("/api/players/resolve?name=Unknown+Player&team_abbr=ATH")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertFalse(body.get("found"))
        self.assertEqual(body.get("team_abbr"), "OAK")
        mock_resolve.assert_called_once_with(player_id=None, name="Unknown Player", team_abbr="ATH")

    @patch("backend.app.routers.mlb.resolve_player", side_effect=RuntimeError("resolver unavailable"))
    def test_players_resolve_runtime_error_maps_503(self, _mock_resolve):
        resp = self.client.get("/api/players/resolve?name=Shohei+Ohtani")
        self.assertEqual(resp.status_code, 503)
        self.assertIn("resolver unavailable", str(resp.json().get("detail")))

    @patch("backend.app.routers.mlb.resolve_player", side_effect=Exception("boom"))
    def test_players_resolve_unexpected_error_maps_500(self, _mock_resolve):
        resp = self.client.get("/api/players/resolve?name=Shohei+Ohtani")
        self.assertEqual(resp.status_code, 500)
        self.assertIn("Exception: boom", str(resp.json().get("detail")))

    @patch("backend.app.routers.mlb.search_players", side_effect=RuntimeError("db unavailable"))
    def test_players_search_runtime_error_maps_503(self, _mock_search):
        resp = self.client.get("/api/players/search?q=Ohtani&limit=5")
        self.assertEqual(resp.status_code, 503)
        self.assertIn("db unavailable", str(resp.json().get("detail")))

    @patch("backend.app.routers.mlb.lookup_player", side_effect=Exception("boom"))
    def test_players_lookup_unexpected_error_maps_500(self, _mock_lookup):
        resp = self.client.get("/api/players/lookup?player_id=660271")
        self.assertEqual(resp.status_code, 500)
        self.assertIn("Exception: boom", str(resp.json().get("detail")))

    @patch("backend.app.routers.mlb.lookup_player", side_effect=RuntimeError("lookup unavailable"))
    def test_players_lookup_runtime_error_maps_503(self, _mock_lookup):
        resp = self.client.get("/api/players/lookup?player_id=660271")
        self.assertEqual(resp.status_code, 503)
        self.assertIn("lookup unavailable", str(resp.json().get("detail")))

    @patch("backend.app.routers.mlb.player_profile", side_effect=RuntimeError("profile cache down"))
    def test_player_profile_runtime_error_maps_503(self, _mock_profile):
        resp = self.client.get("/api/player-profile/660271")
        self.assertEqual(resp.status_code, 503)
        self.assertIn("profile cache down", str(resp.json().get("detail")))


if __name__ == "__main__":
    unittest.main()
