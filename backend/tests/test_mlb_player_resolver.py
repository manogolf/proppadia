import unittest
from unittest.mock import patch

from backend.domains.mlb import player_resolver as resolver


class TestMlbPlayerResolver(unittest.TestCase):
    def test_resolve_prefers_player_id_and_skips_name_lookup(self):
        with patch.object(
            resolver,
            "_resolve_by_player_id",
            return_value={"player_id": 660271, "player_name": "Shohei Ohtani"},
        ) as mock_by_id, patch.object(resolver, "_resolve_by_name") as mock_by_name:
            out = resolver.resolve_player_candidate(player_id=660271, name="Shohei Ohtani", team_abbr="LAD")

        self.assertIsNotNone(out)
        self.assertEqual(out["player_id"], 660271)
        mock_by_id.assert_called_once_with(660271)
        mock_by_name.assert_not_called()

    def test_resolve_falls_back_to_name_when_player_id_misses(self):
        with patch.object(resolver, "_resolve_by_player_id", return_value=None) as mock_by_id, patch.object(
            resolver,
            "_resolve_by_name",
            return_value={"player_id": 660271, "player_name": "Shohei Ohtani"},
        ) as mock_by_name:
            out = resolver.resolve_player_candidate(player_id=660271, name="Shohei Ohtani", team_abbr="LAD")

        self.assertIsNotNone(out)
        self.assertEqual(out["player_id"], 660271)
        mock_by_id.assert_called_once_with(660271)
        mock_by_name.assert_called_once_with("Shohei Ohtani", "LAD")

    def test_resolve_returns_none_without_inputs(self):
        with patch.object(resolver, "_resolve_by_player_id") as mock_by_id, patch.object(
            resolver, "_resolve_by_name"
        ) as mock_by_name:
            out = resolver.resolve_player_candidate(player_id=None, name="   ", team_abbr=None)

        self.assertIsNone(out)
        mock_by_id.assert_not_called()
        mock_by_name.assert_not_called()


if __name__ == "__main__":
    unittest.main()
