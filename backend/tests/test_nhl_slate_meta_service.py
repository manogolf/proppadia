import unittest
from unittest.mock import patch

from backend.app.services.nhl.slate_meta_service import _slate_meta_cache, get_nhl_slate_meta


class TestNhlSlateMetaService(unittest.TestCase):
    def setUp(self):
        _slate_meta_cache.clear()

    @patch("backend.app.services.nhl.slate_meta_service.fetch_saves")
    @patch("backend.app.services.nhl.slate_meta_service.fetch_sog")
    @patch("backend.app.services.nhl.slate_meta_service.fetch_props_today")
    @patch("backend.app.services.nhl.slate_meta_service.fetch_games_today")
    def test_get_nhl_slate_meta_shape(
        self,
        mock_games,
        mock_props,
        mock_sog,
        mock_saves,
    ):
        mock_games.return_value = {"ok": True, "count": 2, "rows": [{}, {}]}
        mock_props.return_value = {"ok": True, "count": 3, "rows": [{}, {}, {}]}
        mock_sog.return_value = [{}, {}]
        mock_saves.return_value = [{}]

        out = get_nhl_slate_meta(date="2025-11-20", limit=100)
        self.assertTrue(out["ok"])
        self.assertEqual(out["source"], "upstream")
        self.assertIn("components", out)
        self.assertEqual(out["components"]["games_today"]["count"], 2)
        self.assertEqual(out["components"]["props_today"]["count"], 3)
        self.assertEqual(out["components"]["sog"]["count"], 2)
        self.assertEqual(out["components"]["saves"]["count"], 1)
        self.assertTrue(out["all_ok"])

    @patch("backend.app.services.nhl.slate_meta_service.fetch_saves")
    @patch("backend.app.services.nhl.slate_meta_service.fetch_sog")
    @patch("backend.app.services.nhl.slate_meta_service.fetch_props_today")
    @patch("backend.app.services.nhl.slate_meta_service.fetch_games_today")
    def test_get_nhl_slate_meta_cache(
        self,
        mock_games,
        mock_props,
        mock_sog,
        mock_saves,
    ):
        mock_games.return_value = {"ok": True, "count": 1, "rows": [{}]}
        mock_props.return_value = {"ok": True, "count": 1, "rows": [{}]}
        mock_sog.return_value = [{}]
        mock_saves.return_value = [{}]

        first = get_nhl_slate_meta(date="2025-11-20", limit=100)
        second = get_nhl_slate_meta(date="2025-11-20", limit=100)
        self.assertEqual(first["source"], "upstream")
        self.assertEqual(second["source"], "cache")
        self.assertFalse(second["stale"])
        self.assertEqual(mock_games.call_count, 1)


if __name__ == "__main__":
    unittest.main()

