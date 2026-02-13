import unittest
from unittest.mock import patch

from backend.app.services.mlb.market_odds_service import (
    _snapshot_cache,
    _american_to_implied_probability,
    _extract_candidate_outcomes,
    fetch_mlb_market_odds,
    get_market_cache_status,
    refresh_market_cache_for_date,
    get_supported_market_map,
)


class TestMlbMarketOddsService(unittest.TestCase):
    def setUp(self):
        _snapshot_cache.clear()

    def test_american_to_implied_probability(self):
        self.assertAlmostEqual(_american_to_implied_probability(-110), 110 / 210, places=6)
        self.assertAlmostEqual(_american_to_implied_probability(120), 100 / 220, places=6)
        self.assertIsNone(_american_to_implied_probability(0))

    def test_supported_market_map_contains_core_keys(self):
        mapping = get_supported_market_map()
        self.assertEqual(mapping.get("hits"), "batter_hits")
        self.assertEqual(mapping.get("earned_runs"), "pitcher_earned_runs")

    def test_market_cache_status_shape(self):
        status = get_market_cache_status()
        self.assertTrue(status["ok"])
        self.assertIn("ttl_seconds", status)
        self.assertIn("entries", status)
        self.assertIsInstance(status["entries"], list)

    def test_extract_candidate_outcomes_prefers_side_and_line(self):
        events = [
            {
                "id": "evt1",
                "commence_time": "2026-06-01T23:10:00Z",
                "home_team": "Los Angeles Dodgers",
                "away_team": "New York Yankees",
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "title": "DraftKings",
                        "markets": [
                            {
                                "key": "batter_hits",
                                "outcomes": [
                                    {"description": "Shohei Ohtani", "name": "Over", "point": 1.5, "price": -115},
                                    {"description": "Shohei Ohtani", "name": "Under", "point": 1.5, "price": -105},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
        rows = _extract_candidate_outcomes(
            events=events,
            market_key="batter_hits",
            player_name="Shohei Ohtani",
            over_under="over",
            line=1.5,
        )
        self.assertTrue(rows)
        self.assertEqual(rows[0]["side"], "over")
        self.assertEqual(rows[0]["line"], 1.5)
        self.assertEqual(rows[0]["price_american"], -115)

    @patch("backend.app.services.mlb.market_odds_service._fetch_market_snapshot")
    def test_fetch_market_odds_returns_found_for_supported_prop(self, mock_fetch):
        mock_fetch.return_value = [
            {
                "id": "evt1",
                "commence_time": "2026-06-01T23:10:00Z",
                "home_team": "Los Angeles Dodgers",
                "away_team": "New York Yankees",
                "bookmakers": [
                    {
                        "key": "fanduel",
                        "title": "FanDuel",
                        "markets": [
                            {
                                "key": "batter_hits",
                                "outcomes": [
                                    {"description": "Shohei Ohtani", "name": "Over", "point": 1.5, "price": -120},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
        out = fetch_mlb_market_odds(
            player_name="Shohei Ohtani",
            prop_type="hits",
            game_date="2026-06-01",
            over_under="over",
            line=1.5,
        )
        self.assertTrue(out["ok"])
        self.assertTrue(out["found"])
        self.assertEqual(out["market_key"], "batter_hits")
        self.assertEqual(out["bookmaker"], "FanDuel")
        self.assertEqual(out["price_american"], -120)
        self.assertIn("implied_probability", out)

    def test_fetch_market_odds_unsupported_prop(self):
        out = fetch_mlb_market_odds(
            player_name="Shohei Ohtani",
            prop_type="runs_rbis",
            game_date="2026-06-01",
            over_under="over",
            line=0.5,
        )
        self.assertTrue(out["ok"])
        self.assertFalse(out["found"])
        self.assertIn("unsupported prop_type", out["reason"])

    @patch("backend.app.services.mlb.market_odds_service._fetch_market_snapshot")
    def test_refresh_market_cache_for_date_success(self, mock_fetch):
        mock_fetch.return_value = [{"id": "evt1"}]
        out = refresh_market_cache_for_date(game_date="2026-06-01")
        self.assertTrue(out["ok"])
        self.assertEqual(out["game_date"], "2026-06-01")
        self.assertEqual(out["rows_cached"], 1)
        self.assertIn("cache_hit", out)

    @patch("backend.app.services.mlb.market_odds_service._fetch_market_snapshot")
    def test_refresh_market_cache_for_date_failure_shape(self, mock_fetch):
        mock_fetch.side_effect = RuntimeError(
            "request failed: ...apiKey=super-secret-token&regions=us"
        )
        out = refresh_market_cache_for_date(game_date="2026-06-01")
        self.assertFalse(out["ok"])
        self.assertIn("reason", out)
        self.assertIn("apiKey=[REDACTED]", out["reason"])
        self.assertNotIn("super-secret-token", out["reason"])

    def test_refresh_market_cache_for_date_rejects_bad_date(self):
        with self.assertRaises(ValueError):
            refresh_market_cache_for_date(game_date="06-01-2026")

    @patch.dict("os.environ", {"ODDS_API_KEY": "test-key"}, clear=False)
    @patch("backend.app.services.mlb.market_odds_service.requests.get")
    def test_fetch_market_odds_reuses_cached_snapshot_across_props(self, mock_get):
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return [
                    {
                        "id": "evt1",
                        "commence_time": "2026-06-01T23:10:00Z",
                        "home_team": "Los Angeles Dodgers",
                        "away_team": "New York Yankees",
                        "bookmakers": [
                            {
                                "key": "fanduel",
                                "title": "FanDuel",
                                "markets": [
                                    {
                                        "key": "batter_hits",
                                        "outcomes": [
                                            {"description": "Shohei Ohtani", "name": "Over", "point": 1.5, "price": -120},
                                        ],
                                    },
                                    {
                                        "key": "batter_home_runs",
                                        "outcomes": [
                                            {"description": "Shohei Ohtani", "name": "Over", "point": 0.5, "price": 210},
                                        ],
                                    },
                                ],
                            }
                        ],
                    }
                ]

        mock_get.return_value = _Resp()

        out1 = fetch_mlb_market_odds(
            player_name="Shohei Ohtani",
            prop_type="hits",
            game_date="2026-06-01",
            over_under="over",
            line=1.5,
        )
        out2 = fetch_mlb_market_odds(
            player_name="Shohei Ohtani",
            prop_type="home_runs",
            game_date="2026-06-01",
            over_under="over",
            line=0.5,
        )

        self.assertTrue(out1["found"])
        self.assertTrue(out2["found"])
        self.assertEqual(mock_get.call_count, 1)
        called_markets = mock_get.call_args.kwargs["params"]["markets"]
        self.assertIn("batter_hits", called_markets)
        self.assertIn("batter_home_runs", called_markets)

    @patch.dict("os.environ", {"ODDS_API_KEY": "test-key"}, clear=False)
    @patch("backend.app.services.mlb.market_odds_service.requests.get")
    def test_fetch_market_odds_falls_back_to_event_endpoint_on_422(self, mock_get):
        class _Sport422:
            status_code = 422

            def raise_for_status(self):
                return None

            def json(self):
                return {"message": "invalid markets for this endpoint"}

        class _EventsResp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return [
                    {
                        "id": "evt1",
                        "commence_time": "2026-06-01T23:10:00Z",
                    }
                ]

        class _EventOddsResp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "id": "evt1",
                    "commence_time": "2026-06-01T23:10:00Z",
                    "home_team": "Los Angeles Dodgers",
                    "away_team": "New York Yankees",
                    "bookmakers": [
                        {
                            "key": "fanduel",
                            "title": "FanDuel",
                            "markets": [
                                {
                                    "key": "batter_hits",
                                    "outcomes": [
                                        {
                                            "description": "Shohei Ohtani",
                                            "name": "Over",
                                            "point": 1.5,
                                            "price": -120,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }

        mock_get.side_effect = [_Sport422(), _EventsResp(), _EventOddsResp()]

        out = fetch_mlb_market_odds(
            player_name="Shohei Ohtani",
            prop_type="hits",
            game_date="2026-06-01",
            over_under="over",
            line=1.5,
        )
        self.assertTrue(out["ok"])
        self.assertTrue(out["found"])
        self.assertEqual(out["market_key"], "batter_hits")
        self.assertGreaterEqual(mock_get.call_count, 3)


if __name__ == "__main__":
    unittest.main()
