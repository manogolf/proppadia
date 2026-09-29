import unittest

import pandas as pd

from backend.nhl.cross_market_shadow.core import normalize_markets


def canonical_schedule():
    rows = [
        (2026020001, "2026-09-29T21:00:00Z", 12, "CAR", 13, "FLA"),
        (2026020002, "2026-09-29T23:00:00Z", 10, "TOR", 8, "MTL"),
        (2026020003, "2026-09-30T00:00:00Z", 6, "BOS", 3, "NYR"),
        (2026020004, "2026-09-30T02:00:00Z", 22, "EDM", 23, "VAN"),
        (2026020005, "2026-09-30T02:30:00Z", 54, "VGK", 16, "CHI"),
    ]
    return pd.DataFrame([{
        "canonical_season": 2026, "slate_date": "2026-09-29",
        "game_id": game_id, "game_date": start[:10],
        "scheduled_start_time_utc": start, "home_team_id": home_id,
        "home_team": home, "away_team_id": away_id, "away_team": away,
        "game_status": "SCHEDULED", "game_type_code": 2,
    } for game_id, start, home_id, home, away_id, away in rows])


def event(event_id, home, away, commence, markets=None):
    return {
        "id": event_id, "sport_key": "icehockey_nhl", "home_team": home,
        "away_team": away, "commence_time": commence,
        "bookmakers": [{
            "key": "fixturebook", "title": "Fixture Book",
            "last_update": "2026-09-29T20:00:00Z",
            "markets": markets or [],
        }],
    }


class ProviderEventUnicodeBindingTest(unittest.TestCase):
    def setUp(self):
        self.tor_mtl_markets = [
            {"key": "h2h", "outcomes": [
                {"name": "Montréal Canadiens", "price": -112},
                {"name": "Toronto Maple Leafs", "price": -108},
            ]},
            {"key": "spreads", "outcomes": [
                {"name": "Montréal Canadiens", "point": -1.5, "price": 225},
                {"name": "Toronto Maple Leafs", "point": 1.5, "price": -278},
            ]},
        ]
        self.schedule = canonical_schedule()
        events = [
            event("e-car-fla", "Carolina Hurricanes", "Florida Panthers", "2026-09-29T21:00:00Z"),
            event("e-tor-mtl", "Toronto Maple Leafs", "Montréal Canadiens", "2026-09-29T23:10:00Z", self.tor_mtl_markets),
            event("e-bos-nyr", "Boston Bruins", "New York Rangers", "2026-09-30T00:00:00Z"),
            event("e-edm-van", "Edmonton Oilers", "Vancouver Canucks", "2026-09-30T02:00:00Z"),
            event("e-vgk-chi", "Vegas Golden Knights", "Chicago Blackhawks", "2026-09-30T02:30:00Z"),
        ]
        self.envelope = {
            "capture_timestamp_utc": "2026-09-29T20:30:00Z",
            "provider_response": events,
        }
        self.quotes, self.bindings, _ = normalize_markets(self.envelope, self.schedule)

    def test_ordered_provider_orientation_matches_canonical(self):
        binding = self.bindings.set_index("provider_event_id").loc["e-tor-mtl"]
        self.assertEqual(binding.binding_status, "BOUND")
        self.assertEqual(int(binding.canonical_game_id), 2026020002)
        self.assertEqual(binding.binding_reason, "ORDERED_TEAM_PAIR_AND_START_TIME_MATCH")
        self.assertEqual(binding.team_name_normalization, "UNICODE_DIACRITICS_FOLDED")

    def test_h2h_outcomes_bind_by_named_team_to_canonical_home_away(self):
        quotes = self.quotes[
            self.quotes.provider_event_id.eq("e-tor-mtl")
            & self.quotes.provider_market_key.eq("h2h")
        ].set_index("side_team")
        self.assertEqual(int(quotes.loc["TOR", "game_id"]), 2026020002)
        self.assertEqual(quotes.loc["TOR", "side_orientation"], "HOME")
        self.assertEqual(quotes.loc["MTL", "side_orientation"], "AWAY")

    def test_spread_points_remain_attached_to_named_team(self):
        quotes = self.quotes[
            self.quotes.provider_event_id.eq("e-tor-mtl")
            & self.quotes.provider_market_key.eq("spreads")
        ].set_index("side_team")
        self.assertEqual(quotes.loc["MTL", "point"], -1.5)
        self.assertEqual(quotes.loc["MTL", "side_orientation"], "AWAY")
        self.assertEqual(quotes.loc["TOR", "point"], 1.5)
        self.assertEqual(quotes.loc["TOR", "side_orientation"], "HOME")

    def test_different_team_pair_remains_unmatched(self):
        binding = self.bindings[self.bindings.provider_event_id.eq("e-tor-mtl")].iloc[0]
        self.assertEqual(binding.canonical_game_id, 2026020002)
        wrong = event("wrong-pair", "Toronto Maple Leafs", "Boston Bruins", "2026-09-29T23:00:00Z")
        _, bindings, _ = normalize_markets({**self.envelope, "provider_response": [wrong]}, self.schedule)
        self.assertEqual(bindings.iloc[0].binding_status, "UNMATCHED_OR_AMBIGUOUS")

    def test_ambiguous_name_and_time_outside_tolerance_fail_closed(self):
        ambiguous = event("ambiguous", "Toronto Maple Leafs", "Montréal Canadiens Reserves", "2026-09-29T23:00:00Z")
        too_late = event("late", "Toronto Maple Leafs", "Montréal Canadiens", "2026-09-29T23:16:00Z")
        _, bindings, _ = normalize_markets(
            {**self.envelope, "provider_response": [ambiguous, too_late]}, self.schedule,
        )
        self.assertTrue(bindings.binding_status.eq("UNMATCHED_OR_AMBIGUOUS").all())
        self.assertTrue(bindings.binding_reason.eq("NO_UNIQUE_CANONICAL_CANDIDATE").all())

    def test_no_automatic_reverse_orientation_is_applied(self):
        reversed_event = event(
            "reversed", "Montréal Canadiens", "Toronto Maple Leafs",
            "2026-09-29T23:00:00Z", self.tor_mtl_markets,
        )
        _, bindings, _ = normalize_markets(
            {**self.envelope, "provider_response": [reversed_event]}, self.schedule,
        )
        self.assertEqual(bindings.iloc[0].binding_status, "UNMATCHED_OR_AMBIGUOUS")

    def test_all_five_canonical_game_bindings_are_unique_and_provider_ids_retained(self):
        bound = self.bindings[self.bindings.binding_status.eq("BOUND")]
        self.assertEqual(len(bound), 5)
        self.assertEqual(bound.canonical_game_id.nunique(), 5)
        self.assertEqual(bound.provider_event_id.nunique(), 5)
        bound_tor_mtl = bound[bound.canonical_game_id.eq(2026020002)].iloc[0]
        self.assertEqual(bound_tor_mtl.provider_event_id, "e-tor-mtl")


if __name__ == "__main__":
    unittest.main()
