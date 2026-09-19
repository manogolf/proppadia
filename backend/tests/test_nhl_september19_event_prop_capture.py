from __future__ import annotations

import pandas as pd

from backend.nhl.scripts.run_nhl_september19_event_prop_capture import bind_events, count_family


def test_bind_events_requires_team_and_start_identity():
    games = pd.DataFrame([{
        "game_id": 2026010001,
        "home_team": "STL",
        "away_team": "DAL",
        "scheduled_start_time_utc": "2026-09-19T23:00:00Z",
    }])
    events = [
        {"id": "exact", "home_team": "St. Louis Blues", "away_team": "Dallas Stars", "commence_time": "2026-09-19T23:00:00Z"},
        {"id": "wrong-time", "home_team": "St. Louis Blues", "away_team": "Dallas Stars", "commence_time": "2026-09-20T01:00:00Z"},
    ]
    assert bind_events(events, games) == [{
        "canonical_game_id": 2026010001,
        "provider_event_id": "exact",
        "home_team": "STL",
        "away_team": "DAL",
        "canonical_start_utc": "2026-09-19T23:00:00+00:00",
        "provider_start_utc": "2026-09-19T23:00:00Z",
    }]


def test_family_counts_preserve_book_market_rows():
    event = {"bookmakers": [
        {"key": "book_a", "markets": [
            {"key": "player_points", "outcomes": [{}, {}]},
            {"key": "player_total_saves", "outcomes": [{}, {}, {}]},
        ]},
        {"key": "book_b", "markets": [{"key": "player_points", "outcomes": [{}]}]},
    ]}
    assert count_family(event, {"player_points"}) == {
        "book_count": 2,
        "market_objects": 2,
        "outcome_rows": 3,
        "by_book_market_outcomes": {
            "book_a": {"player_points": 2},
            "book_b": {"player_points": 1},
        },
    }
