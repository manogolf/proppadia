from __future__ import annotations

from backend.mlb.scripts.report_mlb_totals_prospective_snapshot_v1 import (
    no_vig_over, select_canonical_observations,
)


def test_no_vig_probability_is_two_sided():
    assert no_vig_over(-110, -110) == .5


def test_cross_provider_consensus_uses_freshest_canonical_book_and_preserves_lineage():
    base = {"game_pk": 1, "canonical_bookmaker_id": "betonline", "fetch_timestamp_utc": "2026-08-07T12:49:03Z"}
    observations = [
        {**base, "provider": "THE_ODDS_API", "bookmaker_update_timestamp_utc": "2026-08-07T12:48:00Z", "total_line": 8.5},
        {**base, "provider": "SPORTSGAMEODDS", "fetch_timestamp_utc": "2026-08-07T12:49:05Z",
         "bookmaker_update_timestamp_utc": "2026-08-07T12:49:00Z", "total_line": 9.0},
    ]
    selected = select_canonical_observations(observations)
    assert len(selected) == 1
    assert selected[0]["provider"] == "SPORTSGAMEODDS"
    assert selected[0]["preserved_source_observation_count"] == 2
    assert selected[0]["alternate_provider_observations"] == "THE_ODDS_API"
