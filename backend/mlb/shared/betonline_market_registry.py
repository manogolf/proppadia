"""Canonical BetOnline MLB player-prop market registry.

This module is intentionally data-only. It gives acquisition and validation
code one shared source for the BetOnline MLB player-prop markets the project
intends to retain for modeling/research.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List


@dataclass(frozen=True)
class BetOnlineMarket:
    local_prop_type: str
    oddsapi_key: str
    prop_family: str
    line_semantics: str
    parser_mapping: str
    expected_side_structure: str
    active_eligibility: str
    endpoint_family: str
    batching_group: str


ACTIVE_BETONLINE_MARKETS: tuple[BetOnlineMarket, ...] = (
    BetOnlineMarket(
        local_prop_type="hits",
        oddsapi_key="batter_hits",
        prop_family="hitter",
        line_semantics="player batter hits total; commonly 0.5 or 1.5",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_hitter_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="hitter_contact",
    ),
    BetOnlineMarket(
        local_prop_type="total_bases",
        oddsapi_key="batter_total_bases",
        prop_family="hitter",
        line_semantics="player batter total bases total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_hitter_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="hitter_contact",
    ),
    BetOnlineMarket(
        local_prop_type="hits_runs_rbis",
        oddsapi_key="batter_hits_runs_rbis",
        prop_family="hitter",
        line_semantics="player hits plus runs plus RBI total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_hitter_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="hitter_run_creation",
    ),
    BetOnlineMarket(
        local_prop_type="home_runs",
        oddsapi_key="batter_home_runs",
        prop_family="hitter",
        line_semantics="player home runs total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_hitter_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="hitter_power_speed",
    ),
    BetOnlineMarket(
        local_prop_type="stolen_bases",
        oddsapi_key="batter_stolen_bases",
        prop_family="hitter",
        line_semantics="player stolen bases total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_hitter_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="hitter_power_speed",
    ),
    BetOnlineMarket(
        local_prop_type="strikeouts_pitching",
        oddsapi_key="pitcher_strikeouts",
        prop_family="pitcher",
        line_semantics="pitcher strikeouts total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_pitcher_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="pitcher_primary",
    ),
    BetOnlineMarket(
        local_prop_type="outs_recorded",
        oddsapi_key="pitcher_outs",
        prop_family="pitcher",
        line_semantics="pitcher outs recorded total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_pitcher_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="pitcher_primary",
    ),
    BetOnlineMarket(
        local_prop_type="earned_runs",
        oddsapi_key="pitcher_earned_runs",
        prop_family="pitcher",
        line_semantics="pitcher earned runs allowed total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_pitcher_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="pitcher_secondary",
    ),
    BetOnlineMarket(
        local_prop_type="hits_allowed",
        oddsapi_key="pitcher_hits_allowed",
        prop_family="pitcher",
        line_semantics="pitcher hits allowed total",
        parser_mapping="player description + over/under outcome + point line",
        expected_side_structure="over_under",
        active_eligibility="active_betonline_pitcher_market",
        endpoint_family="mlb_player_props_event_odds",
        batching_group="pitcher_secondary",
    ),
)


def active_market_rows() -> List[Dict[str, str]]:
    return [asdict(row) for row in ACTIVE_BETONLINE_MARKETS]


def active_market_keys() -> List[str]:
    return [row.oddsapi_key for row in ACTIVE_BETONLINE_MARKETS]


def active_prop_to_market_map() -> Dict[str, str]:
    return {row.local_prop_type: row.oddsapi_key for row in ACTIVE_BETONLINE_MARKETS}


def active_market_to_prop_map() -> Dict[str, str]:
    return {row.oddsapi_key: row.local_prop_type for row in ACTIVE_BETONLINE_MARKETS}


def market_batches(*, max_markets_per_call: int = 6) -> List[Dict[str, str]]:
    step = max(1, int(max_markets_per_call))
    keys = active_market_keys()
    out: List[Dict[str, str]] = []
    for idx, start in enumerate(range(0, len(keys), step), start=1):
        group = keys[start : start + step]
        out.append(
            {
                "batch_id": f"betonline_mlb_player_props_batch_{idx:02d}",
                "market_count": str(len(group)),
                "market_keys": ",".join(group),
            }
        )
    return out
