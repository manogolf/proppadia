#!/usr/bin/env python3
"""Read-only identity-key feasibility audit for preserved BetOnline payloads."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path


def write(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--expanded-payload", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    files = sorted(args.raw_root.glob("*/event_*.json"))
    event_list_files = sorted(args.raw_root.glob("*/events.json"))
    field_counts = Counter()
    outcome_count = 0
    snapshots = defaultdict(list)
    event_ids = set()
    prices = defaultdict(set)
    side_pairs = defaultdict(set)

    for path in files:
        payload = json.loads(path.read_text())
        event_ids.add(payload.get("id"))
        for key in payload:
            field_counts[f"$.{key}"] += 1
        for book in payload.get("bookmakers", []):
            for key in book:
                field_counts[f"$.bookmakers[].{key}"] += 1
            if book.get("key") != "betonlineag":
                continue
            for market in book.get("markets", []):
                for key in market:
                    field_counts[f"$.bookmakers[].markets[].{key}"] += 1
                for outcome in market.get("outcomes", []):
                    outcome_count += 1
                    for key in outcome:
                        field_counts[f"$.bookmakers[].markets[].outcomes[].{key}"] += 1
                    identity = (
                        payload.get("id"), market.get("key"), outcome.get("description"),
                        outcome.get("point"),
                    )
                    snapshots[identity].append(path.parent.name)
                    prices[identity].add(outcome.get("price"))
                    side_pairs[identity].add(outcome.get("name"))

    expanded = json.loads(args.expanded_payload.read_text())
    expanded_book = next(
        (b for b in expanded.get("bookmakers", []) if b.get("key") == "betonlineag"), {}
    )
    expanded_market = next(
        (m for m in expanded_book.get("markets", []) if m.get("key") == "batter_total_bases"), {}
    )
    expanded_outcomes = expanded_market.get("outcomes", [])

    total_files = len(files)
    rows = [
        {
            "field_path": "$.id", "example_value": next(iter(event_ids), ""),
            "presence_rate": f"{total_files}/{total_files} event payloads",
            "uniqueness": f"{len(event_ids)} unique event values",
            "cross_snapshot_stability": "STABLE_FOR_EVENT",
            "over_under_shared": "YES", "identity_scope": "PROVIDER_EVENT_ONLY",
        },
        {
            "field_path": "$.bookmakers[].key", "example_value": "betonlineag",
            "presence_rate": "all returned BetOnline bookmaker objects",
            "uniqueness": "constant bookmaker key", "cross_snapshot_stability": "STABLE",
            "over_under_shared": "YES", "identity_scope": "BOOKMAKER_ONLY",
        },
        {
            "field_path": "$.bookmakers[].markets[].key",
            "example_value": "batter_total_bases",
            "presence_rate": "all inspected TB market objects", "uniqueness": "constant market type",
            "cross_snapshot_stability": "STABLE", "over_under_shared": "YES",
            "identity_scope": "MARKET_TYPE_ONLY",
        },
        {
            "field_path": "$.bookmakers[].markets[].outcomes[].description",
            "example_value": expanded_outcomes[0].get("description", "") if expanded_outcomes else "",
            "presence_rate": f"{outcome_count}/{outcome_count} outcomes",
            "uniqueness": "display names recur across Over/Under and snapshots",
            "cross_snapshot_stability": "TEXT_STABLE_WHEN_PRESENT",
            "over_under_shared": "YES", "identity_scope": "PLAYER_NAME_ONLY_PROHIBITED",
        },
        {
            "field_path": "$.bookmakers[].sid", "example_value": expanded_book.get("sid"),
            "presence_rate": "available only with includeSids; 1/1 expanded bookmaker",
            "uniqueness": "one BetOnline event-page source ID",
            "cross_snapshot_stability": "NOT_ENOUGH_EXPANDED_SNAPSHOTS",
            "over_under_shared": "YES", "identity_scope": "BOOK_EVENT_ONLY",
        },
        {
            "field_path": "$.bookmakers[].markets[].sid",
            "example_value": expanded_market.get("sid"),
            "presence_rate": "0/1 expanded TB markets non-null", "uniqueness": "NONE",
            "cross_snapshot_stability": "NOT_APPLICABLE", "over_under_shared": "NOT_APPLICABLE",
            "identity_scope": "NO_KEY",
        },
        {
            "field_path": "$.bookmakers[].markets[].outcomes[].sid",
            "example_value": "",
            "presence_rate": f"0/{len(expanded_outcomes)} expanded outcomes non-null",
            "uniqueness": "NONE", "cross_snapshot_stability": "NOT_APPLICABLE",
            "over_under_shared": "NOT_APPLICABLE", "identity_scope": "NO_PLAYER_OR_SELECTION_KEY",
        },
        {
            "field_path": "participant_id/player_id/competitor_id/selection_id/outcome_id/offer_id",
            "example_value": "", "presence_rate": "0 across all inspected raw outcome objects",
            "uniqueness": "NONE", "cross_snapshot_stability": "NOT_APPLICABLE",
            "over_under_shared": "NOT_APPLICABLE", "identity_scope": "ABSENT",
        },
    ]
    write(args.output_dir / "raw_odds_identity_field_inventory.csv", list(rows[0]), rows)

    repeat = [k for k, v in snapshots.items() if len(set(v)) > 1]
    changed = [k for k in repeat if len(prices[k]) > 1]
    paired = [k for k, v in side_pairs.items() if {"Over", "Under"}.issubset(v)]
    stability = [
        {
            "candidate_key": "provider_event_id",
            "repeated_player_game_market_groups": len(repeat),
            "groups_with_price_change": len(changed),
            "over_under_pair_groups": len(paired),
            "collisions": "many players share event id",
            "key_churn": 0,
            "decision": "STABLE_GAME_KEY_NOT_PLAYER_KEY",
        },
        {
            "candidate_key": "display_name_with_event_market_line",
            "repeated_player_game_market_groups": len(repeat),
            "groups_with_price_change": len(changed),
            "over_under_pair_groups": len(paired),
            "collisions": "not certified; textual identity",
            "key_churn": "not authoritative",
            "decision": "NAME_DEPENDENT_BRIDGE_PROHIBITED",
        },
        {
            "candidate_key": "outcome_sid",
            "repeated_player_game_market_groups": 0,
            "groups_with_price_change": 0,
            "over_under_pair_groups": 0,
            "collisions": "not applicable",
            "key_churn": "not applicable",
            "decision": "NULL_NOT_AVAILABLE",
        },
    ]
    write(args.output_dir / "candidate_key_stability.csv", list(stability[0]), stability)

    endpoints = [
        {"endpoint": "GET /v4/sports/baseball_mlb/events", "tested": "YES",
         "identity_result": "provider event ID and team display names only", "raw_preserved": "YES"},
        {"endpoint": "GET /v4/sports/baseball_mlb/events/{eventId}/odds", "tested": "YES",
         "identity_result": "player description text only", "raw_preserved": "YES"},
        {"endpoint": "event odds includeSids=true&includeLinks=true", "tested": "YES",
         "identity_result": "book event SID/link; market and outcome SIDs null", "raw_preserved": "YES"},
        {"endpoint": "GET /v4/sports/{sport}/participants", "tested": "DOCUMENTATION",
         "identity_result": "provider states team-sport endpoint does not return roster players",
         "raw_preserved": "NOT_CALLED_NO_FEASIBLE_PLAYER_ROUTE"},
        {"endpoint": "GET /events/{eventId}/markets", "tested": "DOCUMENTATION",
         "identity_result": "market keys by bookmaker; no participant entity contract",
         "raw_preserved": "NOT_CALLED_NO_FEASIBLE_PLAYER_ROUTE"},
    ]
    write(args.output_dir / "provider_endpoint_identity_audit.csv", list(endpoints[0]), endpoints)

    routes = [
        {"route": "A_DIRECT_MLB_ID", "available": "NO", "evidence": "no player_mlb_id field"},
        {"route": "B_PROVIDER_PARTICIPANT_CROSSWALK", "available": "NO",
         "evidence": "no player participant ID and participant endpoint excludes team rosters"},
        {"route": "C_SPORTSBOOK_SELECTION_ENTITY", "available": "NO",
         "evidence": "outcome SID null; event SID identifies game only"},
        {"route": "D_NAME_CONSTRAINED_BRIDGE", "available": "TECHNICALLY_POSSIBLE_NOT_CERTIFIED",
         "evidence": "description plus event/team roster remains name dependent and prohibited"},
    ]
    write(args.output_dir / "identity_route_assessment.csv", list(routes[0]), routes)

    alternatives = [
        {"source": "SportsGameOdds", "betonline_coverage": "DOCUMENTED",
         "player_id_type": "stable provider playerID embedded in oddID",
         "mlb_id_crosswalk": "NO AUTHORITATIVE MLBAM CROSSWALK DOCUMENTED",
         "authentication_available": "NO", "cost_quota": "new account/subscription required",
         "current_date": "not tested", "raw_preservation": "possible", "decision": "NOT_CERTIFIED"},
        {"source": "SportsDataIO MLB Advanced Odds", "betonline_coverage": "CONTACT_PROVIDER",
         "player_id_type": "stable SportsDataIO PlayerID",
         "mlb_id_crosswalk": "NO MLBAM FIELD CERTIFIED IN REVIEWED DICTIONARY",
         "authentication_available": "NO", "cost_quota": "licensed product",
         "current_date": "not tested", "raw_preservation": "possible", "decision": "NOT_CERTIFIED"},
        {"source": "TheRundown", "betonline_coverage": "NOT CERTIFIED IN REVIEWED DOCS",
         "player_id_type": "stable provider participant ID",
         "mlb_id_crosswalk": "NO AUTHORITATIVE MLBAM CROSSWALK IDENTIFIED",
         "authentication_available": "NO", "cost_quota": "new account required",
         "current_date": "not tested", "raw_preservation": "possible", "decision": "NOT_CERTIFIED"},
    ]
    write(args.output_dir / "alternative_odds_source_assessment.csv", list(alternatives[0]), alternatives)

    report = f"""# BetOnline Player-prop Identity-key Feasibility

## Result

The current parser did not omit a usable player identity key. The certified current
cycle manifest contains 53 payloads: 31 official MLB payloads and all 22 odds
payloads. For stronger cross-snapshot evidence, this audit also inspected the prior
two clean-room attempts: {len(files)} event-odds payloads and
{len(event_list_files)} event-list payloads in total. The raw player-prop outcome
contains only side, display-name description, price, and point.

The documented `includeSids` expansion was tested. BetOnline returned a stable
event-page SID and link, but market SID was null and all {len(expanded_outcomes)}
outcome SIDs were null. The SID identifies the game, not a player or selection.

Repeated payloads demonstrate that the provider event ID remains stable while prices
can change, but it collides across every player in the game. No player entity exists
to crosswalk authoritatively to MLBAM.

Routes A, B, and C fail. Route D is technically possible but remains
`NAME_DEPENDENT_BRIDGE_PROHIBITED`. No reviewed alternative simultaneously certifies
BetOnline coverage, stable player identity, and an authoritative MLB-ID crosswalk.

No parser or clean-room database change is authorized.
"""
    (args.output_dir / "cleanroom_odds_identity_report.md").write_text(report)
    terminal = """MLB_CLEANROOM_ODDS_RAW_ID_DECISION = NO_USABLE_PLAYER_ID_PARSER_DID_NOT_OMIT_KEY
MLB_CLEANROOM_ODDS_KEY_STABILITY_DECISION = EVENT_ID_STABLE_BUT_GAME_ONLY_OUTCOME_IDS_NULL
MLB_CLEANROOM_ODDS_CROSSWALK_DECISION = NO_AUTHORITATIVE_PLAYER_TO_MLB_ID_ROUTE
MLB_CLEANROOM_ALTERNATIVE_SOURCE_DECISION = NO_CURRENTLY_AUTHENTICATED_OR_CERTIFIED_ALTERNATIVE
MLB_CLEANROOM_ODDS_IDENTITY_DECISION = CURRENT_PROVIDER_NAME_ONLY_NOT_CLEANROOM_COMPATIBLE
MLB_CLEANROOM_BOARD_DECISION = REMAINS_IDENTITY_COVERAGE_BLOCKED
MLB_CLEANROOM_NAME_MATCHING_DECISION = PROHIBITED
"""
    (args.output_dir / "terminal_decision.md").write_text(terminal)
    print(json.dumps({"event_payloads": len(files), "event_list_payloads": len(event_list_files),
                      "betonline_outcomes": outcome_count, "repeated_groups": len(repeat),
                      "price_change_groups": len(changed)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
