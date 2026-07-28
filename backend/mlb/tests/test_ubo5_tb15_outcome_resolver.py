from __future__ import annotations

from backend.mlb.shared.ubo5_tb15_outcome_resolver import (
    resolve_tb15_outcome,
    total_bases_from_stats,
)


IDENT = ("2026-07-27", 777, 13, "total_bases", 1.5)


def stats(*, pa: int = 4, singles: int = 0, doubles: int = 0,
          triples: int = 0, home_runs: int = 0) -> dict:
    total = singles + 2 * doubles + 3 * triples + 4 * home_runs
    return {
        "value": total,
        "stats": {
            "plate_appearances": pa,
            "at_bats": max(pa - 1, 0),
            "singles": singles,
            "doubles": doubles,
            "triples": triples,
            "home_runs": home_runs,
            "total_bases": total,
        },
    }


def resolve(**overrides: object) -> dict:
    args = {
        "reconcile_outcome": None,
        "player_stats_outcome": None,
        "official_game_status": "Final",
        "market_action": True,
        "final_lineup_member": True,
    }
    args.update(overrides)
    return resolve_tb15_outcome(IDENT, **args)


def test_total_bases_arithmetic() -> None:
    value, conflict = total_bases_from_stats({
        "singles": 1, "doubles": 1, "triples": 1, "home_runs": 1,
        "total_bases": 10,
    })
    assert value == 10
    assert conflict is False


def test_missing_market_outcome_uses_exact_id_player_stats() -> None:
    row = resolve(player_stats_outcome=stats(doubles=1))
    assert row["result"] == "WIN"
    assert row["resolution_method"] == "EXACT_ID_OFFICIAL_PLAYER_GAME"


def test_market_backed_reconciliation_has_precedence() -> None:
    row = resolve(
        reconcile_outcome={"value": 1},
        player_stats_outcome=stats(singles=1),
    )
    assert row["result"] == "LOSS"
    assert row["resolution_method"] == "EXACT_ID_MARKET_BACKED"


def test_conflicting_authoritative_sources_fail_closed() -> None:
    row = resolve(
        reconcile_outcome={"value": 1},
        player_stats_outcome=stats(home_runs=1),
    )
    assert row["result"] == "TECHNICAL_UNRESOLVED"
    assert row["resolution_reason_code"] == "CROSS_SOURCE_TOTAL_BASES_CONFLICT"


def test_final_game_absent_lineup_and_no_participation_is_no_action() -> None:
    row = resolve(final_lineup_member=False)
    assert row["result"] == "NO_ACTION"
    assert row["resolution_reason_code"] == "NOT_IN_FINAL_LINEUP_NO_ACTION"


def test_nonstarter_who_later_appears_is_action() -> None:
    row = resolve(
        final_lineup_member=False,
        player_stats_outcome=stats(pa=1, singles=1),
    )
    assert row["result"] == "LOSS"
    assert row["resolution_reason_code"] == "NOT_IN_FINAL_LINEUP_LATER_APPEARANCE_ACTION"


def test_postponed_game_remains_pending() -> None:
    row = resolve(official_game_status="Postponed", final_lineup_member=False)
    assert row["result"] == "PENDING"
    assert row["resolution_reason_code"] == "POSTPONED_GAME_PENDING"


def test_missing_certified_source_fails_closed_for_final_game() -> None:
    row = resolve(player_stats_available=False)
    assert row["result"] == "TECHNICAL_UNRESOLVED"
    assert row["resolution_reason_code"] == "CERTIFIED_PLAYER_STATS_SOURCE_UNAVAILABLE"


def test_wrong_game_identity_cannot_supply_makeup_outcome() -> None:
    # Outcome dictionaries are already keyed by the complete exact identity.
    # Passing no value for this original game cannot borrow a same-player makeup.
    row = resolve(official_game_status="Postponed")
    assert row["result"] == "PENDING"
    assert row["game_pk"] == 777


def test_market_no_action_contract_is_not_outcome_derived() -> None:
    row = resolve(player_stats_outcome=stats(home_runs=1), market_action=False)
    assert row["result"] == "NO_ACTION"
    assert row["resolution_reason_code"] == "MARKET_ROW_WITHOUT_ACTION"


def test_resolution_content_is_stable_except_timestamp() -> None:
    first = resolve(player_stats_outcome=stats(doubles=1))
    second = resolve(player_stats_outcome=stats(doubles=1))
    first.pop("resolved_timestamp_utc")
    second.pop("resolved_timestamp_utc")
    assert first == second


def test_authoritative_change_changes_only_that_identity_result() -> None:
    loss = resolve(player_stats_outcome=stats(singles=1))
    win = resolve(player_stats_outcome=stats(doubles=1))
    assert loss["result"] == "LOSS"
    assert win["result"] == "WIN"
    assert loss["game_pk"] == win["game_pk"] == 777
    assert loss["batter_mlb_id"] == win["batter_mlb_id"] == 13
