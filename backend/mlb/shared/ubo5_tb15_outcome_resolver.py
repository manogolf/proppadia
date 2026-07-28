"""Canonical exact-identity outcome resolution for UBO-5 TB 1.5 populations."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

Identity = tuple[str, int, int, str, float]
FINAL_STATES = {"final", "game over", "completed early"}
PENDING_STATES = {
    "postponed", "suspended", "delayed", "scheduled", "pre-game",
    "in progress", "warmup",
}


def number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def identity(date: str, row: dict) -> Identity:
    return (
        date,
        int(float(row.get("game_pk") or row.get("game_id"))),
        int(float(row.get("batter_mlb_id") or row.get("player_id"))),
        "total_bases",
        1.5,
    )


def total_bases_from_stats(stats: dict) -> tuple[float | None, bool]:
    parts = [number(stats.get(k)) for k in ("singles", "doubles", "triples", "home_runs")]
    calculated = (
        None if any(v is None for v in parts)
        else parts[0] + 2 * parts[1] + 3 * parts[2] + 4 * parts[3]
    )
    stored = number(stats.get("total_bases"))
    conflict = calculated is not None and stored is not None and calculated != stored
    return (calculated if calculated is not None else stored), conflict


def resolve_tb15_outcome(
    ident: Identity,
    *,
    reconcile_outcome: dict | None,
    player_stats_outcome: dict | None,
    official_game_status: str,
    market_action: bool = True,
    final_lineup_member: bool | None = None,
    reconcile_source_path: str = "",
    player_stats_source_path: str = "mlb.player_stats",
    game_status_source_path: str = "",
    source_revision: str = "",
    player_stats_available: bool = True,
) -> dict:
    """Resolve one frozen population member without name matching.

    Strong market-backed reconciliation has precedence, followed by exact-ID
    official player stats, then official game/participation state.
    """
    now = datetime.now(timezone.utc).isoformat()
    base = {
        "slate_date": ident[0], "game_pk": ident[1], "batter_mlb_id": ident[2],
        "prop_type": ident[3], "line": ident[4], "source_revision": source_revision,
        "resolved_timestamp_utc": now,
    }
    market = reconcile_outcome or {}
    official = player_stats_outcome or {}
    market_value = number(market.get("value"))
    official_value = number(official.get("value"))
    stats = official.get("stats") or {}
    pa = number(stats.get("plate_appearances"))
    appeared = pa is not None and pa > 0
    status = (official_game_status or "").strip().casefold()
    game_final = status in FINAL_STATES
    game_pending = not game_final or status in PENDING_STATES

    if market.get("conflict") or official.get("conflict"):
        return base | {
            "result": "TECHNICAL_UNRESOLVED",
            "resolution_reason_code": "CONFLICTING_AUTHORITATIVE_OUTCOMES",
            "value": None, "stats": stats, "outcome_source": "CONFLICT",
            "outcome_source_path": f"{reconcile_source_path}|{player_stats_source_path}",
            "resolution_method": "FAIL_CLOSED_CONFLICT",
        }
    if market_value is not None and official_value is not None and market_value != official_value:
        return base | {
            "result": "TECHNICAL_UNRESOLVED",
            "resolution_reason_code": "CROSS_SOURCE_TOTAL_BASES_CONFLICT",
            "value": None, "stats": stats, "outcome_source": "CONFLICT",
            "outcome_source_path": f"{reconcile_source_path}|{player_stats_source_path}",
            "resolution_method": "FAIL_CLOSED_CONFLICT",
        }
    if not market_action:
        return base | {
            "result": "NO_ACTION", "resolution_reason_code": "MARKET_ROW_WITHOUT_ACTION",
            "value": official_value if official_value is not None else market_value,
            "stats": stats, "outcome_source": "POPULATION_CONTRACT",
            "outcome_source_path": "", "resolution_method": "NO_ACTION_CONTRACT",
        }
    if market_value is not None:
        return base | {
            "result": "WIN" if market_value >= 2 else "LOSS",
            "resolution_reason_code": "MARKET_BACKED_RECONCILIATION",
            "value": market_value, "stats": stats, "outcome_source": "RECONCILE_ROWS",
            "outcome_source_path": reconcile_source_path,
            "resolution_method": "EXACT_ID_MARKET_BACKED",
        }
    if game_pending:
        return base | {
            "result": "PENDING",
            "resolution_reason_code": (
                "POSTPONED_GAME_PENDING" if status == "postponed"
                else "OFFICIAL_GAME_NOT_FINAL_PENDING"
            ),
            "value": None, "stats": stats, "outcome_source": "OFFICIAL_GAME_STATUS",
            "outcome_source_path": game_status_source_path,
            "resolution_method": "PENDING_OFFICIAL_GAME",
        }
    if not player_stats_available and market_value is None:
        return base | {
            "result": "TECHNICAL_UNRESOLVED",
            "resolution_reason_code": "CERTIFIED_PLAYER_STATS_SOURCE_UNAVAILABLE",
            "value": None, "stats": stats, "outcome_source": "NONE",
            "outcome_source_path": player_stats_source_path,
            "resolution_method": "FAIL_CLOSED_SOURCE_UNAVAILABLE",
        }
    if official_value is not None:
        return base | {
            "result": "WIN" if official_value >= 2 else "LOSS",
            "resolution_reason_code": (
                "NOT_IN_FINAL_LINEUP_LATER_APPEARANCE_ACTION"
                if final_lineup_member is False and appeared
                else "EXACT_ID_PLAYER_STATS_FALLBACK"
            ),
            "value": official_value, "stats": stats, "outcome_source": "MLB_PLAYER_STATS",
            "outcome_source_path": player_stats_source_path,
            "resolution_method": "EXACT_ID_OFFICIAL_PLAYER_GAME",
        }
    if final_lineup_member is False or not appeared:
        return base | {
            "result": "NO_ACTION",
            "resolution_reason_code": "NOT_IN_FINAL_LINEUP_NO_ACTION",
            "value": None, "stats": stats, "outcome_source": "OFFICIAL_LINEUP_AND_PARTICIPATION",
            "outcome_source_path": game_status_source_path,
            "resolution_method": "NO_ACTION_FINAL_LINEUP",
        }
    return base | {
        "result": "TECHNICAL_UNRESOLVED",
        "resolution_reason_code": "COMPLETED_GAME_OUTCOME_UNAVAILABLE",
        "value": None, "stats": stats, "outcome_source": "NONE",
        "outcome_source_path": f"{reconcile_source_path}|{player_stats_source_path}",
        "resolution_method": "FAIL_CLOSED_MISSING_COMPLETED_OUTCOME",
    }


def resolution_counts(rows: list[dict]) -> dict:
    return {
        "population_rows_inspected": len(rows),
        "market_backed_resolutions": sum(r.get("resolution_method") == "EXACT_ID_MARKET_BACKED" for r in rows),
        "exact_id_fallback_resolutions": sum(r.get("resolution_method") == "EXACT_ID_OFFICIAL_PLAYER_GAME" for r in rows),
        "no_action_classifications": sum(r.get("result") == "NO_ACTION" for r in rows),
        "void_classifications": sum(r.get("result") == "VOID" for r in rows),
        "pending_postponed_rows": sum(r.get("resolution_reason_code") == "POSTPONED_GAME_PENDING" for r in rows),
        "technical_unresolved_rows": sum(r.get("result") == "TECHNICAL_UNRESOLVED" for r in rows),
    }
