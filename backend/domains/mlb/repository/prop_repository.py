"""MLB prop persistence queries."""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from backend.shared.db import pg_execute, pg_fetchone


class DuplicatePropError(Exception):
    """Raised when DB unique constraints indicate a duplicate prop insert."""


def find_duplicate_prop_id(
    *,
    player_id: int,
    game_id: int,
    prop_type: str,
    over_under: str,
    prop_value: float,
    prop_source: str,
) -> Optional[str]:
    sql = """
        SELECT id
        FROM player_props
        WHERE CAST(player_id AS TEXT) = %s
          AND CAST(game_id AS TEXT) = %s
          AND prop_type = %s
          AND over_under = %s
          AND prop_value = %s
          AND prop_source = %s
        LIMIT 1
    """
    row = pg_fetchone(
        sql,
        (
            str(player_id),
            str(game_id),
            prop_type,
            over_under,
            prop_value,
            prop_source,
        ),
    )
    if not row:
        return None
    value = row.get("id")
    return str(value) if value is not None else None


def insert_prop_row(
    *,
    player_id: int,
    player_name: Optional[str],
    team: Optional[str],
    team_id: Optional[int],
    game_id: int,
    game_date: str,
    prop_type: str,
    prop_value: float,
    over_under: str,
    prop_source: str,
    recommendation: str,
    probability: float,
) -> None:
    sql = """
        INSERT INTO player_props (
          player_id, player_name, team, team_id,
          game_id, game_date, prop_type, prop_value, over_under,
          status, prop_source, predicted_outcome, confidence_score,
          created_at, prediction_timestamp
        ) VALUES (
          %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          'pending', %s, %s, %s,
          NOW(), NOW()
        )
    """
    try:
        pg_execute(
            sql,
            (
                str(player_id),
                player_name,
                team,
                int(team_id) if team_id is not None else None,
                str(game_id),
                game_date,
                prop_type,
                prop_value,
                over_under,
                prop_source,
                recommendation,
                probability,
            ),
        )
    except Exception as e:
        # SQLSTATE 23505 = unique_violation (race-safe duplicate handling).
        if getattr(e, "sqlstate", None) == "23505":
            raise DuplicatePropError(str(e)) from e
        raise
