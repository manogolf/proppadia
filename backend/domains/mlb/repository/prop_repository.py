"""MLB prop persistence queries."""

from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

try:
    import psycopg
    import psycopg.rows
except Exception:  # pragma: no cover - environment-dependent import
    psycopg = None  # type: ignore

from backend.supabase.supabase_utils import get_database_url


def _db_url() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return url


def _fetchone(sql: str, params: Sequence[Any]) -> Optional[Dict[str, Any]]:
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    with psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def _execute(sql: str, params: Sequence[Any]) -> None:
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    with psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()


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
    row = _fetchone(
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
    _execute(
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
