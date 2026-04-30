"""MLB prop persistence queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from backend.shared.db import pg_execute, pg_fetchall, pg_fetchone


class DuplicatePropError(Exception):
    """Raised when DB unique constraints indicate a duplicate prop insert."""


_has_user_id_column_cache: Optional[bool] = None
_player_props_columns_cache: Optional[Set[str]] = None


def _player_props_columns() -> Set[str]:
    global _player_props_columns_cache
    if _player_props_columns_cache is not None:
        return _player_props_columns_cache
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='mlb'
          AND table_name='player_props'
    """
    rows = pg_fetchall(sql)
    _player_props_columns_cache = {str(r.get("column_name") or "").strip() for r in rows}
    return _player_props_columns_cache


def _has_user_id_column() -> bool:
    global _has_user_id_column_cache
    if _has_user_id_column_cache is not None:
        return _has_user_id_column_cache
    _has_user_id_column_cache = "user_id" in _player_props_columns()
    return _has_user_id_column_cache


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
        FROM mlb.player_props
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
    game_type: Optional[str] = None,
    user_id: Optional[str] = None,
) -> None:
    normalized_game_type = str(game_type or "").strip().upper() or None
    columns = [
        "player_id",
        "player_name",
        "team",
        "team_id",
        "game_id",
        "game_date",
        "prop_type",
        "prop_value",
        "over_under",
        "status",
        "prop_source",
        "predicted_outcome",
        "confidence_score",
    ]
    values = [
        str(player_id),
        player_name,
        team,
        int(team_id) if team_id is not None else None,
        str(game_id),
        game_date,
        prop_type,
        prop_value,
        over_under,
        "pending",
        prop_source,
        recommendation,
        probability,
    ]
    placeholders = ["%s"] * len(values)
    columns.extend(["created_at", "prediction_timestamp"])
    placeholders.extend(["NOW()", "NOW()"])

    if "game_type" in _player_props_columns():
        columns.append("game_type")
        placeholders.append("%s")
        values.append(normalized_game_type)

    if user_id and _has_user_id_column():
        columns.append("user_id")
        placeholders.append("%s")
        values.append(str(user_id))

    sql = f"""
        INSERT INTO mlb.player_props ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
    """
    try:
        pg_execute(
            sql,
            tuple(values),
        )
    except Exception as e:
        # SQLSTATE 23505 = unique_violation (race-safe duplicate handling).
        if getattr(e, "sqlstate", None) == "23505":
            raise DuplicatePropError(str(e)) from e
        raise


def fetch_prop_history_rows(
    *,
    limit: int = 50,
    offset: int = 0,
    user_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = None,
    prop_source_prefix: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    where_sql, params = _build_prop_history_where(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        prop_source_prefix=prop_source_prefix,
        status=status,
    )

    columns = [
        "id",
        "player_id",
        "player_name",
        "team",
        "team_id",
        "game_id",
        "game_date",
        "prop_type",
        "prop_value",
        "over_under",
        "status",
        "outcome",
        "prop_source",
        "confidence_score",
        "predicted_outcome",
        "prediction_timestamp",
        "created_at",
    ]
    available = _player_props_columns()
    if "updated_at" in available:
        columns.append("updated_at")
    if "user_id" in available:
        columns.append("user_id")

    sql = f"""
        SELECT
          {", ".join(columns)}
        FROM mlb.player_props
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([lim, off])
    return pg_fetchall(sql, tuple(params))


def count_prop_history_rows(
    *,
    user_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = None,
    prop_source_prefix: Optional[str] = None,
    status: Optional[str] = None,
) -> int:
    where_sql, params = _build_prop_history_where(
        user_id=user_id,
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        prop_source_prefix=prop_source_prefix,
        status=status,
    )
    sql = f"""
        SELECT COUNT(*) AS total
        FROM mlb.player_props
        WHERE {where_sql}
    """
    row = pg_fetchone(sql, tuple(params))
    total = (row or {}).get("total", 0)
    try:
        return int(total)
    except Exception:
        return 0


def fetch_model_training_prop_history_rows(
    *,
    limit: int = 50,
    offset: int = 0,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = "mlb_api",
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Read current model-backed MLB prop history for dashboard context.

    This intentionally does not read mlb.player_props, which is the legacy
    user-added/current-prop table and is no longer refreshed for broad MLB data.
    """
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    where_sql, params = _build_model_training_history_where(
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    sql = f"""
        SELECT
          CAST(id AS TEXT) AS id,
          player_id,
          player_name,
          team,
          team_id,
          game_id,
          game_date,
          prop_type,
          COALESCE(prop_value, line) AS prop_value,
          over_under,
          status,
          outcome,
          prop_source,
          confidence_score,
          predicted_outcome,
          prediction_timestamp,
          created_at,
          updated_at
        FROM mlb.model_training_props
        WHERE {where_sql}
        ORDER BY game_date DESC NULLS LAST, created_at DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    params.extend([lim, off])
    return pg_fetchall(sql, tuple(params))


def count_model_training_prop_history_rows(
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = "mlb_api",
    status: Optional[str] = None,
) -> int:
    where_sql, params = _build_model_training_history_where(
        from_date=from_date,
        to_date=to_date,
        prop_source=prop_source,
        status=status,
    )
    row = pg_fetchone(
        f"""
        SELECT COUNT(*) AS total
        FROM mlb.model_training_props
        WHERE {where_sql}
        """,
        tuple(params),
    )
    total = (row or {}).get("total", 0)
    try:
        return int(total)
    except Exception:
        return 0


def _build_prop_history_where(
    *,
    user_id: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    prop_source: Optional[str],
    prop_source_prefix: Optional[str],
    status: Optional[str],
) -> Tuple[str, List[Any]]:
    where = ["1=1"]
    params: List[Any] = []

    if from_date:
        where.append("game_date >= %s")
        params.append(from_date)
    if to_date:
        where.append("game_date <= %s")
        params.append(to_date)
    if prop_source:
        where.append("prop_source = %s")
        params.append(prop_source)
    elif prop_source_prefix:
        where.append("prop_source LIKE %s")
        params.append(f"{str(prop_source_prefix).strip()}%")
    if status:
        where.append(
            """
            (
              CASE
                WHEN LOWER(COALESCE(outcome, '')) IN ('win', 'loss', 'push')
                  THEN LOWER(outcome)
                ELSE LOWER(COALESCE(status, 'pending'))
              END
            ) = %s
            """
        )
        params.append(str(status).strip().lower())
    if user_id and _has_user_id_column():
        where.append("CAST(user_id AS TEXT) = %s")
        params.append(str(user_id))
    return " AND ".join(where), params


def _build_model_training_history_where(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    prop_source: Optional[str],
    status: Optional[str],
) -> Tuple[str, List[Any]]:
    where = ["1=1"]
    params: List[Any] = []

    if from_date:
        where.append("game_date >= %s")
        params.append(from_date)
    if to_date:
        where.append("game_date <= %s")
        params.append(to_date)
    if prop_source:
        where.append("prop_source = %s")
        params.append(prop_source)
    if status:
        where.append(
            """
            (
              CASE
                WHEN LOWER(COALESCE(outcome, '')) IN ('win', 'loss', 'push')
                  THEN LOWER(outcome)
                ELSE LOWER(COALESCE(status, 'pending'))
              END
            ) = %s
            """
        )
        params.append(str(status).strip().lower())
    return " AND ".join(where), params
