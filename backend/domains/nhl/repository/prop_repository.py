"""NHL user prop persistence queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from backend.shared.db import pg_connect, pg_execute, pg_fetchall, pg_fetchone


class DuplicatePropError(Exception):
    """Raised when DB unique constraints indicate a duplicate prop insert."""


_user_props_columns_cache: Optional[Set[str]] = None
_user_props_ensured: bool = False


def ensure_user_props_table() -> None:
    global _user_props_ensured, _user_props_columns_cache
    if _user_props_ensured:
        return

    statements = [
        """
        CREATE TABLE IF NOT EXISTS nhl.user_props (
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            prediction_timestamp timestamptz NOT NULL DEFAULT NOW(),
            game_id bigint NOT NULL,
            game_date date,
            player_id bigint NOT NULL REFERENCES nhl.players(player_id),
            player_name text,
            team text,
            team_id bigint REFERENCES nhl.teams(team_id),
            opponent_id bigint REFERENCES nhl.teams(team_id),
            prop_type text NOT NULL,
            prop_value numeric(6,2) NOT NULL,
            over_under text NOT NULL,
            status text NOT NULL DEFAULT 'pending',
            outcome text,
            prop_source text NOT NULL DEFAULT 'nhl_user_added',
            predicted_outcome text,
            confidence_score double precision,
            user_id text,
            CONSTRAINT nhl_user_props_over_under_check
                CHECK (over_under = ANY (ARRAY['over'::text, 'under'::text])),
            CONSTRAINT nhl_user_props_prop_type_check
                CHECK (prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text, 'points'::text]))
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS nhl_user_props_lookup_idx
            ON nhl.user_props (game_id, player_id, prop_type)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS nhl_user_props_unique_prop_idx
            ON nhl.user_props (game_id, player_id, prop_type, over_under, prop_value, prop_source)
        """,
    ]
    with pg_connect() as conn, conn.cursor() as cur:
        for sql in statements:
            cur.execute(sql)
        conn.commit()
    _user_props_ensured = True
    _user_props_columns_cache = None


def _user_props_columns() -> Set[str]:
    global _user_props_columns_cache
    ensure_user_props_table()
    if _user_props_columns_cache is not None:
        return _user_props_columns_cache
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='nhl'
          AND table_name='user_props'
    """
    rows = pg_fetchall(sql)
    _user_props_columns_cache = {str(r.get("column_name") or "").strip() for r in rows}
    return _user_props_columns_cache


def find_duplicate_prop_id(
    *,
    player_id: int,
    game_id: int,
    prop_type: str,
    over_under: str,
    prop_value: float,
    prop_source: str,
) -> Optional[str]:
    ensure_user_props_table()
    sql = """
        SELECT id
        FROM nhl.user_props
        WHERE player_id = %s
          AND game_id = %s
          AND prop_type = %s
          AND over_under = %s
          AND prop_value = %s
          AND prop_source = %s
        LIMIT 1
    """
    row = pg_fetchone(
        sql,
        (
            int(player_id),
            int(game_id),
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
    opponent_id: Optional[int] = None,
    user_id: Optional[str] = None,
) -> None:
    ensure_user_props_table()
    columns = _user_props_columns()
    insert_cols = ["player_id", "game_id", "prop_type", "prop_value", "over_under", "prop_source"]
    values: List[Any] = [int(player_id), int(game_id), prop_type, prop_value, over_under, prop_source]
    placeholders = ["%s"] * len(values)

    optional_pairs = [
        ("player_name", player_name),
        ("team", team),
        ("team_id", int(team_id) if team_id is not None else None),
        ("opponent_id", int(opponent_id) if opponent_id is not None else None),
        ("game_date", game_date),
        ("status", "pending"),
        ("predicted_outcome", recommendation),
        ("confidence_score", probability),
        ("user_id", str(user_id) if user_id is not None else None),
    ]
    for col, value in optional_pairs:
        if col in columns:
            insert_cols.append(col)
            placeholders.append("%s")
            values.append(value)

    if "prediction_timestamp" in columns:
        insert_cols.append("prediction_timestamp")
        placeholders.append("NOW()")
    if "created_at" in columns:
        insert_cols.append("created_at")
        placeholders.append("NOW()")
    if "updated_at" in columns:
        insert_cols.append("updated_at")
        placeholders.append("NOW()")

    sql = f"""
        INSERT INTO nhl.user_props ({", ".join(insert_cols)})
        VALUES ({", ".join(placeholders)})
    """
    try:
        pg_execute(sql, tuple(values))
    except Exception as e:
        if getattr(e, "sqlstate", None) == "23505":
            raise DuplicatePropError(str(e)) from e
        raise


def delete_prop_row(
    *,
    prop_id: str,
    user_id: Optional[str] = None,
) -> int:
    ensure_user_props_table()
    where = ["id::text = %s"]
    params: List[Any] = [str(prop_id)]
    columns = _user_props_columns()
    if user_id and "user_id" in columns:
        where.append("CAST(user_id AS TEXT) = %s")
        params.append(str(user_id))
    sql = f"""
        DELETE FROM nhl.user_props
        WHERE {" AND ".join(where)}
    """
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        deleted = int(cur.rowcount or 0)
        conn.commit()
    return deleted


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
    ensure_user_props_table()
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
    columns = _user_props_columns()
    player_name_expr = (
        "COALESCE(p.player_name, pl.full_name) AS player_name"
        if "player_name" in columns
        else "pl.full_name AS player_name"
    )
    team_expr = (
        "COALESCE(p.team, tt.team, tp.team) AS team"
        if "team" in columns
        else "COALESCE(tt.team, tp.team) AS team"
    )
    game_date_expr = (
        "COALESCE(p.game_date, g.game_date) AS game_date"
        if "game_date" in columns
        else "g.game_date AS game_date"
    )
    selected = [
        "p.id",
        "p.player_id",
        player_name_expr,
        team_expr,
        "p.team_id",
        "p.game_id",
        game_date_expr,
        "p.prop_type",
        "p.prop_value",
        "p.over_under",
        "p.status" if "status" in columns else "NULL::text AS status",
        "p.outcome" if "outcome" in columns else "NULL::text AS outcome",
        "p.prop_source" if "prop_source" in columns else "NULL::text AS prop_source",
        "p.confidence_score" if "confidence_score" in columns else "NULL::float8 AS confidence_score",
        "p.predicted_outcome" if "predicted_outcome" in columns else "NULL::text AS predicted_outcome",
        "p.prediction_timestamp" if "prediction_timestamp" in columns else "NULL::timestamptz AS prediction_timestamp",
        "p.created_at" if "created_at" in columns else "NULL::timestamptz AS created_at",
        "p.updated_at" if "updated_at" in columns else "NULL::timestamptz AS updated_at",
        "p.user_id" if "user_id" in columns else "NULL::text AS user_id",
    ]
    sql = f"""
        SELECT
          {", ".join(selected)}
        FROM nhl.user_props p
        LEFT JOIN nhl.players pl
          ON pl.player_id = p.player_id
        LEFT JOIN nhl.teams tt
          ON tt.team_id = p.team_id
        LEFT JOIN nhl.players pl2
          ON pl2.player_id = p.player_id
        LEFT JOIN nhl.teams tp
          ON tp.team_id = pl2.current_team_id
        LEFT JOIN nhl.games g
          ON g.game_id = p.game_id
        WHERE {where_sql}
        ORDER BY COALESCE(p.created_at, p.prediction_timestamp) DESC NULLS LAST, p.id DESC
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
    ensure_user_props_table()
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
        FROM nhl.user_props
        WHERE {where_sql}
    """
    row = pg_fetchone(sql, tuple(params))
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
    columns = _user_props_columns()

    if from_date:
        where.append("COALESCE(game_date, CURRENT_DATE) >= %s")
        params.append(from_date)
    if to_date:
        where.append("COALESCE(game_date, CURRENT_DATE) <= %s")
        params.append(to_date)
    if prop_source:
        where.append("prop_source = %s")
        params.append(prop_source)
    elif prop_source_prefix:
        where.append("prop_source LIKE %s")
        params.append(f"{str(prop_source_prefix).strip()}%")
    if status:
        if "outcome" in columns and "status" in columns:
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
        elif "status" in columns:
            where.append("LOWER(COALESCE(status, 'pending')) = %s")
        else:
            where.append("'pending' = %s")
        params.append(str(status).strip().lower())
    if user_id and "user_id" in columns:
        where.append("CAST(user_id AS TEXT) = %s")
        params.append(str(user_id))
    return " AND ".join(where), params
