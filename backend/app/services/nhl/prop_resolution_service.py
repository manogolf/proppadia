"""NHL prop lifecycle resolution helpers (ops-driven)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from backend.shared.db import pg_fetchall, pg_fetchone
from backend.domains.nhl.repository.prop_repository import ensure_user_props_table

ET = ZoneInfo("America/New_York")
_PLAYER_PROPS_COLUMNS_CACHE: Optional[set[str]] = None
_SKATER_GAME_LOG_COLUMNS_CACHE: Optional[set[str]] = None
_GOALIE_GAME_LOG_COLUMNS_CACHE: Optional[set[str]] = None


def _player_props_columns() -> set[str]:
    global _PLAYER_PROPS_COLUMNS_CACHE
    ensure_user_props_table()
    if _PLAYER_PROPS_COLUMNS_CACHE is not None:
        return _PLAYER_PROPS_COLUMNS_CACHE
    rows = pg_fetchall(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='nhl'
          AND table_name='user_props'
        """
    )
    _PLAYER_PROPS_COLUMNS_CACHE = {str(r.get("column_name") or "").strip() for r in rows}
    return _PLAYER_PROPS_COLUMNS_CACHE


def _skater_game_log_columns() -> set[str]:
    global _SKATER_GAME_LOG_COLUMNS_CACHE
    if _SKATER_GAME_LOG_COLUMNS_CACHE is not None:
        return _SKATER_GAME_LOG_COLUMNS_CACHE
    rows = pg_fetchall(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='nhl'
          AND table_name='skater_game_logs_raw'
        """
    )
    _SKATER_GAME_LOG_COLUMNS_CACHE = {str(r.get("column_name") or "").strip() for r in rows}
    return _SKATER_GAME_LOG_COLUMNS_CACHE


def _goalie_game_log_columns() -> set[str]:
    global _GOALIE_GAME_LOG_COLUMNS_CACHE
    if _GOALIE_GAME_LOG_COLUMNS_CACHE is not None:
        return _GOALIE_GAME_LOG_COLUMNS_CACHE
    rows = pg_fetchall(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='nhl'
          AND table_name='goalie_game_logs_raw'
        """
    )
    _GOALIE_GAME_LOG_COLUMNS_CACHE = {str(r.get("column_name") or "").strip() for r in rows}
    return _GOALIE_GAME_LOG_COLUMNS_CACHE


def _normalize_prop_source(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    if not raw.startswith("nhl_"):
        raw = f"nhl_{raw}"
    return raw


def _parse_iso_date(value: Optional[str], label: str) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as e:
        raise ValueError(f"{label} must be YYYY-MM-DD") from e
    return parsed.isoformat()


def _build_where(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    only_past_games: bool,
    today_et: str,
) -> tuple[str, List[Any]]:
    where = [
        "prop_source LIKE %s",
        "LOWER(COALESCE(status, 'pending')) = 'pending'",
        "game_date IS NOT NULL",
    ]
    params: List[Any] = ["nhl_%"]
    if from_date:
        where.append("game_date >= %s")
        params.append(from_date)
    if to_date:
        where.append("game_date <= %s")
        params.append(to_date)
    if only_past_games:
        where.append("game_date < %s")
        params.append(today_et)
    return " AND ".join(where), params


def resolve_nhl_pending_props(
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    dry_run: bool = True,
    only_past_games: bool = True,
    outcome: str = "dnp",
) -> Dict[str, Any]:
    from_date_norm = _parse_iso_date(from_date, "from_date")
    to_date_norm = _parse_iso_date(to_date, "to_date")
    if from_date_norm and to_date_norm and from_date_norm > to_date_norm:
        raise ValueError("from_date must be <= to_date")

    outcome_norm = str(outcome or "dnp").strip().lower()
    if outcome_norm not in {"dnp", "push", "win", "loss"}:
        raise ValueError("outcome must be one of: dnp,push,win,loss")

    today_et = datetime.now(ET).date().isoformat()
    where_sql, params = _build_where(
        from_date=from_date_norm,
        to_date=to_date_norm,
        only_past_games=bool(only_past_games),
        today_et=today_et,
    )

    preview_sql = f"""
        SELECT
          COUNT(*)::int AS pending_count,
          MIN(game_date)::text AS min_game_date,
          MAX(game_date)::text AS max_game_date
        FROM nhl.user_props
        WHERE {where_sql}
    """
    preview = pg_fetchone(preview_sql, tuple(params)) or {}
    pending_count = int(preview.get("pending_count") or 0)

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "matched": pending_count,
            "updated": 0,
            "from_date": from_date_norm,
            "to_date": to_date_norm,
            "only_past_games": bool(only_past_games),
            "outcome": outcome_norm,
            "range": {
                "min_game_date": preview.get("min_game_date"),
                "max_game_date": preview.get("max_game_date"),
                "today_et": today_et,
            },
        }

    set_clauses = ["status = %s", "outcome = %s"]
    update_params: List[Any] = [outcome_norm, outcome_norm]
    columns = _player_props_columns()
    if "updated_at" in columns:
        set_clauses.append("updated_at = NOW()")

    update_sql = f"""
        WITH targets AS (
            SELECT id
            FROM nhl.user_props
            WHERE {where_sql}
        ),
        updated AS (
            UPDATE nhl.user_props p
            SET {", ".join(set_clauses)}
            WHERE p.id IN (SELECT id FROM targets)
            RETURNING p.id
        )
        SELECT
            (SELECT COUNT(*)::int FROM targets) AS matched_count,
            (SELECT COUNT(*)::int FROM updated) AS updated_count
    """
    row = pg_fetchone(update_sql, tuple(update_params + params)) or {}
    return {
        "ok": True,
        "dry_run": False,
        "matched": int(row.get("matched_count") or 0),
        "updated": int(row.get("updated_count") or 0),
        "from_date": from_date_norm,
        "to_date": to_date_norm,
        "only_past_games": bool(only_past_games),
        "outcome": outcome_norm,
        "range": {
            "min_game_date": preview.get("min_game_date"),
            "max_game_date": preview.get("max_game_date"),
            "today_et": today_et,
        },
    }


def grade_nhl_pending_props_from_logs(
    *,
    user_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prop_source: Optional[str] = None,
    only_past_games: bool = True,
    dnp_after_days: int = 2,
) -> Dict[str, Any]:
    from_date_norm = _parse_iso_date(from_date, "from_date")
    to_date_norm = _parse_iso_date(to_date, "to_date")
    if from_date_norm and to_date_norm and from_date_norm > to_date_norm:
        raise ValueError("from_date must be <= to_date")

    today_date = datetime.now(ET).date()
    today_et = today_date.isoformat()
    dnp_days = max(0, int(dnp_after_days))
    dnp_cutoff_date = (today_date - timedelta(days=dnp_days)).isoformat()
    source_norm = _normalize_prop_source(prop_source)
    user_id_norm = str(user_id or "").strip() or None

    where = [
        "LOWER(COALESCE(p.status, 'pending')) = 'pending'",
        "p.game_date IS NOT NULL",
    ]
    params: List[Any] = []
    if from_date_norm:
        where.append("p.game_date >= %s")
        params.append(from_date_norm)
    if to_date_norm:
        where.append("p.game_date <= %s")
        params.append(to_date_norm)
    if only_past_games:
        where.append("p.game_date < %s")
        params.append(today_et)

    user_prop_cols = _player_props_columns()
    if user_id_norm and "user_id" in user_prop_cols:
        where.append("CAST(p.user_id AS TEXT) = %s")
        params.append(user_id_norm)

    if source_norm:
        where.append("LOWER(COALESCE(p.prop_source, '')) = %s")
        params.append(source_norm)
    else:
        where.append("LOWER(COALESCE(p.prop_source, '')) LIKE 'nhl_%'")

    sk_cols = _skater_game_log_columns()
    goalie_cols = _goalie_game_log_columns()

    shots_expr = "sgr.shots_on_goal::float8" if "shots_on_goal" in sk_cols else "NULL::float8"
    saves_expr = "ggr.saves::float8" if "saves" in goalie_cols else "NULL::float8"

    if {"points", "goals", "assists"}.issubset(sk_cols):
        points_expr = "COALESCE(sgr.points::float8, (COALESCE(sgr.goals,0)+COALESCE(sgr.assists,0))::float8)"
    elif "points" in sk_cols:
        points_expr = "sgr.points::float8"
    elif {"goals", "assists"}.issubset(sk_cols):
        points_expr = "(COALESCE(sgr.goals,0)+COALESCE(sgr.assists,0))::float8"
    else:
        points_expr = "NULL::float8"

    set_clauses = ["status = g.outcome", "outcome = g.outcome"]
    if "updated_at" in user_prop_cols:
        set_clauses.append("updated_at = NOW()")

    sql = f"""
        WITH candidates AS (
          SELECT
            p.id,
            p.player_id,
            p.game_id,
            p.game_date,
            LOWER(COALESCE(p.prop_type, '')) AS prop_type,
            LOWER(COALESCE(p.over_under, 'over')) AS over_under,
            p.prop_value::float8 AS prop_value
          FROM nhl.user_props p
          WHERE {' AND '.join(where)}
        ),
        actuals AS (
          SELECT
            c.id,
            CASE
              WHEN c.prop_type = 'shots_on_goal' THEN {shots_expr}
              WHEN c.prop_type = 'goalie_saves' THEN {saves_expr}
              WHEN c.prop_type = 'points' THEN {points_expr}
              ELSE NULL::float8
            END AS actual_value
          FROM candidates c
          LEFT JOIN nhl.skater_game_logs_raw sgr
            ON sgr.player_id = c.player_id
           AND sgr.game_id = c.game_id
          LEFT JOIN nhl.goalie_game_logs_raw ggr
            ON ggr.player_id = c.player_id
           AND ggr.game_id = c.game_id
        ),
        graded AS (
          SELECT
            c.id,
            CASE
              WHEN a.actual_value IS NULL THEN NULL::text
              WHEN c.over_under = 'under' THEN
                CASE
                  WHEN a.actual_value < c.prop_value THEN 'win'
                  WHEN a.actual_value = c.prop_value THEN 'push'
                  ELSE 'loss'
                END
              ELSE
                CASE
                  WHEN a.actual_value > c.prop_value THEN 'win'
                  WHEN a.actual_value = c.prop_value THEN 'push'
                  ELSE 'loss'
                END
            END AS outcome
          FROM candidates c
          JOIN actuals a
            ON a.id = c.id
          WHERE a.actual_value IS NOT NULL
        ),
        graded_updated AS (
          UPDATE nhl.user_props p
          SET {', '.join(set_clauses)}
          FROM graded g
          WHERE p.id = g.id
            AND g.outcome IS NOT NULL
          RETURNING g.outcome
        ),
        dnp_candidates AS (
          SELECT c.id
          FROM candidates c
          LEFT JOIN actuals a
            ON a.id = c.id
          WHERE a.actual_value IS NULL
            AND c.game_date <= %s::date
        ),
        dnp_updated AS (
          UPDATE nhl.user_props p
          SET status = 'dnp',
              outcome = 'dnp'
              {", updated_at = NOW()" if "updated_at" in user_prop_cols else ""}
          WHERE p.id IN (SELECT id FROM dnp_candidates)
          RETURNING p.id
        )
        SELECT
          (SELECT COUNT(*)::int FROM candidates) AS pending_candidates,
          (SELECT COUNT(*)::int FROM graded) AS graded_candidates,
          (SELECT COUNT(*)::int FROM graded_updated) AS graded_updated_count,
          (SELECT COUNT(*)::int FROM dnp_candidates) AS dnp_candidates,
          (SELECT COUNT(*)::int FROM dnp_updated) AS dnp_updated_count,
          COUNT(*) FILTER (WHERE outcome = 'win')::int AS wins,
          COUNT(*) FILTER (WHERE outcome = 'loss')::int AS losses,
          COUNT(*) FILTER (WHERE outcome = 'push')::int AS pushes
        FROM graded_updated
    """
    row = pg_fetchone(sql, tuple(params + [dnp_cutoff_date])) or {}
    graded_updated_count = int(row.get("graded_updated_count") or 0)
    dnp_updated_count = int(row.get("dnp_updated_count") or 0)
    return {
        "ok": True,
        "pending_candidates": int(row.get("pending_candidates") or 0),
        "graded_candidates": int(row.get("graded_candidates") or 0),
        "dnp_candidates": int(row.get("dnp_candidates") or 0),
        "graded_updated": graded_updated_count,
        "dnp_updated": dnp_updated_count,
        "updated": graded_updated_count + dnp_updated_count,
        "wins": int(row.get("wins") or 0),
        "losses": int(row.get("losses") or 0),
        "pushes": int(row.get("pushes") or 0),
        "from_date": from_date_norm,
        "to_date": to_date_norm,
        "only_past_games": bool(only_past_games),
        "today_et": today_et,
        "dnp_after_days": dnp_days,
        "dnp_cutoff_date": dnp_cutoff_date,
        "prop_source": source_norm,
        "user_id": user_id_norm,
    }
