"""Repository queries for MLB /today workspace."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.shared.db import pg_fetchall, pg_fetchone


def fetch_today_workspace_rows(
    *,
    slate_date: Optional[str] = None,
    prop_type: Optional[str] = None,
    team: Optional[str] = None,
    side: Optional[str] = None,
    timing_signal: Optional[str] = None,
    player_id: Optional[int] = None,
    player_query: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    lim = max(1, min(int(limit), 5000))
    off = max(0, int(offset))

    where = []
    params: List[Any] = []

    if slate_date:
        where.append("game_date = %s::date")
        params.append(str(slate_date).strip())
    if prop_type:
        where.append("lower(trim(prop_type)) = lower(trim(%s))")
        params.append(str(prop_type).strip())
    if team:
        where.append("upper(trim(team)) = upper(trim(%s))")
        params.append(str(team).strip())
    if side:
        where.append("upper(trim(side)) = upper(trim(%s))")
        params.append(str(side).strip())
    if timing_signal:
        where.append("upper(trim(timing_signal)) = upper(trim(%s))")
        params.append(str(timing_signal).strip())
    if player_id is not None:
        where.append("CAST(player_id AS TEXT) = %s")
        params.append(str(player_id))
    if player_query:
        where.append("player_name ILIKE %s")
        params.append(f"%{str(player_query).strip()}%")

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
    SELECT
      player_id,
      player_name,
      team,
      opponent,
      game_id,
      prop_type,
      line,
      side,
      best_price,
      best_price_book,
      market_median,
      market_range,
      value_vs_market,
      coverage_quality_label,
      coverage_quality_reason,
      timing_signal,
      timing_reason,
      decision_label,
      decision_reason,
      streak_context_label,
      streak_count,
      baseline_delta,
      consistency_score,
      hit_rate_last_5,
      hit_rate_last_10,
      hit_rate_season,
      open_price,
      latest_price,
      num_snapshots,
      price_change_from_open,
      book_count,
      price_dispersion,
      last_5_avg,
      last_10_avg,
      season_avg,
      COUNT(*) OVER()::int AS total_rows
    FROM mlb.today_workspace_mlb
    {where_sql}
    ORDER BY abs(value_vs_market) DESC NULLS LAST, player_name ASC, prop_type ASC, line ASC, side ASC
    LIMIT %s::int
    OFFSET %s::int
    """
    params.extend([lim, off])
    return pg_fetchall(sql, tuple(params))


def fetch_today_workspace_last_updated(*, slate_date: str) -> Optional[Any]:
    row = pg_fetchone(
        """
        SELECT coalesce(
          (
            SELECT max(ms.last_snapshot_ts)
            FROM mlb.today_market_snapshot ms
            WHERE ms.game_date = %s::date
          ),
          (
            SELECT max(o.snapshot_ts)
            FROM mlb.today_odds_book_rows o
            WHERE o.slate_date = %s::date
          )
        ) AS last_updated
        """,
        (slate_date, slate_date),
    )
    if not row:
        return None
    return row.get("last_updated")


def fetch_today_prop_availability(
    *,
    slate_date: str,
    player_id: int,
    prop_type: str,
) -> Dict[str, Any]:
    row = pg_fetchone(
        """
        WITH params AS (
          SELECT
            %s::date AS slate_date,
            %s::bigint AS player_id,
            lower(trim(%s)) AS prop_type
        )
        SELECT
          EXISTS(
            SELECT 1
            FROM mlb.today_odds_book_rows o, params p
            WHERE o.slate_date = p.slate_date
              AND o.player_id = p.player_id
              AND lower(trim(o.prop_type)) = p.prop_type
          ) AS exists_in_odds,
          EXISTS(
            SELECT 1
            FROM mlb.today_workspace_mlb w, params p
            WHERE w.game_date = p.slate_date
              AND w.player_id = p.player_id
              AND lower(trim(w.prop_type)) = p.prop_type
          ) AS exists_in_workspace,
          (
            SELECT count(*)::int
            FROM mlb.today_odds_book_rows o, params p
            WHERE o.slate_date = p.slate_date
              AND o.player_id = p.player_id
              AND lower(trim(o.prop_type)) = p.prop_type
          ) AS odds_rows,
          (
            SELECT count(*)::int
            FROM mlb.today_workspace_mlb w, params p
            WHERE w.game_date = p.slate_date
              AND w.player_id = p.player_id
              AND lower(trim(w.prop_type)) = p.prop_type
          ) AS workspace_rows
        """,
        (str(slate_date).strip(), int(player_id), str(prop_type).strip()),
    ) or {}
    return {
        "exists_in_odds": bool(row.get("exists_in_odds")),
        "exists_in_workspace": bool(row.get("exists_in_workspace")),
        "odds_rows": int(row.get("odds_rows") or 0),
        "workspace_rows": int(row.get("workspace_rows") or 0),
    }
