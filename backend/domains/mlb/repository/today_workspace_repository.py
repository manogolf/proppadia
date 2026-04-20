"""Repository queries for MLB /today workspace."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.shared.db import pg_fetchall


def fetch_today_workspace_rows(
    *,
    prop_type: Optional[str] = None,
    team: Optional[str] = None,
    timing_signal: Optional[str] = None,
    player_query: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    lim = max(1, min(int(limit), 5000))
    off = max(0, int(offset))

    where = []
    params: List[Any] = []

    if prop_type:
        where.append("lower(trim(prop_type)) = lower(trim(%s))")
        params.append(str(prop_type).strip())
    if team:
        where.append("upper(trim(team)) = upper(trim(%s))")
        params.append(str(team).strip())
    if timing_signal:
        where.append("upper(trim(timing_signal)) = upper(trim(%s))")
        params.append(str(timing_signal).strip())
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
      best_price,
      market_median,
      value_vs_market,
      timing_signal,
      timing_reason,
      streak_context_label,
      streak_count,
      baseline_delta,
      consistency_score,
      hit_rate_last_5,
      hit_rate_last_10,
      hit_rate_season,
      open_over_price,
      latest_over_price,
      num_snapshots,
      over_price_change_from_open,
      last_5_avg,
      last_10_avg,
      season_avg,
      COUNT(*) OVER()::int AS total_rows
    FROM mlb.today_workspace_mlb
    {where_sql}
    ORDER BY abs(value_vs_market) DESC NULLS LAST, player_name ASC, prop_type ASC, line ASC
    LIMIT %s::int
    OFFSET %s::int
    """
    params.extend([lim, off])
    return pg_fetchall(sql, tuple(params))
