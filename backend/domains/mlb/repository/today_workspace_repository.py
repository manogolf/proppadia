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
    where.append(
        """
        abs(w.best_price) <= 500
        AND w.market_median IS NOT NULL
        AND abs(w.market_median) <= 500
        AND EXISTS (
          SELECT 1
          FROM mlb.today_workspace_mlb w_over
          WHERE w_over.game_date = w.game_date
            AND w_over.game_id = w.game_id
            AND w_over.player_id = w.player_id
            AND lower(trim(w_over.prop_type)) = lower(trim(w.prop_type))
            AND w_over.line = w.line
            AND upper(trim(w_over.side)) = 'OVER'
            AND w_over.best_price IS NOT NULL
            AND abs(w_over.best_price) <= 500
            AND w_over.market_median IS NOT NULL
            AND abs(w_over.market_median) <= 500
        )
        AND EXISTS (
          SELECT 1
          FROM mlb.today_workspace_mlb w_under
          WHERE w_under.game_date = w.game_date
            AND w_under.game_id = w.game_id
            AND w_under.player_id = w.player_id
            AND lower(trim(w_under.prop_type)) = lower(trim(w.prop_type))
            AND w_under.line = w.line
            AND upper(trim(w_under.side)) = 'UNDER'
            AND w_under.best_price IS NOT NULL
            AND abs(w_under.best_price) <= 500
            AND w_under.market_median IS NOT NULL
            AND abs(w_under.market_median) <= 500
        )
        """
    )

    if slate_date:
        where.append("w.game_date = %s::date")
        params.append(str(slate_date).strip())
    if prop_type:
        where.append("lower(trim(w.prop_type)) = lower(trim(%s))")
        params.append(str(prop_type).strip())
    if team:
        where.append("upper(trim(w.team)) = upper(trim(%s))")
        params.append(str(team).strip())
    if side:
        where.append("upper(trim(w.side)) = upper(trim(%s))")
        params.append(str(side).strip())
    if timing_signal:
        where.append("upper(trim(w.timing_signal)) = upper(trim(%s))")
        params.append(str(timing_signal).strip())
    if player_id is not None:
        where.append("CAST(w.player_id AS TEXT) = %s")
        params.append(str(player_id))
    if player_query:
        where.append("w.player_name ILIKE %s")
        params.append(f"%{str(player_query).strip()}%")

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
    WITH scored AS (
      SELECT
        w.*,
        LEAST(
          100,
          GREATEST(
            0,
            50
            + CASE upper(trim(coalesce(w.timing_signal, '')))
                WHEN 'STABLE' THEN 18
                WHEN 'EARLY' THEN 10
                WHEN 'LOW CONFIDENCE' THEN -12
                WHEN 'WAIT' THEN -12
                WHEN 'VOLATILE' THEN -20
                ELSE 0
              END
            + CASE upper(trim(coalesce(w.coverage_quality_label, '')))
                WHEN 'STRONG' THEN 18
                WHEN 'GOOD' THEN 10
                WHEN 'LIMITED' THEN -14
                WHEN 'THIN' THEN -20
                WHEN 'UNRELIABLE' THEN -25
                ELSE 0
              END
            + CASE
                WHEN w.market_range IS NULL THEN -8
                WHEN w.market_range <= 20 THEN 12
                WHEN w.market_range <= 40 THEN 8
                WHEN w.market_range <= 80 THEN 2
                WHEN w.market_range <= 120 THEN -8
                ELSE -18
              END
            + CASE
                WHEN coalesce(w.book_count, 0) >= 5 THEN 10
                WHEN coalesce(w.book_count, 0) >= 4 THEN 7
                WHEN coalesce(w.book_count, 0) >= 3 THEN 4
                WHEN coalesce(w.book_count, 0) >= 2 THEN 0
                ELSE -12
              END
          )
        )::int AS signal_quality_score
      FROM mlb.today_workspace_mlb w
      {where_sql}
    ),
    quality AS (
      SELECT
        scored.*,
        CASE
          WHEN signal_quality_score >= 75 THEN 'HIGH'
          WHEN signal_quality_score >= 50 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS signal_quality_tier
      FROM scored
    )
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
      CASE
        WHEN upper(trim(coalesce(timing_signal, ''))) = 'WAIT' THEN 'LOW CONFIDENCE'
        ELSE timing_signal
      END AS timing_signal,
      timing_reason,
      decision_label AS market_signal_label,
      decision_reason AS market_signal_reason,
      CASE
        WHEN signal_quality_tier = 'HIGH'
          AND upper(trim(coalesce(timing_signal, ''))) = 'STABLE'
          AND abs(coalesce(value_vs_market, 0)) >= 100
          THEN 'ACTIONABLE'
        WHEN signal_quality_tier = 'HIGH'
          AND upper(trim(coalesce(timing_signal, ''))) = 'EARLY'
          THEN 'MONITOR'
        WHEN signal_quality_tier = 'MEDIUM'
          AND upper(trim(coalesce(timing_signal, ''))) = 'STABLE'
          THEN 'CONSIDER'
        ELSE 'IGNORE'
      END AS decision_label,
      CASE
        WHEN signal_quality_tier = 'HIGH'
          AND upper(trim(coalesce(timing_signal, ''))) = 'STABLE'
          AND abs(coalesce(value_vs_market, 0)) >= 100
          THEN 'High-quality stable signal with a large market gap.'
        WHEN signal_quality_tier = 'HIGH'
          AND upper(trim(coalesce(timing_signal, ''))) = 'EARLY'
          THEN 'High-quality signal, but the market is still early.'
        WHEN signal_quality_tier = 'MEDIUM'
          AND upper(trim(coalesce(timing_signal, ''))) = 'STABLE'
          THEN 'Stable signal with medium market quality.'
        ELSE 'Signal quality, timing, or market gap does not clear the decision screen.'
      END AS decision_reason,
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
      signal_quality_score,
      signal_quality_tier,
      COUNT(*) OVER()::int AS total_rows
    FROM quality
    ORDER BY CASE
        WHEN signal_quality_tier = 'HIGH'
          AND upper(trim(coalesce(timing_signal, ''))) = 'STABLE'
          AND abs(coalesce(value_vs_market, 0)) >= 100
          THEN 1
        WHEN signal_quality_tier = 'HIGH'
          AND upper(trim(coalesce(timing_signal, ''))) = 'EARLY'
          THEN 2
        WHEN signal_quality_tier = 'MEDIUM'
          AND upper(trim(coalesce(timing_signal, ''))) = 'STABLE'
          THEN 3
        ELSE 4
      END ASC,
      signal_quality_score DESC NULLS LAST,
      abs(value_vs_market) DESC NULLS LAST,
      player_name ASC,
      prop_type ASC,
      line ASC,
      side ASC
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
