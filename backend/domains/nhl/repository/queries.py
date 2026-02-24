from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.shared.db import pg_fetchall


def _resolve_target_date(date: Optional[str]):
    if date:
        try:
            return datetime.fromisoformat(date).date(), None
        except ValueError:
            return None, "invalid date format; expected YYYY-MM-DD"
    return datetime.now(ZoneInfo("America/New_York")).date(), None


def fetch_games_today(date: Optional[str], limit: int, offset: int) -> Dict[str, Any]:
    target_date, err = _resolve_target_date(date)
    if err:
        return {"ok": False, "error": err}

    sql = """
        SELECT
            g.game_id,
            g.game_date,
            g.start_time_utc,
            g.status,
            g.season,
            g.game_type,
            g.home_team_id,
            g.away_team_id,
            g.home_team_code AS home_abbr,
            g.away_team_code AS away_abbr
        FROM nhl.games g
        WHERE g.game_date = %s
        ORDER BY g.start_time_utc NULLS LAST, g.game_id
        LIMIT %s OFFSET %s
    """
    try:
        rows = pg_fetchall(sql, (target_date, limit, offset))
        return {"ok": True, "date": str(target_date), "count": len(rows), "rows": rows}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def fetch_props_today(date: Optional[str], limit: int, offset: int) -> Dict[str, Any]:
    target_date, err = _resolve_target_date(date)
    if err:
        return {"ok": False, "error": err}

    sql = """
        SELECT
            p.prediction_id, p.player_id, p.game_id, p.prop, p.line, p.p_over,
            p.model_version, p.created_at
        FROM nhl.predictions p
        JOIN nhl.games g ON g.game_id = p.game_id
        WHERE g.game_date = %s
        ORDER BY p.created_at DESC
        LIMIT %s OFFSET %s
    """
    try:
        rows = pg_fetchall(sql, (target_date, limit, offset))
        return {"ok": True, "date": str(target_date), "count": len(rows), "rows": rows}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def fetch_sog(date: Optional[str], limit: int, offset: int):
    sql = """
    WITH base AS (
    SELECT
        p.player_id,
        p.game_id,
        p.line,
        p.p_over
    FROM nhl.predictions p
    WHERE p.prop = 'shots_on_goal'
        AND p.model_family = 'denali_blend'
        AND p.model_version = 'phoenix_v2'
        AND p.feature_hash = 'phoenix_v2'
    ),
    joined AS (
    SELECT
        b.player_id,
        pl.full_name AS player_name,
        b.game_id,
        gm.game_date,
        COALESCE(s.team_id, pl.current_team_id) AS team_id,
        t.team AS team_abbr,
        t.full_team_name,
        b.line,
        b.p_over
    FROM base b
    JOIN nhl.games gm
        ON gm.game_id = b.game_id
    LEFT JOIN nhl.skater_game_logs_raw s
        ON s.player_id = b.player_id
    AND s.game_id   = b.game_id
    LEFT JOIN nhl.players pl
        ON pl.player_id = b.player_id
    LEFT JOIN nhl.teams t
        ON t.team_id = COALESCE(s.team_id, pl.current_team_id)
    WHERE (%s::date IS NULL OR gm.game_date = %s::date)
    )
    SELECT
    player_id,
    player_name,
    game_id,
    game_date,
    team_id,
    team_abbr,
    full_team_name,
    MAX(p_over) FILTER (WHERE line = 1.5) AS p_over_1_5,
    MAX(p_over) FILTER (WHERE line = 2.5) AS p_over_2_5,
    MAX(p_over) FILTER (WHERE line = 3.5) AS p_over_3_5
    FROM joined
    GROUP BY player_id, player_name, game_id, game_date, team_id, team_abbr, full_team_name
    ORDER BY game_id, team_abbr NULLS LAST, player_name NULLS LAST
    LIMIT %s OFFSET %s;
    """
    try:
        return pg_fetchall(sql, (date, date, limit, offset))
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def fetch_saves(date: Optional[str], limit: int, offset: int):
    sql = """
    WITH base AS (
      SELECT
        p.player_id,
        p.game_id,
        p.line,
        p.p_over
      FROM nhl.predictions p
      WHERE p.prop = 'goalie_saves'
        AND p.line IN (18.5, 19.5, 20.5, 21.5, 22.5, 23.5)
    ),
    joined AS (
      SELECT
        b.player_id,
        pl.full_name AS player_name,
        b.game_id,
        gm.game_date,
        COALESCE(gl.team_id, pl.current_team_id) AS team_id,
        t.team AS team_abbr,
        t.full_team_name,
        b.line,
        b.p_over
      FROM base b
      JOIN nhl.games gm
        ON gm.game_id = b.game_id
      LEFT JOIN nhl.goalie_game_logs_raw gl
        ON gl.player_id = b.player_id
       AND gl.game_id   = b.game_id
      LEFT JOIN nhl.players pl
        ON pl.player_id = b.player_id
      LEFT JOIN nhl.teams t
        ON t.team_id = COALESCE(gl.team_id, pl.current_team_id)
      WHERE (%s::date IS NULL OR gm.game_date = %s::date)
    )
    SELECT
      player_id,
      player_name,
      game_id,
      game_date,
      team_id,
      team_abbr,
      full_team_name,
      MAX(p_over) FILTER (WHERE line = 18.5) AS p_over_18_5,
      MAX(p_over) FILTER (WHERE line = 19.5) AS p_over_19_5,
      MAX(p_over) FILTER (WHERE line = 20.5) AS p_over_20_5,
      MAX(p_over) FILTER (WHERE line = 21.5) AS p_over_21_5,
      MAX(p_over) FILTER (WHERE line = 22.5) AS p_over_22_5,
      MAX(p_over) FILTER (WHERE line = 23.5) AS p_over_23_5
    FROM joined
    GROUP BY player_id, player_name, game_id, game_date, team_id, team_abbr, full_team_name
    ORDER BY game_id, team_abbr NULLS LAST, player_name NULLS LAST
    LIMIT %s OFFSET %s;
    """
    try:
        return pg_fetchall(sql, (date, date, limit, offset))
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def fetch_sog_streaks(
    date: Optional[str],
    lookback_days: int,
    window_games: int,
    min_streak: int,
    top_n: int,
) -> Dict[str, Any]:
    target_date, err = _resolve_target_date(date)
    if err:
        return {"ok": False, "error": err}

    from_date = target_date - timedelta(days=lookback_days)

    sql = """
        WITH pred_base AS (
            SELECT
                p.player_id,
                p.game_id,
                p.line::float8 AS line_value,
                p.p_over::float8 AS p_over
            FROM nhl.predictions p
            JOIN nhl.games g
              ON g.game_id = p.game_id
            WHERE p.prop = 'shots_on_goal'
              AND p.line IS NOT NULL
              AND p.p_over IS NOT NULL
              AND g.game_date >= %s::date
              AND g.game_date <= %s::date
        ),
        pred_ranked AS (
            SELECT
                pb.*,
                ROW_NUMBER() OVER (
                    PARTITION BY pb.player_id, pb.game_id
                    ORDER BY ABS(pb.p_over - 0.5) ASC, pb.line_value ASC
                ) AS line_rank
            FROM pred_base pb
        ),
        obs AS (
            SELECT
                pr.player_id,
                pr.game_id,
                g.game_date,
                COALESCE(pl.full_name, pr.player_id::text) AS player_name,
                COALESCE(ts.team, tp.team) AS team_abbr,
                pr.line_value,
                pr.p_over,
                s.shots_on_goal::float8 AS actual_value,
                CASE
                    WHEN s.shots_on_goal >= pr.line_value THEN 'win'
                    ELSE 'loss'
                END AS outcome
            FROM pred_ranked pr
            JOIN nhl.games g
              ON g.game_id = pr.game_id
            JOIN nhl.skater_game_logs_raw s
              ON s.player_id = pr.player_id
             AND s.game_id = pr.game_id
            LEFT JOIN nhl.players pl
              ON pl.player_id = pr.player_id
            LEFT JOIN nhl.teams ts
              ON ts.team_id = s.team_id
            LEFT JOIN nhl.teams tp
              ON tp.team_id = pl.current_team_id
            WHERE pr.line_rank = 1
              AND s.shots_on_goal IS NOT NULL
        ),
        ordered AS (
            SELECT
                o.*,
                ROW_NUMBER() OVER (
                    PARTITION BY o.player_id
                    ORDER BY o.game_date DESC, o.game_id DESC
                ) AS rn,
                FIRST_VALUE(o.outcome) OVER (
                    PARTITION BY o.player_id
                    ORDER BY o.game_date DESC, o.game_id DESC
                ) AS first_outcome
            FROM obs o
        ),
        recent AS (
            SELECT *
            FROM ordered
            WHERE rn <= %s
        ),
        streak_marks AS (
            SELECT
                r.*,
                SUM(CASE WHEN r.outcome <> r.first_outcome THEN 1 ELSE 0 END) OVER (
                    PARTITION BY r.player_id
                    ORDER BY r.rn
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS break_group
            FROM recent r
        ),
        agg AS (
            SELECT
                sm.player_id,
                MAX(sm.player_name) AS player_name,
                MAX(CASE WHEN sm.rn = 1 THEN sm.team_abbr END) AS team_abbr,
                MAX(CASE WHEN sm.rn = 1 THEN sm.line_value END) AS line_value,
                MAX(CASE WHEN sm.rn = 1 THEN sm.actual_value END) AS last_actual_value,
                MAX(CASE WHEN sm.rn = 1 THEN sm.p_over END) AS latest_p_over,
                MAX(CASE WHEN sm.rn = 1 THEN sm.game_date END) AS last_game_date,
                MAX(sm.first_outcome) AS streak_type,
                SUM(CASE WHEN sm.break_group = 0 THEN 1 ELSE 0 END) AS streak,
                COUNT(*) AS window_games,
                SUM(CASE WHEN sm.outcome = 'win' THEN 1 ELSE 0 END) AS window_wins,
                SUM(CASE WHEN sm.outcome = 'loss' THEN 1 ELSE 0 END) AS window_losses
            FROM streak_marks sm
            GROUP BY sm.player_id
        ),
        ranked_out AS (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.streak_type
                    ORDER BY
                        a.streak DESC,
                        CASE
                            WHEN a.streak_type = 'win' THEN a.window_wins
                            ELSE a.window_losses
                        END DESC,
                        a.player_name ASC
                ) AS bucket_rank
            FROM agg a
            WHERE a.streak_type IN ('win', 'loss')
              AND a.streak >= %s
        )
        SELECT
            player_id,
            player_name,
            team_abbr,
            line_value,
            last_actual_value,
            latest_p_over,
            last_game_date,
            streak_type,
            streak,
            window_games,
            window_wins,
            window_losses,
            bucket_rank
        FROM ranked_out
        WHERE bucket_rank <= %s
        ORDER BY streak_type ASC, bucket_rank ASC;
    """
    try:
        rows = pg_fetchall(
            sql,
            (from_date, target_date, window_games, min_streak, top_n),
        )
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    hot = []
    cold = []
    for row in rows:
        if row.get("streak_type") == "win":
            hot.append(row)
        elif row.get("streak_type") == "loss":
            cold.append(row)

    return {
        "ok": True,
        "date": str(target_date),
        "from_date": str(from_date),
        "window_games": int(window_games),
        "min_streak": int(min_streak),
        "top_n": int(top_n),
        "hot": hot,
        "cold": cold,
    }


def fetch_players_directory(limit: int, offset: int, include_inactive: bool = False):
    sql = """
    WITH latest_team AS (
      SELECT DISTINCT ON (rs.player_id)
        rs.player_id,
        rs.team_id
      FROM nhl.roster_status rs
      ORDER BY rs.player_id, rs.asof_ts DESC
    ),
    last_prop AS (
      SELECT
        pp.player_id,
        MAX(pp.game_date)::date AS last_prop_date
      FROM mlb.player_props pp
      WHERE pp.prop_source LIKE 'nhl_%'
      GROUP BY pp.player_id
    )
    SELECT
      p.player_id,
      p.full_name AS player_name,
      COALESCE(t.team, 'Unknown') AS team_abbr,
      COALESCE(t.team, 'Unknown') AS team,
      p.position,
      p.status,
      lp.last_prop_date
    FROM nhl.players p
    LEFT JOIN latest_team lt
      ON lt.player_id = p.player_id
    LEFT JOIN nhl.teams t
      ON t.team_id = COALESCE(p.current_team_id, lt.team_id)
    LEFT JOIN last_prop lp
      ON lp.player_id = p.player_id
    WHERE (%s::boolean OR COALESCE(LOWER(p.status), 'active') = 'active')
    ORDER BY
      COALESCE(t.team, 'Unknown') ASC,
      p.full_name ASC NULLS LAST,
      p.player_id ASC
    LIMIT %s OFFSET %s
    """
    try:
        return pg_fetchall(sql, (include_inactive, limit, offset))
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
