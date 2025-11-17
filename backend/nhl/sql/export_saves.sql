\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 \
--     -v slate_date=2025-10-28 \
--     -f backend/nhl/sql/export_saves.sql > exports/train_goalie_saves_v2.csv

SET statement_timeout = 0;  -- allow this heavy export to finish

COPY (
WITH base AS (
  SELECT
    rs.player_id,
    rs.game_id,
    g.game_date,
    rs.team_id,
    CASE
      WHEN rs.team_id = g.home_team_id THEN g.away_team_id
      WHEN rs.team_id = g.away_team_id THEN g.home_team_id
      ELSE NULL
    END AS opponent_id,
    (rs.team_id = g.home_team_id) AS is_home
  FROM nhl.roster_status rs
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date = DATE :'slate_date'
),

-- Goalie logs with per-60 rates and rolling windows that EXCLUDE the current game
glogs AS (
  SELECT
    l.player_id,
    l.game_id,
    l.game_date,
    l.saves,
    l.shots_faced AS shots_against,
    l.toi_minutes,

    -- per-60 (guard divide-by-zero)
    CASE WHEN COALESCE(l.toi_minutes,0) > 0
         THEN (l.saves::double precision / l.toi_minutes::double precision) * 60.0
         ELSE NULL END AS saves_per60,
    CASE WHEN COALESCE(l.toi_minutes,0) > 0
         THEN (l.shots_faced::double precision / l.toi_minutes::double precision) * 60.0
         ELSE NULL END AS sa_per60,

    -- per-game save%
    CASE WHEN COALESCE(l.shots_faced,0) > 0
        THEN (l.saves::double precision / l.shots_faced::double precision)
         ELSE NULL END AS save_pct,

    -- d5/d10/d20 rolling windows (exclude current row)
    AVG(CASE WHEN COALESCE(l.toi_minutes,0) > 0
             THEN (l.saves::double precision / l.toi_minutes::double precision) * 60.0 END)
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING)  AS d5_saves_per60,

    AVG(CASE WHEN COALESCE(l.toi_minutes,0) > 0
             THEN (l.saves::double precision / l.toi_minutes::double precision) * 60.0 END)
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_saves_per60,

    AVG(CASE WHEN COALESCE(l.toi_minutes,0) > 0
             THEN (l.saves::double precision / l.toi_minutes::double precision) * 60.0 END)
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS d20_saves_per60,

    AVG(CASE WHEN COALESCE(l.toi_minutes,0) > 0
            THEN (l.shots_faced::double precision / l.toi_minutes::double precision) * 60.0 END)
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING)  AS d5_sa_per60,

    AVG(CASE WHEN COALESCE(l.toi_minutes,0) > 0
             THEN (l.shots_faced::double precision / l.toi_minutes::double precision) * 60.0 END)
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_sa_per60
  FROM nhl.goalie_game_logs_raw l
),

-- Most recent prior log snapshot for each goalie on the slate
snap AS (
  SELECT
    b.player_id,
    b.game_id,
    (SELECT r.d5_saves_per60   FROM glogs r WHERE r.player_id=b.player_id AND r.game_date<b.game_date ORDER BY r.game_date DESC, r.game_id DESC LIMIT 1) AS d5_saves_per60,
    (SELECT r.d10_saves_per60  FROM glogs r WHERE r.player_id=b.player_id AND r.game_date<b.game_date ORDER BY r.game_date DESC, r.game_id DESC LIMIT 1) AS d10_saves_per60,
    (SELECT r.d20_saves_per60  FROM glogs r WHERE r.player_id=b.player_id AND r.game_date<b.game_date ORDER BY r.game_date DESC, r.game_id DESC LIMIT 1) AS d20_saves_per60,
    (SELECT r.d5_sa_per60      FROM glogs r WHERE r.player_id=b.player_id AND r.game_date<b.game_date ORDER BY r.game_date DESC, r.game_id DESC LIMIT 1) AS d5_shots_faced_per60,
    (SELECT r.d10_sa_per60     FROM glogs r WHERE r.player_id=b.player_id AND r.game_date<b.game_date ORDER BY r.game_date DESC, r.game_id DESC LIMIT 1) AS d10_shots_faced_per60,
    (
      SELECT AVG(sub.save_pct)
      FROM (
        SELECT r.save_pct
        FROM glogs r
        WHERE r.player_id = b.player_id
          AND r.game_date < b.game_date
        ORDER BY r.game_date DESC, r.game_id DESC
        LIMIT 10
      ) AS sub
    ) AS d10_save_pct
  FROM base b
),

-- Last prior appearance → rest_days / b2b
last_game AS (
  SELECT DISTINCT ON (b.player_id)
         b.player_id, lg.game_date AS prev_game_date
  FROM base b
  JOIN glogs lg
    ON lg.player_id = b.player_id
   AND lg.game_date <  b.game_date
  ORDER BY b.player_id, lg.game_date DESC, lg.game_id DESC
),

-- Season-to-date save% (exclude today) using NHL season key (year flips on July 1)
season_to_date AS (
  SELECT
    b.player_id,
    AVG(r.save_pct) AS season_save_pct
  FROM base b
  JOIN glogs r
    ON r.player_id = b.player_id
   AND r.game_date < b.game_date
  WHERE (
    CASE WHEN EXTRACT(MONTH FROM r.game_date) >= 7
         THEN EXTRACT(YEAR FROM r.game_date)+1
         ELSE EXTRACT(YEAR FROM r.game_date) END
  ) = (
    CASE WHEN EXTRACT(MONTH FROM b.game_date) >= 7
         THEN EXTRACT(YEAR FROM b.game_date)+1
         ELSE EXTRACT(YEAR FROM b.game_date) END
  )
  GROUP BY b.player_id
)

SELECT
  NULL::text                              AS full_name,      -- name join is downstream

  b.player_id,
  b.game_id,
  b.team_id,
  b.opponent_id,
  (b.is_home)::int                        AS is_home,
  b.game_date::date                       AS game_date,

  -- Core features (snapshots from rolling windows)
  s.d10_shots_faced_per60,
  s.d10_save_pct,
  tc.d10_sf_per60                         AS team_d10_sf_per_game,       -- naming compatibility
  tc.opp_d10_sf_per60                     AS opp_d10_sf_allowed_per_game,

  NULL::numeric                           AS pace_index,                  -- column present; source optional
  GREATEST(0, (b.game_date - lg.prev_game_date))::int AS rest_days,
  (GREATEST(0, (b.game_date - lg.prev_game_date)) = 1) AS b2b_flag,

  s.d5_saves_per60,
  s.d10_saves_per60,
  s.d5_shots_faced_per60,
  std.season_save_pct,

  tc.opp_d10_sf_per60,
  tc.d10_sa_per60                         AS team_d10_sa_per60,
  tc.pace_matchup_index,
  s.d20_saves_per60,
  tc.d10_sf_per60                         AS team_d10_sf_per60,
  tc.opp_d10_sa_per60,
  NULL::numeric                           AS start_prob                   -- keep column; value filled upstream if available

FROM base b
LEFT JOIN snap s
  ON s.player_id = b.player_id AND s.game_id = b.game_id
LEFT JOIN last_game lg
  ON lg.player_id = b.player_id
LEFT JOIN season_to_date std
  ON std.player_id = b.player_id
LEFT JOIN nhl.team_context_rolling tc
  ON tc.game_id = b.game_id
 AND tc.team_id = b.team_id
ORDER BY b.game_id, b.player_id
) TO STDOUT WITH CSV HEADER;
