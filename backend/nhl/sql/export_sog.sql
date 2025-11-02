\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 \
--     -v slate_date=2025-10-28 \
--     -f backend/nhl/sql/export_sog.sql > exports/train_nhl_sog_v2.csv

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

-- Raw logs with per-60 rates and rolling windows that EXCLUDE the current row
logs AS (
  SELECT
    l.player_id,
    l.game_id,
    l.game_date,
    l.shots_on_goal,
    l.shot_attempts,
    l.toi_minutes,
    l.pp_toi_minutes,

    -- per-60 rates (guard divide-by-zero)
    CASE WHEN COALESCE(l.toi_minutes,0) > 0
         THEN (l.shots_on_goal::double precision / l.toi_minutes::double precision) * 60.0
         ELSE NULL END AS sog_per60,
    CASE WHEN COALESCE(l.toi_minutes,0) > 0
         THEN (l.shot_attempts::double precision / l.toi_minutes::double precision) * 60.0
         ELSE NULL END AS attempts_per60,

    -- d5/d10/d20 rolling per-60 windows (exclude current row with 1 PRECEDING end)
    AVG( CASE WHEN COALESCE(l.toi_minutes,0) > 0
              THEN (l.shots_on_goal::double precision / l.toi_minutes::double precision) * 60.0 END )
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING) AS d5_sog_per60,

    AVG( CASE WHEN COALESCE(l.toi_minutes,0) > 0
              THEN (l.shots_on_goal::double precision / l.toi_minutes::double precision) * 60.0 END )
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS d10_sog_per60,

    AVG( CASE WHEN COALESCE(l.toi_minutes,0) > 0
              THEN (l.shots_on_goal::double precision / l.toi_minutes::double precision) * 60.0 END )
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS d20_sog_per60,

    AVG( CASE WHEN COALESCE(l.toi_minutes,0) > 0
              THEN (l.shot_attempts::double precision / l.toi_minutes::double precision) * 60.0 END )
      OVER (PARTITION BY l.player_id ORDER BY l.game_date, l.game_id
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS attempts_d10_per60
  FROM nhl.skater_game_logs_raw l
),

-- Last appearance before the slate date (for rest_days / b2b)
last_game AS (
  SELECT DISTINCT ON (b.player_id)
         b.player_id,
         lg.game_date AS prev_game_date
  FROM base b
  JOIN logs lg
    ON lg.player_id = b.player_id
   AND lg.game_date <  b.game_date
  ORDER BY b.player_id, lg.game_date DESC, lg.game_id DESC
)

SELECT
  -- name left to the UI join; keep a placeholder column to match scorer output
  NULL::text                      AS full_name,

  b.player_id,
  b.game_id,
  b.team_id,
  b.opponent_id,
  (b.is_home)::int                AS is_home,
  b.game_date::date               AS game_date,

  NULL::int                       AS shots_on_goal,            -- unknown pregame

  -- Rolling features from most recent prior log row
  w.d5_sog_per60,
  w.d10_sog_per60,
  w.d20_sog_per60,

  -- Team context (per-60); mapped to expected column names
  t.d10_sf_per60                  AS team_d10_sf_per_game,     -- same units for now
  t.opp_d10_sf_per60              AS opp_d10_sf_allowed_per_game,

  -- Extra features expected by scorer; fill what we can
  NULL::numeric                   AS role_pp_share,            -- TODO: wire from roster usage
  GREATEST(0, (b.game_date - lg.prev_game_date))::int AS rest_days,
  (GREATEST(0, (b.game_date - lg.prev_game_date)) = 1) AS b2b_flag,

  w.attempts_d10_per60,
  NULL::numeric                   AS pace_index,               -- TODO: if you keep this, define source
  t.opp_d10_sf_per60,
  t.d10_sa_per60                  AS team_d10_sa_per60,
  t.pace_matchup_index

FROM base b
LEFT JOIN LATERAL (
  -- Most recent prior log row for this player (to snapshot the rolling windows)
  SELECT
    r.d5_sog_per60,
    r.d10_sog_per60,
    r.d20_sog_per60,
    r.attempts_d10_per60
  FROM logs r
  WHERE r.player_id = b.player_id
    AND r.game_date <  b.game_date
  ORDER BY r.game_date DESC, r.game_id DESC
  LIMIT 1
) AS w ON TRUE
LEFT JOIN last_game lg
  ON lg.player_id = b.player_id
LEFT JOIN nhl.team_context_rolling t
  ON t.game_id = b.game_id
 AND t.team_id = b.team_id
ORDER BY b.game_id, b.player_id;

-- Emit CSV
\copy (SELECT * FROM (
  SELECT
    full_name, player_id, game_id, team_id, opponent_id, is_home, game_date,
    shots_on_goal,
    d5_sog_per60, d10_sog_per60, d20_sog_per60,
    team_d10_sf_per_game, opp_d10_sf_allowed_per_game,
    role_pp_share, rest_days, b2b_flag,
    attempts_d10_per60, pace_index,
    opp_d10_sf_per60, team_d10_sa_per60, pace_matchup_index
  FROM (
    SELECT * FROM pg_temp.export_sog_materialized
  ) e
) q) TO STDOUT WITH CSV HEADER
\gset
