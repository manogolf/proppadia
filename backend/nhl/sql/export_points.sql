-- Phoenix points export:
--   One row per (player_id, game_id) on the slate_date.
--   Features aligned with points_phoenix models (pre-game only).
--
-- Player rolling windows use strict-prior regular-season history across
-- canonical season boundaries. Player season-to-date fields are scoped to
-- nhl.games.season. Team history intentionally retains its existing 120-day
-- bound; last10_team_sog_share therefore has a cross-season player numerator
-- and a 120-day-bounded team denominator.
--
-- Usage:
--   psql "$SUPABASE_DB_URL" --no-psqlrc -q \
--     -v ON_ERROR_STOP=1 \
--     -v slate_date=2025-11-08 \
--     -f backend/nhl/sql/export_points.sql > exports/train_nhl_points_v2.csv
--
-- Output columns and order are frozen to the existing 15-feature model schema.

COPY (
  WITH base AS (
    SELECT
      rs.player_id,
      rs.game_id,
      g.game_date,
      g.season::int AS canonical_season,
      rs.team_id,
      CASE
        WHEN rs.team_id = g.home_team_id THEN g.away_team_id
        WHEN rs.team_id = g.away_team_id THEN g.home_team_id
        ELSE NULL
      END AS opponent_id,
      (rs.team_id = g.home_team_id) AS is_home
    FROM nhl.roster_status rs
    JOIN nhl.games g ON g.game_id = rs.game_id
    WHERE g.game_date = DATE :'slate_date'
  ),

  slate_teams AS (
    SELECT DISTINCT team_id
    FROM base
    WHERE team_id IS NOT NULL
  ),

  -- Pull only the latest ten strict-prior regular-season games for each
  -- slate player/game. There is deliberately no date lower bound: LIMIT 10
  -- bounds the retained window without changing its membership.
  player_window_rows AS (
    SELECT
      b.player_id,
      b.game_id AS target_game_id,
      h.history_game_id,
      h.game_date,
      h.start_time_utc,
      h.shots_on_goal,
      h.shot_attempts,
      h.toi_minutes,
      ROW_NUMBER() OVER (
        PARTITION BY b.player_id, b.game_id
        ORDER BY h.game_date DESC, h.start_time_utc DESC NULLS LAST,
                 h.history_game_id DESC
      ) AS rn_desc
    FROM base b
    JOIN LATERAL (
      SELECT
        l.game_id AS history_game_id,
        g2.game_date,
        g2.start_time_utc,
        COALESCE(l.shots_on_goal, 0)::float AS shots_on_goal,
        COALESCE(l.shot_attempts, 0)::float AS shot_attempts,
        NULLIF(l.toi_minutes, 0)::float AS toi_minutes
      FROM nhl.skater_game_logs_raw l
      JOIN nhl.games g2 ON g2.game_id = l.game_id
      WHERE l.player_id = b.player_id
        AND substring(g2.game_id::text, 5, 2) = '02'  -- regular season
        AND g2.game_date < b.game_date             -- strict prior by existing date contract
      ORDER BY g2.game_date DESC, g2.start_time_utc DESC NULLS LAST,
               g2.game_id DESC
      LIMIT 10
    ) h ON TRUE
  ),

  player_window_features AS (
    SELECT
      player_id,
      target_game_id,
      AVG(CASE WHEN rn_desc <= 5 AND toi_minutes > 0
               THEN shots_on_goal * 60.0 / toi_minutes END)::float AS d5_sog_per60,
      AVG(CASE WHEN rn_desc <= 10 AND toi_minutes > 0
               THEN shots_on_goal * 60.0 / toi_minutes END)::float AS d10_sog_per60,
      AVG(CASE WHEN rn_desc <= 10 AND toi_minutes > 0
               THEN shot_attempts * 60.0 / toi_minutes END)::float AS attempts_d10_per60,
      SUM(CASE WHEN rn_desc <= 5 THEN shots_on_goal ELSE 0 END)::float AS num_shotwasongoal_last5,
      SUM(CASE WHEN rn_desc <= 10 THEN shots_on_goal ELSE 0 END)::float AS num_shotwasongoal_last10,
      SUM(CASE WHEN rn_desc <= 5 THEN shot_attempts ELSE 0 END)::float AS num_event_shot_last5,
      SUM(CASE WHEN rn_desc <= 10 THEN shot_attempts ELSE 0 END)::float AS num_event_shot_last10,
      CASE WHEN SUM(CASE WHEN rn_desc <= 5 THEN shots_on_goal ELSE 0 END) >= 15
           THEN 1 ELSE 0 END AS hot_last5_flag
    FROM player_window_rows
    GROUP BY player_id, target_game_id
  ),

  -- These fields are genuinely season-to-date: use nhl.games.season, the
  -- canonical season identity attached to the target game, and strict prior.
  player_season_to_date AS (
    SELECT
      b.player_id,
      b.game_id AS target_game_id,
      COALESCE(s.num_shotwasongoal_season_to_date, 0)::float AS num_shotwasongoal_season_to_date,
      COALESCE(s.num_event_shot_season_to_date, 0)::float AS num_event_shot_season_to_date
    FROM base b
    LEFT JOIN LATERAL (
      SELECT
        SUM(COALESCE(l.shots_on_goal, 0)) AS num_shotwasongoal_season_to_date,
        SUM(COALESCE(l.shot_attempts, 0)) AS num_event_shot_season_to_date
      FROM nhl.skater_game_logs_raw l
      JOIN nhl.games gh ON gh.game_id = l.game_id
      WHERE l.player_id = b.player_id
        AND gh.season::int = b.canonical_season
        AND substring(gh.game_id::text, 5, 2) = '02'  -- regular season
        AND gh.game_date < b.game_date
    ) s ON TRUE
  ),

  -- Team-side contract intentionally unchanged: slate-team restriction and
  -- the existing 120-day regular-season window remain in effect.
  team_logs AS (
    SELECT
      l.team_id,
      l.game_id,
      g.game_date,
      SUM(COALESCE(l.shots_on_goal, 0))::float AS team_sog,
      SUM(COALESCE(l.shot_attempts, 0))::float AS team_attempts
    FROM nhl.skater_game_logs_raw l
    JOIN nhl.games g ON g.game_id = l.game_id
    WHERE substring(g.game_id::text, 5, 2) = '02'  -- regular season
      AND l.team_id IN (SELECT team_id FROM slate_teams)
      AND g.game_date < DATE :'slate_date'
      AND g.game_date >= (DATE :'slate_date' - INTERVAL '120 days')
    GROUP BY l.team_id, l.game_id, g.game_date
  )

  SELECT
    b.player_id,
    b.game_id,
    (b.is_home)::int AS is_home,
    COALESCE(pwf.d5_sog_per60, 0.0) AS d5_sog_per60,
    COALESCE(pwf.d10_sog_per60, 0.0) AS d10_sog_per60,
    COALESCE(pwf.attempts_d10_per60, 0.0) AS attempts_d10_per60,

    -- Team d10 remains based on the existing bounded team history.
    COALESCE((
      SELECT AVG(tl.team_sog)
      FROM (
        SELECT team_sog
        FROM team_logs
        WHERE team_id = b.team_id AND game_date < b.game_date
        ORDER BY game_date DESC, game_id DESC
        LIMIT 10
      ) tl
    ), 0.0) AS team_d10_sf_per_game,

    -- Mixed contract: player numerator is cross-season last-10; team
    -- denominator preserves the existing team 120-day window.
    CASE
      WHEN COALESCE((
        SELECT SUM(team_sog)
        FROM (
          SELECT team_sog
          FROM team_logs
          WHERE team_id = b.team_id AND game_date < b.game_date
          ORDER BY game_date DESC, game_id DESC
          LIMIT 10
        ) team_window
      ), 0.0) > 0
      THEN COALESCE(pwf.num_shotwasongoal_last10, 0.0) / (
        SELECT SUM(team_sog)
        FROM (
          SELECT team_sog
          FROM team_logs
          WHERE team_id = b.team_id AND game_date < b.game_date
          ORDER BY game_date DESC, game_id DESC
          LIMIT 10
        ) team_window
      )
      ELSE 0.0
    END AS last10_team_sog_share,

    COALESCE(pwf.num_shotwasongoal_last5, 0.0) AS num_shotwasongoal_last5,
    COALESCE(pwf.num_shotwasongoal_last10, 0.0) AS num_shotwasongoal_last10,
    COALESCE(pstd.num_shotwasongoal_season_to_date, 0.0) AS num_shotwasongoal_season_to_date,
    COALESCE(pwf.num_event_shot_last5, 0.0) AS num_event_shot_last5,
    COALESCE(pwf.num_event_shot_last10, 0.0) AS num_event_shot_last10,
    COALESCE(pstd.num_event_shot_season_to_date, 0.0) AS num_event_shot_season_to_date,

    COALESCE((
      SELECT SUM(team_attempts)
      FROM (
        SELECT team_attempts
        FROM team_logs
        WHERE team_id = b.team_id AND game_date < b.game_date
        ORDER BY game_date DESC, game_id DESC
        LIMIT 10
      ) team_window
    ), 0.0) AS team_num_event_shot_for_last10,
    COALESCE((
      SELECT SUM(team_sog)
      FROM (
        SELECT team_sog
        FROM team_logs
        WHERE team_id = b.team_id AND game_date < b.game_date
        ORDER BY game_date DESC, game_id DESC
        LIMIT 10
      ) team_window
    ), 0.0) AS team_num_shotwasongoal_for_last10,
    COALESCE(pwf.hot_last5_flag, 0) AS hot_last5_flag
  FROM base b
  LEFT JOIN player_window_features pwf
    ON pwf.player_id = b.player_id AND pwf.target_game_id = b.game_id
  LEFT JOIN player_season_to_date pstd
    ON pstd.player_id = b.player_id AND pstd.target_game_id = b.game_id
  ORDER BY b.player_id, b.game_id
) TO STDOUT WITH CSV HEADER;
