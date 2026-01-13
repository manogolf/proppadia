-- backend/nhl/sql/export_points.sql
-- Phoenix points export:
--   One row per (player_id, game_id) on the slate_date.
--   Features aligned with points_phoenix models (pre-game only).
--
-- Usage:
--   psql "$SUPABASE_DB_URL" --no-psqlrc -q \
--     -v ON_ERROR_STOP=1 \
--     -v slate_date=2025-11-08 \
--     -f backend/nhl/sql/export_points.sql > exports/train_nhl_points_v2.csv
--
-- Output columns:
--   player_id, game_id,
--   is_home,
--   d5_sog_per60,
--   d10_sog_per60,
--   attempts_d10_per60,
--   team_d10_sf_per_game,
--   last10_team_sog_share,
--   num_shotwasongoal_last5,
--   num_shotwasongoal_last10,
--   num_shotwasongoal_season_to_date,
--   num_event_shot_last5,
--   num_event_shot_last10,
--   num_event_shot_season_to_date,
--   team_num_event_shot_for_last10,
--   team_num_shotwasongoal_for_last10,
--   hot_last5_flag

COPY (
  WITH base AS (
    -- All (player, game) pairs on today's slate
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
    JOIN nhl.games g
      ON g.game_id = rs.game_id
    WHERE g.game_date = DATE :'slate_date'
  ),

  -- =========================
  -- PATCH (Option B): restrict history CTEs to slate players/teams + date window
  --
  -- Safe/conservative window: last ~120 days before slate_date (covers last-10 games
  -- even with breaks; shrinks scans dramatically).
  -- =========================

  slate_players AS (
    SELECT DISTINCT player_id
    FROM base
    WHERE player_id IS NOT NULL
  ),

  slate_teams AS (
    SELECT DISTINCT team_id
    FROM base
    WHERE team_id IS NOT NULL
  ),

  player_logs AS (
    -- Historical skater logs (regular season only) for ONLY slate players
    -- Windowed to keep exports fast; features only need last 10 anyway.
    SELECT
      l.player_id,
      l.game_id,
      g2.game_date,
      l.team_id,
      COALESCE(l.shots_on_goal, 0)::float  AS shots_on_goal,
      COALESCE(l.shot_attempts, 0)::float  AS shot_attempts,
      NULLIF(l.toi_minutes, 0)::float      AS toi_minutes
    FROM nhl.skater_game_logs_raw l
    JOIN nhl.games g2
      ON g2.game_id = l.game_id
    WHERE substring(g2.game_id::text, 5, 2) = '02'  -- regular season
      AND l.player_id IN (SELECT player_id FROM slate_players)
      AND g2.game_date <  DATE :'slate_date'
      AND g2.game_date >= (DATE :'slate_date' - INTERVAL '120 days')
  ),

  team_logs AS (
    -- Team-level SOG / attempts per game (regular season only) for ONLY slate teams
    -- Same conservative date window for speed.
    SELECT
      l.team_id,
      l.game_id,
      g.game_date,
      SUM(COALESCE(l.shots_on_goal, 0))::float   AS team_sog,
      SUM(COALESCE(l.shot_attempts, 0))::float   AS team_attempts
    FROM nhl.skater_game_logs_raw l
    JOIN nhl.games g
      ON g.game_id = l.game_id
    WHERE substring(g.game_id::text, 5, 2) = '02'  -- regular season
      AND l.team_id IN (SELECT team_id FROM slate_teams)
      AND g.game_date <  DATE :'slate_date'
      AND g.game_date >= (DATE :'slate_date' - INTERVAL '120 days')
    GROUP BY l.team_id, l.game_id, g.game_date
  )

  SELECT
    b.player_id,
    b.game_id,
    (b.is_home)::int AS is_home,

    -- d5_sog_per60: last 5 games
    COALESCE((
      SELECT AVG(x.sog_per60)
      FROM (
        SELECT
          CASE
            WHEN pl.toi_minutes > 0
            THEN pl.shots_on_goal * 60.0 / pl.toi_minutes
            ELSE NULL
          END AS sog_per60
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 5
      ) x
    ), 0.0) AS d5_sog_per60,

    -- d10_sog_per60: last 10 games
    COALESCE((
      SELECT AVG(x.sog_per60)
      FROM (
        SELECT
          CASE
            WHEN pl.toi_minutes > 0
            THEN pl.shots_on_goal * 60.0 / pl.toi_minutes
            ELSE NULL
          END AS sog_per60
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS d10_sog_per60,

    -- attempts_d10_per60: last 10 games, attempts per 60
    COALESCE((
      SELECT AVG(x.att_per60)
      FROM (
        SELECT
          CASE
            WHEN pl.toi_minutes > 0
            THEN pl.shot_attempts * 60.0 / pl.toi_minutes
            ELSE NULL
          END AS att_per60
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS attempts_d10_per60,

    -- team_d10_sf_per_game: team SOG per game over last 10
    COALESCE((
      SELECT AVG(x.team_sog)
      FROM (
        SELECT
          tl.team_sog
        FROM team_logs tl
        WHERE tl.team_id = b.team_id
          AND tl.game_date < b.game_date
        ORDER BY tl.game_date DESC, tl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS team_d10_sf_per_game,

    -- last10_team_sog_share: player's share of team SOG over last 10
    (
      SELECT
        CASE
          WHEN team_sog_sum > 0 THEN player_sog_sum / team_sog_sum
          ELSE 0.0
        END
      FROM (
        SELECT
          COALESCE((
            SELECT SUM(s.sog)
            FROM (
              SELECT pl.shots_on_goal AS sog
              FROM player_logs pl
              WHERE pl.player_id = b.player_id
                AND pl.game_date < b.game_date
              ORDER BY pl.game_date DESC, pl.game_id DESC
              LIMIT 10
            ) s
          ), 0.0) AS player_sog_sum,
          COALESCE((
            SELECT SUM(ts.team_sog)
            FROM (
              SELECT tl.team_sog
              FROM team_logs tl
              WHERE tl.team_id = b.team_id
                AND tl.game_date < b.game_date
              ORDER BY tl.game_date DESC, tl.game_id DESC
              LIMIT 10
            ) ts
          ), 0.0) AS team_sog_sum
      ) z
    ) AS last10_team_sog_share,

    -- num_shotwasongoal_last5: total SOG last 5
    COALESCE((
      SELECT SUM(x.sog)
      FROM (
        SELECT pl.shots_on_goal AS sog
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 5
      ) x
    ), 0.0) AS num_shotwasongoal_last5,

    -- num_shotwasongoal_last10: total SOG last 10
    COALESCE((
      SELECT SUM(x.sog)
      FROM (
        SELECT pl.shots_on_goal AS sog
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS num_shotwasongoal_last10,

    -- num_shotwasongoal_season_to_date: total SOG before today (regular season games only)
    COALESCE((
      SELECT SUM(pl.shots_on_goal)
      FROM player_logs pl
      WHERE pl.player_id = b.player_id
        AND pl.game_date < b.game_date
    ), 0.0) AS num_shotwasongoal_season_to_date,

    -- num_event_shot_last5: total attempts last 5
    COALESCE((
      SELECT SUM(x.att)
      FROM (
        SELECT pl.shot_attempts AS att
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 5
      ) x
    ), 0.0) AS num_event_shot_last5,

    -- num_event_shot_last10: total attempts last 10
    COALESCE((
      SELECT SUM(x.att)
      FROM (
        SELECT pl.shot_attempts AS att
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS num_event_shot_last10,

    -- num_event_shot_season_to_date: total attempts before today (regular season only)
    COALESCE((
      SELECT SUM(pl.shot_attempts)
      FROM player_logs pl
      WHERE pl.player_id = b.player_id
        AND pl.game_date < b.game_date
    ), 0.0) AS num_event_shot_season_to_date,

    -- team_num_event_shot_for_last10: team attempts last 10 games
    COALESCE((
      SELECT SUM(x.team_attempts)
      FROM (
        SELECT tl.team_attempts
        FROM team_logs tl
        WHERE tl.team_id = b.team_id
          AND tl.game_date < b.game_date
        ORDER BY tl.game_date DESC, tl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS team_num_event_shot_for_last10,

    -- team_num_shotwasongoal_for_last10: team SOG last 10 games
    COALESCE((
      SELECT SUM(x.team_sog)
      FROM (
        SELECT tl.team_sog
        FROM team_logs tl
        WHERE tl.team_id = b.team_id
          AND tl.game_date < b.game_date
        ORDER BY tl.game_date DESC, tl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS team_num_shotwasongoal_for_last10,

    -- hot_last5_flag: simple heater indicator from last 5 games' SOG
    CASE
      WHEN COALESCE((
        SELECT SUM(x.sog)
        FROM (
          SELECT pl.shots_on_goal AS sog
          FROM player_logs pl
          WHERE pl.player_id = b.player_id
            AND pl.game_date < b.game_date
          ORDER BY pl.game_date DESC, pl.game_id DESC
          LIMIT 5
        ) x
      ), 0.0) >= 15.0
      THEN 1
      ELSE 0
    END AS hot_last5_flag

  FROM base b
  ORDER BY b.player_id, b.game_id
) TO STDOUT WITH CSV HEADER;
