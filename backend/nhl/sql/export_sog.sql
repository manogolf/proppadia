-- backend/nhl/sql/export_sog.sql
-- Phoenix SOG export
-- One row per (player_id, game_id) on :slate_date with pre-game features
-- aligned to sog_player_v2 / phoenix SOG models.
--
-- Usage:
--   psql "$SUPABASE_DB_URL" --no-psqlrc -q \
--     -v ON_ERROR_STOP=1 \
--     -v slate_date=YYYY-MM-DD \
--     -f backend/nhl/sql/export_sog.sql \
--     > exports/train_nhl_sog_v2.csv
--
-- Output columns:
--   player_id,game_id,is_home,
--   num_shotwasongoal_last5,
--   num_shotwasongoal_last10,
--   num_shotwasongoal_season_to_date,
--   num_event_shot_last5,
--   num_event_shot_last10,
--   num_event_shot_season_to_date,
--   team_num_event_shot_for_last10,
--   team_num_shotwasongoal_for_last10,
--   last5_sog_per_game,
--   last10_sog_per_game,
--   last10_shot_attempts_per_game,
--   last10_team_sog_share,
--   hot_last5_flag

COPY (
  WITH base AS (
    -- Slate players for the given date
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
  player_logs AS (
    -- Historical player SOG/attempts
    SELECT
      l.player_id,
      l.game_id,
      l.game_date,
      l.team_id,
      COALESCE(l.shots_on_goal, 0)::float  AS shots_on_goal,
      COALESCE(l.shot_attempts, 0)::float  AS shot_attempts
    FROM nhl.skater_game_logs_raw l
  ),
  team_logs AS (
    -- Team-level SOG/attempts per game
    SELECT
      l.team_id,
      l.game_id,
      g.game_date,
      SUM(COALESCE(l.shots_on_goal, 0))::float AS team_sog,
      SUM(COALESCE(l.shot_attempts, 0))::float AS team_attempts
    FROM nhl.skater_game_logs_raw l
    JOIN nhl.games g
      ON g.game_id = l.game_id
    GROUP BY l.team_id, l.game_id, g.game_date
  )
  SELECT
    b.player_id,
    b.game_id,
    (b.is_home)::int AS is_home,

    -- num_shotwasongoal_last5
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

    -- num_shotwasongoal_last10
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

    -- num_shotwasongoal_season_to_date
    COALESCE((
      SELECT SUM(pl.shots_on_goal)
      FROM player_logs pl
      WHERE pl.player_id = b.player_id
        AND pl.game_date < b.game_date
    ), 0.0) AS num_shotwasongoal_season_to_date,

    -- num_event_shot_last5 (attempts)
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

    -- num_event_shot_last10 (attempts)
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

    -- num_event_shot_season_to_date (attempts)
    COALESCE((
      SELECT SUM(pl.shot_attempts)
      FROM player_logs pl
      WHERE pl.player_id = b.player_id
        AND pl.game_date < b.game_date
    ), 0.0) AS num_event_shot_season_to_date,

    -- team_num_event_shot_for_last10 (team attempts last 10)
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

    -- team_num_shotwasongoal_for_last10 (team SOG last 10)
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

    -- last5_sog_per_game
    COALESCE((
      SELECT AVG(x.sog)
      FROM (
        SELECT pl.shots_on_goal AS sog
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 5
      ) x
    ), 0.0) AS last5_sog_per_game,

    -- last10_sog_per_game
    COALESCE((
      SELECT AVG(x.sog)
      FROM (
        SELECT pl.shots_on_goal AS sog
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS last10_sog_per_game,

    -- last10_shot_attempts_per_game
    COALESCE((
      SELECT AVG(x.att)
      FROM (
        SELECT pl.shot_attempts AS att
        FROM player_logs pl
        WHERE pl.player_id = b.player_id
          AND pl.game_date < b.game_date
        ORDER BY pl.game_date DESC, pl.game_id DESC
        LIMIT 10
      ) x
    ), 0.0) AS last10_shot_attempts_per_game,

    -- last10_team_sog_share: player SOG share of team SOG last 10
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

    -- hot_last5_flag: on a heater if ≥15 SOG last 5
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
      ), 0.0) >= 15.0 THEN 1
      ELSE 0
    END AS hot_last5_flag

  FROM base b
  ORDER BY b.player_id, b.game_id
) TO STDOUT WITH CSV HEADER;
