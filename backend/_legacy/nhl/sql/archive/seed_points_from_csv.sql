-- Copies CSV created by ingest_points_from_boxscores.py into stage and promotes to raw.
-- temp staging table for this load

CREATE TEMP TABLE _points_stage(
  player_id bigint,
  game_id   bigint,
  game_date date,
  goals     int,
  assists   int
);

\set ON_ERROR_STOP on
\echo [seed_points_from_csv] csv_path=:csv_path
\copy _points_stage FROM :csv_path WITH (FORMAT csv, HEADER true)

-- upsert into nhl.import_skater_logs_stage
INSERT INTO nhl.import_skater_logs_stage (player_id, game_id, game_date, goals, assists)
SELECT player_id, game_id, game_date, goals, assists
FROM _points_stage
ON CONFLICT (player_id, game_id) DO UPDATE SET
  goals   = EXCLUDED.goals,
  assists = EXCLUDED.assists;

-- keep raw in sync via same promote pattern the CLI uses
WITH src AS (
  SELECT DISTINCT s.player_id, s.game_id, s.game_date, s.goals, s.assists
  FROM nhl.import_skater_logs_stage s
  WHERE s.game_date = CURRENT_DATE
)
INSERT INTO nhl.skater_game_logs_raw (player_id, game_id, game_date, goals, assists, points)
SELECT player_id, game_id, game_date, goals, assists, COALESCE(goals,0)+COALESCE(assists,0)
FROM src
ON CONFLICT (player_id, game_id) DO UPDATE SET
  goals   = EXCLUDED.goals,
  assists = EXCLUDED.assists,
  points  = EXCLUDED.points;
