-- Seed today's points predictions into nhl.predictions from a wide CSV.
-- Vars:
--   :csv_path   path to backend/nhl/data/processed/points_predictions.csv
--   :slate_date e.g. 2025-10-28 (ET)

\set csv_path   :'csv_path'
\set slate_date :'slate_date'

-- 1) Create a temp table matching the CSV header (super-set of expected cols).
DROP TABLE IF EXISTS _pts_stage;
CREATE TEMP TABLE _pts_stage (
  player_id    bigint,
  game_id      bigint,
  team_id      bigint,
  opponent_id  bigint,
  is_home      int,
  game_date    date,
  lambda_hat   double precision,
  p_over_0_5   double precision,
  p_over_1_5   double precision,
  p_over_2_5   double precision,
  p_over_3_5   double precision
);

-- 2) Load the CSV. Missing columns in the file will just stay NULL if present in the table.
\copy _pts_stage FROM :'csv_path' WITH (FORMAT csv, HEADER true)

-- 3) Unpivot present p_over_* columns into (line, p_over).
WITH melted AS (
  SELECT s.player_id, s.game_id,
         unp.line, unp.p_over
  FROM _pts_stage s
  CROSS JOIN LATERAL (
    VALUES
      (0.5, s.p_over_0_5),
      (1.5, s.p_over_1_5),
      (2.5, s.p_over_2_5),
      (3.5, s.p_over_3_5)
  ) AS unp(line, p_over)
  WHERE p_over IS NOT NULL
),
ins AS (
  INSERT INTO nhl.predictions (game_id, player_id, prop, line, p_over, model_family, model_version, model_params)
  SELECT m.game_id, m.player_id, 'player_points' AS prop, m.line, m.p_over,
         'poisson'::text AS model_family,
         'latest/player_points'::text AS model_version,
         '{}'::jsonb AS model_params
  FROM melted m
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date = DATE :'slate_date'
  ON CONFLICT (prediction_id) DO NOTHING
  RETURNING 1
)
SELECT COALESCE((SELECT COUNT(*) FROM ins), 0) AS inserted_rows;
