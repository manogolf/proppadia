-- backend/nhl/sql/export_sog_denali_calibration_training.sql
-- Build calibration training data for Denali SOG:
-- One row per (player_id, game_id, line) with:
--   season, game_date, line, prob_over, y_over
--   where y_over = 1 if shots_on_goal > line, else 0
--
-- Usage (from repo root, adjust connection string as needed):
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 \
--     -f backend/nhl/sql/export_sog_denali_calibration_training.sql \
--     > backend/nhl/data/processed/sog_calibration_training_denali.csv

COPY (
  WITH logs AS (
    -- Canonical SOG per player/game
    SELECT
      l.player_id,
      l.game_id,
      MAX(COALESCE(l.shots_on_goal, 0))::int AS sog
    FROM nhl.skater_game_logs_raw l
    GROUP BY l.player_id, l.game_id
  ),
  preds AS (
    -- Denali SOG predictions from nhl.predictions
    SELECT
      p.player_id,
      p.game_id,
      p.line,
      p.p_over,
      g.season,
      g.game_date,
      l.sog
    FROM nhl.predictions p
    JOIN nhl.games g USING (game_id)
    JOIN logs  l
      ON l.player_id = p.player_id
     AND l.game_id   = p.game_id
    WHERE p.prop         = 'shots_on_goal'
      AND p.model_family = 'sog_denali_lr_rf'
      AND g.season IN (2024, 2025)
      AND p.line IN (0.5, 1.5, 2.5, 3.5)
  )
  SELECT
    season,
    game_date,
    line,
    p_over AS prob_over,
    CASE WHEN sog > line THEN 1 ELSE 0 END AS y_over
  FROM preds
  ORDER BY season, game_date, line, player_id, game_id
) TO STDOUT WITH (FORMAT csv, HEADER true);
