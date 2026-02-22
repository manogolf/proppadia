-- backend/nhl/sql/export_sog_calibration_training.sql
--
-- Export row-level SOG predictions + outcomes for calibration.
-- Output CSV:
--   backend/nhl/data/processed/sog_calibration_training.csv
--
-- Each row = one resolved SOG prop:
--   player_id, game_id, game_date, line, p_over_raw, y_over
--
-- Assumes:
--   * Predictions table: nhl.predictions
--   * Games table:       nhl.games (with game_date)
--   * SOG props:         prop_type = 'shots_on_goal'
--   * Model prob:        p_over        (NUMERIC 0–1)
--   * Outcome flag:      outcome_over  (0/1 or boolean)
--   * Modern regime:     game_date >= 2024-10-01

\set ON_ERROR_STOP on
\pset format csv
\pset footer off
\pset tuples_only off

\o backend/nhl/data/processed/sog_calibration_training.csv

SELECT
  p.player_id,
  p.game_id,
  g.game_date::date     AS game_date,
  p.line::numeric       AS line,
  p.p_over::numeric     AS p_over_raw,
  CASE
    WHEN p.outcome_over IN (1, true)  THEN 1
    WHEN p.outcome_over IN (0, false) THEN 0
    ELSE NULL
  END                   AS y_over
FROM nhl.predictions p
JOIN nhl.games g
  ON g.game_id = p.game_id
WHERE
  p.p_over IS NOT NULL
  AND p.outcome_over IS NOT NULL
  AND g.game_date >= DATE '2024-10-01'
  AND p.prop_type = 'shots_on_goal';

\o
\pset format aligned
