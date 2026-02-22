-- backend/nhl/sql/sog_calibration_explorer.sql
--
-- Creates a view nhl.sog_calibration_explorer that buckets SOG predictions
-- into 5% probability bands and lets you slice by:
--   - hot_last5_flag
--   - line
--   - (optionally) date ranges in ad-hoc queries
--
-- Assumptions (tweak these to match your schema):
--   * Predictions live in:      nhl.predictions
--   * SOG props are tagged by:  p.prop_type = 'shots_on_goal'
--   * Final prob used by site:  p.p_over           (NUMERIC 0–1)
--   * Outcome over/under:       p.outcome_over     (0/1 or boolean)
--   * Features table:           nhl.training_features_sog_denali f
--   * Has:                      f.hot_last5_flag   (boolean)
--   * Join key:                 (player_id, game_id, line)
--
-- If any of these differ, adjust the FROM/JOIN/WHERE/SELECT parts accordingly.

-- backend/nhl/sql/sog_calibration_explorer.sql

CREATE OR REPLACE VIEW nhl.sog_calibration_explorer AS
WITH base AS (
  SELECT
    p.game_id,
    p.player_id,
    p.line,                                   -- line comes from predictions
    p.game_date::date                            AS game_date,
    p.p_over                                     AS model_prob,     -- model prob 0–1
    CASE
      WHEN p.outcome_over IN (1, true) THEN 1.0
      WHEN p.outcome_over IN (0, false) THEN 0.0
      ELSE NULL
    END                                          AS is_over,        -- numeric for AVG()
    f.hot_last5_flag                             AS hot_last5_flag  -- boolean
  FROM nhl.predictions p
  JOIN nhl.training_features_sog_denali f
    ON f.player_id = p.player_id
   AND f.game_id  = p.game_id
  WHERE
    -- If/when you confirm a SOG discriminator column, add it back here, e.g.:
    --   AND p.prop_type = 'shots_on_goal'
    p.p_over IS NOT NULL
    AND p.outcome_over IS NOT NULL              -- only resolved props
    AND p.game_date >= DATE '2024-10-01'        -- “modern regime” guardrail
),
bucketed AS (
  SELECT
    game_date,
    line,
    hot_last5_flag,
    width_bucket(model_prob, 0.0, 1.0, 20)      AS bucket_idx,      -- 5% buckets
    COUNT(*)                                    AS n_props,
    AVG(is_over)                               AS actual_over_rate,
    AVG(model_prob)                            AS avg_model_prob
  FROM base
  GROUP BY game_date, line, hot_last5_flag, bucket_idx
)
SELECT
  line,
  hot_last5_flag,
  bucket_idx,
  format(
    '%s–%s%%',
    to_char((bucket_idx - 1) * 5, 'FM00'),
    to_char(bucket_idx * 5, 'FM00')
  )                                                   AS prob_bucket,
  SUM(n_props)                                        AS n_props,
  SUM(n_props * avg_model_prob) / NULLIF(SUM(n_props), 0)      AS avg_model_prob,
  SUM(n_props * actual_over_rate) / NULLIF(SUM(n_props), 0)    AS actual_over_rate
FROM bucketed
GROUP BY line, hot_last5_flag, bucket_idx
ORDER BY line, hot_last5_flag, bucket_idx;
