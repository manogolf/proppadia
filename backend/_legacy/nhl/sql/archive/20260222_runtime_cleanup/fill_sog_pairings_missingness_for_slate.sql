-- ============================================================
-- add + fill pairings coverage-aware columns (d10/d20)
-- Run:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date=2026-01-04 -f <this_file>.sql
-- ============================================================

\set ON_ERROR_STOP on

-- 0) Add columns (idempotent)
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS d10_pairings_missing_flag   int NULL,
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket     text NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_missing_flag   int NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket     text NULL;

-- 1) Fill for slate_date (coverage_rate is canonical; NULL => missing)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_pairings_missing_flag = CASE WHEN t.d10_shiftcharts_coverage_rate IS NULL THEN 1 ELSE 0 END,
  d10_pairings_cov_bucket   = CASE
    WHEN t.d10_shiftcharts_coverage_rate IS NULL THEN 'missing'
    WHEN t.d10_shiftcharts_coverage_rate < 0.33 THEN 'low'
    WHEN t.d10_shiftcharts_coverage_rate < 0.67 THEN 'mid'
    ELSE 'high'
  END,

  d20_pairings_missing_flag = CASE WHEN t.d20_shiftcharts_coverage_rate IS NULL THEN 1 ELSE 0 END,
  d20_pairings_cov_bucket   = CASE
    WHEN t.d20_shiftcharts_coverage_rate IS NULL THEN 'missing'
    WHEN t.d20_shiftcharts_coverage_rate < 0.33 THEN 'low'
    WHEN t.d20_shiftcharts_coverage_rate < 0.67 THEN 'mid'
    ELSE 'high'
  END
WHERE t.game_date = DATE :'slate_date';

-- 2) Quick slate summary
SELECT
  DATE :'slate_date' AS game_date,
  COUNT(*) AS pregame_rows,

  COUNT(*) FILTER (WHERE d10_shiftcharts_coverage_rate IS NOT NULL) AS nonnull_d10_cov_rate,
  COUNT(*) FILTER (WHERE d20_shiftcharts_coverage_rate IS NOT NULL) AS nonnull_d20_cov_rate,

  COUNT(*) FILTER (WHERE d10_pairings_cov_bucket = 'missing') AS d10_b0_missing,
  COUNT(*) FILTER (WHERE d10_pairings_cov_bucket = 'low')     AS d10_b1_low,
  COUNT(*) FILTER (WHERE d10_pairings_cov_bucket = 'mid')     AS d10_b2_mid,
  COUNT(*) FILTER (WHERE d10_pairings_cov_bucket = 'high')    AS d10_b3_high,

  COUNT(*) FILTER (WHERE d20_pairings_cov_bucket = 'missing') AS d20_b0_missing,
  COUNT(*) FILTER (WHERE d20_pairings_cov_bucket = 'low')     AS d20_b1_low,
  COUNT(*) FILTER (WHERE d20_pairings_cov_bucket = 'mid')     AS d20_b2_mid,
  COUNT(*) FILTER (WHERE d20_pairings_cov_bucket = 'high')    AS d20_b3_high,

  MIN(d10_shiftcharts_coverage_rate) AS d10_cov_min,
  MAX(d10_shiftcharts_coverage_rate) AS d10_cov_max,
  MIN(d20_shiftcharts_coverage_rate) AS d20_cov_min,
  MAX(d20_shiftcharts_coverage_rate) AS d20_cov_max
FROM nhl.training_features_nhl_sog_enriched_pregame_v2
WHERE game_date = DATE :'slate_date';
