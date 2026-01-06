-- ============================================================
-- backend/nhl/sql/fill_sog_pairings_for_slate.sql
-- Fill slate-day pairing (teammate overlap) features onto the SOG pregame table.
--
-- Run:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date=2026-01-03 -f backend/nhl/sql/fill_sog_pairings_for_slate.sql
--
-- Requires:
--   - nhl.shiftcharts_pairings_game populated for the slate games (d0)
--   - nhl.shift_teammate_overlap_features_rolling_d10 / d20 exist (rolling)
--   - pregame table exists: nhl.training_features_nhl_sog_enriched_pregame_v2
-- ============================================================

\set ON_ERROR_STOP on

-- 0) Add columns (idempotent)
-- Keep DDL consolidated to reduce fragility in daily automation.
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  -- d0 (same-game pairings)
  ADD COLUMN IF NOT EXISTS d0_top_mate_player_id             bigint NULL,
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_sec           int    NULL,
  ADD COLUMN IF NOT EXISTS d0_top_mate_overlap_share         numeric NULL,
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_avg         numeric NULL,
  ADD COLUMN IF NOT EXISTS d0_top3_overlap_share_std         numeric NULL,

  -- rolling pairings (d10)
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_games             int NULL,
  ADD COLUMN IF NOT EXISTS d10_shiftcharts_coverage_rate      double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top_mate_overlap_share_avg     double precision NULL,
  ADD COLUMN IF NOT EXISTS d10_top3_mates_overlap_share_avg   double precision NULL,

  -- rolling pairings (d20)
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_games             int NULL,
  ADD COLUMN IF NOT EXISTS d20_shiftcharts_coverage_rate      double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top_mate_overlap_share_avg     double precision NULL,
  ADD COLUMN IF NOT EXISTS d20_top3_mates_overlap_share_avg   double precision NULL,

  -- missingness-aware features (computed AFTER rolling fills)
  ADD COLUMN IF NOT EXISTS d10_pairings_available            boolean NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_available            boolean NULL,
  ADD COLUMN IF NOT EXISTS d10_pairings_cov_bucket           smallint NULL,
  ADD COLUMN IF NOT EXISTS d20_pairings_cov_bucket           smallint NULL,

  -- bookkeeping
  ADD COLUMN IF NOT EXISTS pairings_source                   text NULL,
  ADD COLUMN IF NOT EXISTS pairings_updated_at               timestamptz NULL;

-- ============================================================
-- 1) d0 update from shiftcharts_pairings_game (slate games only)
-- ============================================================

WITH games AS (
  SELECT game_id::bigint AS game_id
  FROM nhl.games
  WHERE game_date = DATE :'slate_date'
),
src_d0 AS (
  SELECT
    p.game_id::bigint    AS game_id,
    p.player_id::bigint  AS player_id,
    p.top_mate_player_id::bigint          AS d0_top_mate_player_id,
    p.top_mate_overlap_sec::int           AS d0_top_mate_overlap_sec,
    p.top_mate_overlap_share              AS d0_top_mate_overlap_share,
    p.top3_overlap_share_avg              AS d0_top3_overlap_share_avg,
    p.top3_overlap_share_std              AS d0_top3_overlap_share_std
  FROM nhl.shiftcharts_pairings_game p
  JOIN games g USING (game_id)
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d0_top_mate_player_id     = src_d0.d0_top_mate_player_id,
  d0_top_mate_overlap_sec   = src_d0.d0_top_mate_overlap_sec,
  d0_top_mate_overlap_share = src_d0.d0_top_mate_overlap_share,
  d0_top3_overlap_share_avg = src_d0.d0_top3_overlap_share_avg,
  d0_top3_overlap_share_std = src_d0.d0_top3_overlap_share_std,
  pairings_source           = 'shiftcharts_pairings_game',
  pairings_updated_at       = now()
FROM src_d0
WHERE t.game_date = DATE :'slate_date'
  AND t.game_id   = src_d0.game_id
  AND t.player_id = src_d0.player_id;

-- ============================================================
-- 2) d10 rolling update (slate games only)
-- ============================================================

WITH games AS (
  SELECT game_id::bigint AS game_id
  FROM nhl.games
  WHERE game_date = DATE :'slate_date'
),
src_d10 AS (
  SELECT
    r.game_id::bigint   AS game_id,
    r.player_id::bigint AS player_id,
    r.d10_shiftcharts_games,
    r.d10_shiftcharts_coverage_rate,
    r.d10_top_mate_overlap_share_avg,
    r.d10_top3_mates_overlap_share_avg
  FROM nhl.shift_teammate_overlap_features_rolling_d10 r
  JOIN games g USING (game_id)
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_shiftcharts_games            = src_d10.d10_shiftcharts_games,
  d10_shiftcharts_coverage_rate    = src_d10.d10_shiftcharts_coverage_rate,
  d10_top_mate_overlap_share_avg   = src_d10.d10_top_mate_overlap_share_avg,
  d10_top3_mates_overlap_share_avg = src_d10.d10_top3_mates_overlap_share_avg,
  pairings_source                  = COALESCE(t.pairings_source, '') ||
                                     CASE
                                       WHEN t.pairings_source IS NULL OR t.pairings_source = '' THEN ''
                                       ELSE ';'
                                     END ||
                                     'rolling_d10',
  pairings_updated_at              = now()
FROM src_d10
WHERE t.game_date = DATE :'slate_date'
  AND t.game_id   = src_d10.game_id
  AND t.player_id = src_d10.player_id;

-- ============================================================
-- 3) d20 rolling update (slate games only)
-- ============================================================

WITH games AS (
  SELECT game_id::bigint AS game_id
  FROM nhl.games
  WHERE game_date = DATE :'slate_date'
),
src_d20 AS (
  SELECT
    r.game_id::bigint   AS game_id,
    r.player_id::bigint AS player_id,
    r.d20_shiftcharts_games,
    r.d20_shiftcharts_coverage_rate,
    r.d20_top_mate_overlap_share_avg,
    r.d20_top3_mates_overlap_share_avg
  FROM nhl.shift_teammate_overlap_features_rolling_d20 r
  JOIN games g USING (game_id)
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d20_shiftcharts_games            = src_d20.d20_shiftcharts_games,
  d20_shiftcharts_coverage_rate    = src_d20.d20_shiftcharts_coverage_rate,
  d20_top_mate_overlap_share_avg   = src_d20.d20_top_mate_overlap_share_avg,
  d20_top3_mates_overlap_share_avg = src_d20.d20_top3_mates_overlap_share_avg,
  pairings_source                  = COALESCE(t.pairings_source, '') ||
                                     CASE
                                       WHEN t.pairings_source IS NULL OR t.pairings_source = '' THEN ''
                                       ELSE ';'
                                     END ||
                                     'rolling_d20',
  pairings_updated_at              = now()
FROM src_d20
WHERE t.game_date = DATE :'slate_date'
  AND t.game_id   = src_d20.game_id
  AND t.player_id = src_d20.player_id;

-- ============================================================
-- 4) Missingness-aware features (compute AFTER rolling updates)
-- ============================================================

UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_pairings_available = (t.d10_shiftcharts_games IS NOT NULL AND t.d10_shiftcharts_games > 0),
  d20_pairings_available = (t.d20_shiftcharts_games IS NOT NULL AND t.d20_shiftcharts_games > 0),

  d10_pairings_cov_bucket = CASE
    WHEN t.d10_shiftcharts_coverage_rate IS NULL THEN 0
    WHEN t.d10_shiftcharts_coverage_rate < 0.33 THEN 1
    WHEN t.d10_shiftcharts_coverage_rate < 0.66 THEN 2
    ELSE 3
  END,

  d20_pairings_cov_bucket = CASE
    WHEN t.d20_shiftcharts_coverage_rate IS NULL THEN 0
    WHEN t.d20_shiftcharts_coverage_rate < 0.33 THEN 1
    WHEN t.d20_shiftcharts_coverage_rate < 0.66 THEN 2
    ELSE 3
  END
WHERE t.game_date = DATE :'slate_date';

-- ============================================================
-- 5) Quick coverage summary (useful vs merely non-null)
-- ============================================================

WITH base AS (
  SELECT
    COUNT(*) AS n_rows,

    COUNT(*) FILTER (WHERE d0_top_mate_overlap_share IS NOT NULL) AS n_with_d0,

    COUNT(*) FILTER (WHERE d10_shiftcharts_games IS NOT NULL AND d10_shiftcharts_games > 0) AS n_with_d10,
    COUNT(*) FILTER (WHERE d20_shiftcharts_games IS NOT NULL AND d20_shiftcharts_games > 0) AS n_with_d20
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date = DATE :'slate_date'
)
SELECT
  DATE :'slate_date' AS game_date,
  n_rows,

  n_with_d0,
  (n_rows - n_with_d0)  AS n_missing_d0,

  n_with_d10,
  (n_rows - n_with_d10) AS n_missing_d10,

  n_with_d20,
  (n_rows - n_with_d20) AS n_missing_d20
FROM base;
