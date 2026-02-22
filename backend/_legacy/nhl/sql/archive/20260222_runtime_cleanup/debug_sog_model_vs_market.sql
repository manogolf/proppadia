-- ============================================================
-- debug_sog_model_vs_market.sql
-- Purpose: Quickly verify whether the *inputs* and/or *outputs*
--          are collapsing your SOG model to ~50/50.
--
-- Run (example):
-- psql "$SUPABASE_DB_URL" \
--  --no-psqlrc \
--  --pset pager=off \
--  -v ON_ERROR_STOP=1 \
--  -v slate_date=2026-01-07 \
--  -v player_id=8473994 \
--  -f backend/nhl/sql/debug_sog_model_vs_market.sql
--
--
-- Notes:
-- - This script is SAFE (read-only).
-- - It assumes your pregame features live in:
--     nhl.training_features_nhl_sog_enriched_pregame_v2
-- - It also tries to *discover* any table/view with columns like
--   p_over/prob_over/calibrated for SOG predictions.
-- ============================================================

\set ON_ERROR_STOP on
\pset pager off
\pset footer on
\pset format aligned

\echo '============================================================'
\echo '0) Confirm variables'
\echo '============================================================'
SELECT DATE :'slate_date' AS slate_date, :'player_id'::bigint AS player_id;

\echo '============================================================'
\echo '1) Slate sanity: games + pregame row counts'
\echo '============================================================'
SELECT COUNT(*) AS games_on_slate
FROM nhl.games
WHERE game_date = DATE :'slate_date';

SELECT
  game_date,
  COUNT(*) AS pregame_rows
FROM nhl.training_features_nhl_sog_enriched_pregame_v2
WHERE game_date = DATE :'slate_date'
GROUP BY 1;

\echo '============================================================'
\echo '2) Feature completeness distribution (slate-wide)'
\echo '   If these are mostly NULL/0, your model will flatten to ~0.5'
\echo '============================================================'

-- Adjust this list if your model expects more/less columns.
WITH s AS (
  SELECT *
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date = DATE :'slate_date'
)
SELECT
  COUNT(*) AS n_rows,

  -- Core rolling features (Denali-ish)
  COUNT(*) FILTER (WHERE d5_sog_per60  IS NULL) AS n_d5_sog_per60_null,
  COUNT(*) FILTER (WHERE d10_sog_per60 IS NULL) AS n_d10_sog_per60_null,
  COUNT(*) FILTER (WHERE d20_sog_per60 IS NULL) AS n_d20_sog_per60_null,
  COUNT(*) FILTER (WHERE attempts_d10_per60 IS NULL) AS n_attempts_d10_per60_null,

  -- Pace + team context
  COUNT(*) FILTER (WHERE pace_matchup_index IS NULL) AS n_pace_matchup_index_null,
  COUNT(*) FILTER (WHERE team_d10_sf_per_game IS NULL) AS n_team_d10_sf_per_game_null,
  COUNT(*) FILTER (WHERE opp_d10_sf_allowed_per_game IS NULL) AS n_opp_d10_sf_allowed_per_game_null,

  -- Pairings (you recently changed these; should not drive 50/50 alone, but check)
  COUNT(*) FILTER (WHERE d10_shiftcharts_games IS NULL) AS n_d10_shiftcharts_games_null,
  COUNT(*) FILTER (WHERE d20_shiftcharts_games IS NULL) AS n_d20_shiftcharts_games_null

FROM s;

\echo '============================================================'
\echo '3) Feature ranges (slate-wide)'
\echo '   Look for tiny ranges or all-zeros (bad joins/fills)'
\echo '============================================================'
WITH s AS (
  SELECT *
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date = DATE :'slate_date'
)
SELECT
  -- rolling
  MIN(d10_sog_per60) AS min_d10_sog_per60,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d10_sog_per60) AS med_d10_sog_per60,
  MAX(d10_sog_per60) AS max_d10_sog_per60,

  MIN(attempts_d10_per60) AS min_attempts_d10_per60,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY attempts_d10_per60) AS med_attempts_d10_per60,
  MAX(attempts_d10_per60) AS max_attempts_d10_per60,

  -- pace
  MIN(pace_matchup_index) AS min_pace_matchup_index,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pace_matchup_index) AS med_pace_matchup_index,
  MAX(pace_matchup_index) AS max_pace_matchup_index

FROM s;

\echo '============================================================'
\echo '4) Single-player feature row (the one you are questioning)'
\echo '   If this looks null/0-heavy for a star, that explains -110-ish'
\echo '============================================================'
SELECT
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,

  -- core rolling
  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  attempts_d10_per60,

  -- pace/team context
  pace_matchup_index,
  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,

  -- pairings (optional)
  d10_shiftcharts_games,
  d10_shiftcharts_coverage_rate,
  d20_shiftcharts_games,
  d20_shiftcharts_coverage_rate

FROM nhl.training_features_nhl_sog_enriched_pregame_v2
WHERE game_date = DATE :'slate_date'
  AND player_id = :'player_id'::bigint
ORDER BY game_id
LIMIT 50;

\echo '============================================================'
\echo '5) Discover WHERE your SOG predictions live (tables/views)'
\echo '   This finds candidates containing p_over/prob_over/calibration-ish cols'
\echo '============================================================'
SELECT
  c.table_schema,
  c.table_name,
  STRING_AGG(c.column_name, ', ' ORDER BY c.column_name) AS matched_columns
FROM information_schema.columns c
WHERE c.table_schema = 'nhl'
  AND (
    c.column_name ILIKE '%p_over%' OR
    c.column_name ILIKE '%prob_over%' OR
    c.column_name ILIKE '%calibr%' OR
    c.column_name ILIKE '%fair%' OR
    c.column_name ILIKE '%odds%' OR
    c.column_name ILIKE '%shots_on_goal%' OR
    c.column_name ILIKE '%sog%'
  )
GROUP BY 1,2
ORDER BY 1,2;

\echo '============================================================'
\echo '6) If you know the exact prediction table/view name, paste it here'
\echo '   (Optional) This block is disabled by default.'
\echo '   Enable by replacing <PRED_TABLE> and uncommenting.'
\echo '============================================================'
-- Example enable:
-- \set pred_table nhl.sog_predictions_latest
-- SELECT * FROM :pred_table WHERE game_date = DATE :'slate_date' AND player_id = :'player_id'::bigint;

\echo '============================================================'
\echo 'Done. Paste outputs of sections 2, 3, 4, and the result of section 5.'
\echo '============================================================'
