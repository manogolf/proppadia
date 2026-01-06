-- ============================================================
-- Fill rolling teammate-overlap features for a slate (d10 + optional d20)
--
-- Usage (psql):
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date=YYYY-MM-DD \
--     -f backend/nhl/sql/fill_sog_pairings_rolling_for_slate.sql
-- ============================================================

\set ON_ERROR_STOP on

-- --- d10 rolling fill (requires view exists; you already have it) ---
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  d10_top_mate_overlap_share_avg   = ov.d10_top_mate_overlap_share_avg,
  d10_top_mate_overlap_share_std   = ov.d10_top_mate_overlap_share_std,
  d10_top3_mates_overlap_share_avg = ov.d10_top3_mates_overlap_share_avg,
  d10_top3_mates_overlap_share_std = ov.d10_top3_mates_overlap_share_std,
  d10_games_in_window              = ov.d10_games_in_window,
  d10_shiftcharts_games            = ov.d10_shiftcharts_games,
  d10_shiftcharts_coverage_rate     = ov.d10_shiftcharts_coverage_rate
FROM nhl.shift_teammate_overlap_features_rolling_d10 ov
WHERE t.game_date = DATE :'slate_date'
  AND ov.game_date = t.game_date
  AND ov.game_id   = t.game_id
  AND ov.player_id = t.player_id;

-- --- d20 rolling fill (only if the view exists) ---
SELECT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'nhl'
    AND c.relname = 'shift_teammate_overlap_features_rolling_d20'
) AS has_d20 \gset

\if :has_d20
  UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
  SET
    d20_top_mate_overlap_share_avg   = ov.d20_top_mate_overlap_share_avg,
    d20_top_mate_overlap_share_std   = ov.d20_top_mate_overlap_share_std,
    d20_top3_mates_overlap_share_avg = ov.d20_top3_mates_overlap_share_avg,
    d20_top3_mates_overlap_share_std = ov.d20_top3_mates_overlap_share_std,
    d20_games_in_window              = ov.d20_games_in_window,
    d20_shiftcharts_games            = ov.d20_shiftcharts_games,
    d20_shiftcharts_coverage_rate     = ov.d20_shiftcharts_coverage_rate
  FROM nhl.shift_teammate_overlap_features_rolling_d20 ov
  WHERE t.game_date = DATE :'slate_date'
    AND ov.game_date = t.game_date
    AND ov.game_id   = t.game_id
    AND ov.player_id = t.player_id;
\else
  \echo 'NOTE: nhl.shift_teammate_overlap_features_rolling_d20 does not exist; skipping d20 fill.'
\endif
