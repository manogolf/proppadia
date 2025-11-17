-- backend/nhl/sql/export_sog.sql
-- Usage:
--   psql "$SUPABASE_DB_URL" \
--     -v ON_ERROR_STOP=1 \
--     -v slate_date=2025-11-15 \
--     -f backend/nhl/sql/export_sog.sql > exports/train_nhl_sog_v2.csv

\set ON_ERROR_STOP on
\set QUIET on
\pset pager off

-- Clear any leftover temp from this session
DROP TABLE IF EXISTS sog_export;

-- Materialize rows for this slate into a temp table
CREATE TEMP TABLE sog_export AS
SELECT
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,
  season,
  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,
  pace_matchup_index,
  role_pp_share,
  rest_days,
  b2b_flag,
  attempts_d10_per60,
  opp_d10_sf_per60,
  team_d10_sa_per60,
  pace_index
FROM nhl.training_features_nhl_sog_v2
WHERE game_date = DATE :'slate_date'
ORDER BY game_id, player_id;

-- Export clean CSV with header only
\copy sog_export TO STDOUT WITH CSV HEADER;
