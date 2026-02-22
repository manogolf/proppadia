-- backend/nhl/sql/reset_2025.sql
-- Reset *only* 2025 NHL data (by season from nhl.games) and leave 2023/2024 untouched.
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/sql/reset_2025.sql

\set ON_ERROR_STOP on

BEGIN;

-- 1) Build the authoritative 2025 game_id set from nhl.games
DROP TABLE IF EXISTS _g2025;
CREATE TEMP TABLE _g2025 AS
SELECT game_id
FROM nhl.games
WHERE season = 2025;

-- Quick preflight counts (what will be affected)
\echo '--- Preflight (rows tied to 2025 games) ---'
SELECT 'g2025_games' AS k, COUNT(*)::bigint AS v FROM _g2025;

DO $$
DECLARE
  n bigint;
BEGIN
  IF to_regclass('nhl.skater_game_logs_raw') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.skater_game_logs_raw s JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'skater_game_logs_raw rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.goalie_game_logs_raw') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.goalie_game_logs_raw t JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'goalie_game_logs_raw rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.roster_status') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.roster_status r JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'roster_status rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.predictions') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.predictions p JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'predictions rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.predictions_sog_stage') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.predictions_sog_stage p JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'predictions_sog_stage rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.predictions_saves_stage') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.predictions_saves_stage p JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'predictions_saves_stage rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.predictions_points_stage') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.predictions_points_stage p JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'predictions_points_stage rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.import_skater_logs_stage') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.import_skater_logs_stage s JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'import_skater_logs_stage rows (2025 games): %', n;
  END IF;

  IF to_regclass('nhl.import_goalie_logs_stage') IS NOT NULL THEN
    EXECUTE 'SELECT COUNT(*) FROM nhl.import_goalie_logs_stage s JOIN _g2025 g USING (game_id)' INTO n;
    RAISE NOTICE 'import_goalie_logs_stage rows (2025 games): %', n;
  END IF;
END $$;

-- 2) Delete children first (safe order)
\echo '--- Deleting 2025 child rows ---'

DO $$
BEGIN
  -- Predictions (wide/long)
  IF to_regclass('nhl.predictions') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.predictions p USING _g2025 g WHERE p.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.predictions';
  END IF;

  IF to_regclass('nhl.predictions_sog_stage') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.predictions_sog_stage p USING _g2025 g WHERE p.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.predictions_sog_stage';
  END IF;

  IF to_regclass('nhl.predictions_saves_stage') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.predictions_saves_stage p USING _g2025 g WHERE p.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.predictions_saves_stage';
  END IF;

  IF to_regclass('nhl.predictions_points_stage') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.predictions_points_stage p USING _g2025 g WHERE p.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.predictions_points_stage';
  END IF;

  -- Staging/import tables (if you persist them)
  IF to_regclass('nhl.import_skater_logs_stage') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.import_skater_logs_stage s USING _g2025 g WHERE s.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.import_skater_logs_stage';
  END IF;

  IF to_regclass('nhl.import_goalie_logs_stage') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.import_goalie_logs_stage s USING _g2025 g WHERE s.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.import_goalie_logs_stage';
  END IF;

  -- Roster rows (per-game)
  IF to_regclass('nhl.roster_status') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.roster_status r USING _g2025 g WHERE r.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.roster_status';
  END IF;

  -- Raw logs (per-game)
  IF to_regclass('nhl.skater_game_logs_raw') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.skater_game_logs_raw s USING _g2025 g WHERE s.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.skater_game_logs_raw';
  END IF;

  IF to_regclass('nhl.goalie_game_logs_raw') IS NOT NULL THEN
    EXECUTE 'DELETE FROM nhl.goalie_game_logs_raw t USING _g2025 g WHERE t.game_id = g.game_id';
    RAISE NOTICE 'deleted from nhl.goalie_game_logs_raw';
  END IF;

  -- Optional: if you have per-game feature tables/materializations persisted as tables, add them here
  -- Example pattern (uncomment + rename if applicable):
  -- IF to_regclass('nhl.training_features_sog_denali') IS NOT NULL THEN
  --   EXECUTE 'DELETE FROM nhl.training_features_sog_denali f USING _g2025 g WHERE f.game_id = g.game_id';
  --   RAISE NOTICE 'deleted from nhl.training_features_sog_denali';
  -- END IF;

END $$;

-- 3) Delete the 2025 games last
\echo '--- Deleting 2025 games ---'
DELETE FROM nhl.games gg
USING _g2025 g
WHERE gg.game_id = g.game_id;

COMMIT;

\echo '--- Postflight (confirm 2025 is empty) ---'
SELECT 'games_2025_remaining' AS k, COUNT(*)::bigint AS v
FROM nhl.games
WHERE season = 2025;
