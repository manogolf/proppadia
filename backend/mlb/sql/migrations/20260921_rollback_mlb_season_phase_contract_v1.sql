-- PREPARED ROLLBACK ONLY. This removes only V1 phase-contract objects.
BEGIN;

DROP VIEW IF EXISTS mlb.canonical_game_phase_v1;

DROP INDEX IF EXISTS mlb.idx_game_info_season_phase_v1;
DROP INDEX IF EXISTS mlb_cleanroom_v1.idx_cleanroom_games_season_phase_v1;

ALTER TABLE mlb.game_info
  DROP CONSTRAINT IF EXISTS game_info_season_phase_v1,
  DROP CONSTRAINT IF EXISTS game_info_phase_round_v1,
  DROP CONSTRAINT IF EXISTS game_info_phase_contract_v1,
  DROP CONSTRAINT IF EXISTS game_info_phase_source_sha256_v1,
  DROP CONSTRAINT IF EXISTS game_info_schedule_relationships_object_v1,
  DROP COLUMN IF EXISTS game_type_source_sha256,
  DROP COLUMN IF EXISTS schedule_relationships,
  DROP COLUMN IF EXISTS source_round,
  DROP COLUMN IF EXISTS season_name,
  DROP COLUMN IF EXISTS postseason_round,
  DROP COLUMN IF EXISTS season_phase,
  DROP COLUMN IF EXISTS source_game_type,
  DROP COLUMN IF EXISTS source_season;

ALTER TABLE mlb_cleanroom_v1.games
  DROP CONSTRAINT IF EXISTS games_season_phase_v1,
  DROP CONSTRAINT IF EXISTS games_phase_round_v1,
  DROP CONSTRAINT IF EXISTS games_phase_contract_v1,
  DROP CONSTRAINT IF EXISTS games_schedule_relationships_object_v1,
  DROP COLUMN IF EXISTS schedule_relationships,
  DROP COLUMN IF EXISTS source_round,
  DROP COLUMN IF EXISTS season_name,
  DROP COLUMN IF EXISTS postseason_round,
  DROP COLUMN IF EXISTS season_phase,
  DROP COLUMN IF EXISTS source_game_type,
  DROP COLUMN IF EXISTS source_season;

COMMIT;
