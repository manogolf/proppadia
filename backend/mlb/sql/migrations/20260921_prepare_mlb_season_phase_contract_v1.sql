-- PREPARED ONLY. Do not apply before the activation preflight in the V1 runbook.
-- Canonical game tables retain source type once; downstream lanes derive phase
-- by exact game_pk/game_id join unless an immutable source payload already has it.
BEGIN;

ALTER TABLE mlb.game_info
  ADD COLUMN IF NOT EXISTS source_game_type text,
  ADD COLUMN IF NOT EXISTS season_phase text,
  ADD COLUMN IF NOT EXISTS postseason_round text,
  ADD COLUMN IF NOT EXISTS season_name text,
  ADD COLUMN IF NOT EXISTS source_round text,
  ADD COLUMN IF NOT EXISTS schedule_relationships jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE mlb_cleanroom_v1.games
  ADD COLUMN IF NOT EXISTS source_game_type text,
  ADD COLUMN IF NOT EXISTS season_phase text,
  ADD COLUMN IF NOT EXISTS postseason_round text,
  ADD COLUMN IF NOT EXISTS season_name text,
  ADD COLUMN IF NOT EXISTS source_round text,
  ADD COLUMN IF NOT EXISTS schedule_relationships jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE mlb.game_info
  DROP CONSTRAINT IF EXISTS game_info_season_phase_v1,
  ADD CONSTRAINT game_info_season_phase_v1
    CHECK (season_phase IS NULL OR season_phase IN ('PRESEASON','REGULAR_SEASON','POSTSEASON')),
  DROP CONSTRAINT IF EXISTS game_info_phase_round_v1,
  ADD CONSTRAINT game_info_phase_round_v1
    CHECK (
      (season_phase = 'POSTSEASON' AND postseason_round IS NOT NULL)
      OR (season_phase IS DISTINCT FROM 'POSTSEASON' AND postseason_round IS NULL)
    );

ALTER TABLE mlb_cleanroom_v1.games
  DROP CONSTRAINT IF EXISTS games_season_phase_v1,
  ADD CONSTRAINT games_season_phase_v1
    CHECK (season_phase IS NULL OR season_phase IN ('PRESEASON','REGULAR_SEASON','POSTSEASON')),
  DROP CONSTRAINT IF EXISTS games_phase_round_v1,
  ADD CONSTRAINT games_phase_round_v1
    CHECK (
      (season_phase = 'POSTSEASON' AND postseason_round IS NOT NULL)
      OR (season_phase IS DISTINCT FROM 'POSTSEASON' AND postseason_round IS NULL)
    );

CREATE INDEX IF NOT EXISTS idx_game_info_season_phase_v1
  ON mlb.game_info (season_name, season_phase, game_id);
CREATE INDEX IF NOT EXISTS idx_cleanroom_games_season_phase_v1
  ON mlb_cleanroom_v1.games (season_name, season_phase, game_pk);

COMMENT ON COLUMN mlb.game_info.source_game_type IS
  'Exact authoritative MLB StatsAPI gameType. Never reconstructed from a date.';
COMMENT ON COLUMN mlb.game_info.schedule_relationships IS
  'Raw rescheduled/resumed relationship fields retained from authoritative schedule source.';
COMMENT ON COLUMN mlb_cleanroom_v1.games.source_game_type IS
  'Exact authoritative MLB StatsAPI gameType. Never reconstructed from a date.';
COMMENT ON COLUMN mlb_cleanroom_v1.games.schedule_relationships IS
  'Raw rescheduled/resumed relationship fields retained from authoritative schedule source.';

COMMIT;
