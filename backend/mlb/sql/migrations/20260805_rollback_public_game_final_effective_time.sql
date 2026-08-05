BEGIN;
ALTER TABLE mlb.public_game_official_finals
  DROP COLUMN IF EXISTS official_final_effective_utc;
COMMIT;
