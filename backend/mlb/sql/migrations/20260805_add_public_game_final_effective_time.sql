BEGIN;
ALTER TABLE mlb.public_game_official_finals
  ADD COLUMN IF NOT EXISTS official_final_effective_utc timestamptz;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM mlb.public_game_official_finals WHERE official_final_effective_utc IS NULL) THEN
    RAISE EXCEPTION 'existing official-final rows require explicit effective-time backfill';
  END IF;
END $$;
ALTER TABLE mlb.public_game_official_finals
  ALTER COLUMN official_final_effective_utc SET NOT NULL;
COMMIT;
