BEGIN;

ALTER TABLE mlb.player_stats
ADD COLUMN IF NOT EXISTS at_bats integer;

ALTER TABLE mlb.player_derived_stats
ADD COLUMN IF NOT EXISTS d7_at_bats numeric,
ADD COLUMN IF NOT EXISTS d15_at_bats numeric,
ADD COLUMN IF NOT EXISTS d30_at_bats numeric;

COMMIT;

