-- Speed up DB-backed MLB player profile sections.
-- These endpoints filter by a single player, current MLB source, and recent game dates.

CREATE INDEX IF NOT EXISTS idx_mtp_profile_player_date_mlb_api
ON mlb.model_training_props (player_id, game_date DESC)
WHERE prop_source = 'mlb_api';

CREATE INDEX IF NOT EXISTS idx_mtp_profile_player_prop_date_mlb_api
ON mlb.model_training_props (player_id, prop_type, game_date DESC)
WHERE prop_source = 'mlb_api';
