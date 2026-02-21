-- Persist MLB schedule game type on props/training rows.
-- Supports redundant preseason cleanup gates (date window + game_type).

ALTER TABLE IF EXISTS mlb.model_training_props
  ADD COLUMN IF NOT EXISTS game_type text;

ALTER TABLE IF EXISTS mlb.player_props
  ADD COLUMN IF NOT EXISTS game_type text;

-- Optional narrow index for date-window + type cleanup scans.
CREATE INDEX IF NOT EXISTS idx_mtp_game_date_game_type_mlb_api
  ON mlb.model_training_props (game_date, game_type)
  WHERE prop_source = 'mlb_api';
