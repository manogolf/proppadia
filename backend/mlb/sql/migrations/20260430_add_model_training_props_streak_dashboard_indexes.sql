-- Speed up MLB streak dashboard aggregation over recent model-backed prop rows.

CREATE INDEX IF NOT EXISTS idx_mtp_streak_dashboard_date_prop_player_mlb_api
ON mlb.model_training_props (game_date DESC, prop_type, player_id, game_id)
WHERE prop_source = 'mlb_api';

CREATE INDEX IF NOT EXISTS idx_mtp_streak_dashboard_player_prop_date_mlb_api
ON mlb.model_training_props (player_id, prop_type, game_date DESC, game_id DESC)
WHERE prop_source = 'mlb_api';
