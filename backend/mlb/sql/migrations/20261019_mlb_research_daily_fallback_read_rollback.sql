-- Remove only the column SELECT grants added by 20261019_mlb_research_daily_fallback_read.sql.
BEGIN;

REVOKE SELECT (created_at, prediction_payload) ON mlb.public_game_moneyline_predictions FROM proppadia_mlb_research_ro;
REVOKE SELECT (grading_timestamp_utc) ON mlb.public_game_moneyline_outcomes FROM proppadia_mlb_research_ro;
REVOKE SELECT (id, created_at, updated_at) ON mlb.player_ids FROM proppadia_mlb_research_ro;
REVOKE SELECT (starting_pitcher_id_home, starting_pitcher_id_away) ON mlb.game_info FROM proppadia_mlb_research_ro;
REVOKE SELECT (prop_type, game_date, over_under, outcome, predicted_outcome, was_correct, confidence_score, prop_source, game_id, player_id, created_at, status, prop_value, user_id) ON mlb.player_props FROM proppadia_mlb_research_ro;
REVOKE SELECT (over_under, outcome, predicted_outcome, was_correct, confidence_score, player_name, team) ON mlb.model_training_props FROM proppadia_mlb_research_ro;
REVOKE SELECT (id, is_home, created_at, updated_at) ON mlb.player_derived_stats FROM proppadia_mlb_research_ro;

COMMIT;
