-- MLB_2026_EXACT_GAME_STAT_DERIVED_FOUNDATION_V1 rollback body.
-- PREPARED ONLY.  Drops only additive V1 objects created by the paired body.

DROP TABLE IF EXISTS mlb.player_game_feature_state_v1;
DROP FUNCTION IF EXISTS mlb.reject_player_game_feature_state_v1_mutation();
