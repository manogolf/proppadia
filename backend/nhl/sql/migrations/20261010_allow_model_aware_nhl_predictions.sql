-- Retire the legacy key that omitted model identity.  The generic prediction
-- loader already upserts on (prop, player, game, line, feature_hash), which
-- permits production and shadow models to coexist for the same observation.
-- The model-aware unique constraint/index remains in place.
DROP INDEX IF EXISTS nhl.uq_nhl_predictions_player_game_prop_line;
