-- Revoke only the BvP verifier's column-level read grant.
BEGIN;

REVOKE SELECT (
    computed_at, feature_set_tag, features, game_date,
    game_id, model_tag, player_id, prop_type
) ON mlb.prop_features_precomputed FROM proppadia_mlb_research_ro;

COMMIT;
