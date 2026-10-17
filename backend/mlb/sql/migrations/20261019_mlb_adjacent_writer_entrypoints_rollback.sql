-- Fail-closed rollback: remove only the five fixed adjacent-writer functions
-- and their function-based access. Do not restore direct relation DML.
REVOKE EXECUTE ON FUNCTION mlb.safe_upsert_player_roster(jsonb) FROM proppadia_mlb_ingest;
REVOKE EXECUTE ON FUNCTION mlb.safe_insert_player_id_if_absent(jsonb) FROM proppadia_mlb_ingest;
REVOKE EXECUTE ON FUNCTION mlb.safe_mark_player_roster_inactive(bigint[],integer,integer) FROM proppadia_mlb_ingest;
REVOKE EXECUTE ON FUNCTION mlb.safe_upsert_game_info(jsonb) FROM proppadia_mlb_ingest;
REVOKE EXECUTE ON FUNCTION mlb.safe_upsert_prop_features_precomputed(jsonb) FROM proppadia_mlb_ingest;
DROP FUNCTION mlb.safe_upsert_player_roster(jsonb);
DROP FUNCTION mlb.safe_insert_player_id_if_absent(jsonb);
DROP FUNCTION mlb.safe_mark_player_roster_inactive(bigint[],integer,integer);
DROP FUNCTION mlb.safe_upsert_game_info(jsonb);
DROP FUNCTION mlb.safe_upsert_prop_features_precomputed(jsonb);
REVOKE SELECT ON mlb.game_info FROM proppadia_mlb_ingest;
