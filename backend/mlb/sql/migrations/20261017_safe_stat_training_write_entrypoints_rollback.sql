-- Fail-closed rollback for the fixed MLB write entrypoints.
-- Keep direct table DML revoked: restoring the former protected-column
-- UPDATE grants would re-enable arbitrary historical overwrites. Until the
-- forward migration is reapplied, these two writers remain intentionally
-- unavailable to proppadia_mlb_ingest.

REVOKE ALL ON FUNCTION mlb.safe_upsert_player_stats(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_upsert_player_stats(jsonb) FROM proppadia_mlb_ingest;
REVOKE ALL ON FUNCTION mlb.safe_fill_player_stats_at_bats(bigint,bigint,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_fill_player_stats_at_bats(bigint,bigint,integer) FROM proppadia_mlb_ingest;
REVOKE ALL ON FUNCTION mlb.safe_upsert_model_training_props(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_upsert_model_training_props(jsonb) FROM proppadia_mlb_ingest;
DROP FUNCTION IF EXISTS mlb.safe_upsert_player_stats(jsonb);
DROP FUNCTION IF EXISTS mlb.safe_fill_player_stats_at_bats(bigint,bigint,integer);
DROP FUNCTION IF EXISTS mlb.safe_upsert_model_training_props(jsonb);

