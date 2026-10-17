-- Fail-closed rollback: remove only the PA backfill entry point. Do not restore
-- direct UPDATE on mlb.player_stats.
REVOKE ALL ON FUNCTION mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text) FROM proppadia_mlb_ingest;
DROP FUNCTION IF EXISTS mlb.safe_fill_player_stats_pa(bigint,bigint,integer,integer,integer,integer,integer,text);
