-- Fail-closed rollback: remove only the fixed prediction backfill entry point.
REVOKE EXECUTE ON FUNCTION mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamptz) FROM proppadia_mlb_ingest;
REVOKE ALL ON FUNCTION mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamptz) FROM PUBLIC;
DROP FUNCTION mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamptz);
