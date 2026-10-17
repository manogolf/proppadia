BEGIN;

REVOKE EXECUTE ON FUNCTION mlb.refresh_today_workspace_views() FROM proppadia_mlb_ingest;
DROP FUNCTION mlb.refresh_today_workspace_views();

COMMIT;
