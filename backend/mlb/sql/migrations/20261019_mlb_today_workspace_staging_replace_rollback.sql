-- Remove the function added by 20261019_mlb_today_workspace_staging_replace.sql.
-- First return build_mlb_today_workspace.py to its previous route if it was
-- switched to the function. Table rows are not touched.
BEGIN;

REVOKE ALL ON FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb) FROM proppadia_mlb_ingest;
DROP FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb);

COMMIT;
