-- PLAN ONLY: do not apply until a dedicated login is approved.
BEGIN;
CREATE ROLE mlb_cleanroom_research NOLOGIN;
REVOKE ALL ON SCHEMA public, mlb FROM mlb_cleanroom_research;
GRANT USAGE ON SCHEMA mlb_cleanroom_v1 TO mlb_cleanroom_research;
GRANT SELECT ON ALL TABLES IN SCHEMA mlb_cleanroom_v1 TO mlb_cleanroom_research;
ALTER DEFAULT PRIVILEGES IN SCHEMA mlb_cleanroom_v1
  GRANT SELECT ON TABLES TO mlb_cleanroom_research;
COMMIT;
