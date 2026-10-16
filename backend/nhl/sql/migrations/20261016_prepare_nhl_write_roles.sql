-- PREPARATION MIGRATION ONLY — DO NOT APPLY WITHOUT SEPARATE AUTHORIZATION.
-- Creates NOLOGIN permission groups and grants a narrow NHL foundation slice.
-- It does not revoke existing owner/API privileges, create credentials, change
-- the active daily connection, or grant these roles to any login.
-- After separate activation approval and secure login creation, the intended
-- memberships are:
--   GRANT proppadia_research_ro TO proppadia_nhl_research_login;
--   GRANT proppadia_nhl_ingest TO proppadia_nhl_daily_login;
-- The correction group is granted only to a controlled operator session.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_research_ro') THEN
    CREATE ROLE proppadia_research_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_nhl_ingest') THEN
    CREATE ROLE proppadia_nhl_ingest NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_nhl_source_correction') THEN
    CREATE ROLE proppadia_nhl_source_correction NOLOGIN NOSUPERUSER NOCREATEDB
      NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

ALTER ROLE proppadia_research_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS;
ALTER ROLE proppadia_nhl_ingest NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS;
ALTER ROLE proppadia_nhl_source_correction NOLOGIN NOSUPERUSER NOCREATEDB
  NOCREATEROLE NOREPLICATION NOBYPASSRLS;

GRANT CONNECT ON DATABASE postgres TO
  proppadia_research_ro, proppadia_nhl_ingest, proppadia_nhl_source_correction;
GRANT USAGE ON SCHEMA nhl TO
  proppadia_research_ro, proppadia_nhl_ingest, proppadia_nhl_source_correction;

GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_research_ro;

GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT INSERT (player_id, game_id, team_id, opponent_id, is_home, game_date,
              shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes, blocks)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT UPDATE (team_id, opponent_id, is_home, game_date, shots_on_goal,
              shot_attempts, toi_minutes, pp_toi_minutes, blocks)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON nhl.import_skater_logs_stage TO proppadia_nhl_ingest;
GRANT SELECT ON nhl.import_skater_logs_stage TO proppadia_research_ro;

GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;
GRANT UPDATE (shot_attempts, pp_toi_minutes)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;

-- There are intentionally no grants here to postgres, anon, authenticated,
-- service_role, an application login, or any correction login. Application
-- membership and all other tables must be reviewed in the cutover migration.
