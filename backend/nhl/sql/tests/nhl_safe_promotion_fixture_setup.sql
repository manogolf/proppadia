-- Run only in a fresh, disposable PostgreSQL database. This is a test fixture,
-- not a production setup or application migration.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_ingest') THEN
    CREATE ROLE proppadia_nhl_ingest NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_daily_login') THEN
    CREATE ROLE proppadia_nhl_daily_login NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_source_correction') THEN
    CREATE ROLE proppadia_nhl_source_correction NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_research_ro') THEN
    CREATE ROLE proppadia_research_ro NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_ops_writer') THEN
    CREATE ROLE proppadia_nhl_ops_writer NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_research_login') THEN
    CREATE ROLE proppadia_nhl_research_login NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_operations_login') THEN
    CREATE ROLE proppadia_nhl_operations_login NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN CREATE ROLE service_role NOLOGIN; END IF;
END
$$;

CREATE SCHEMA nhl;
CREATE TABLE nhl.games(game_id bigint PRIMARY KEY, game_date date NOT NULL);
CREATE TABLE nhl.import_skater_logs_stage(
  player_id bigint NOT NULL, game_id bigint NOT NULL, game_date date NOT NULL,
  shot_attempts integer, pp_toi_minutes numeric(5,2),
  PRIMARY KEY(player_id,game_id));
CREATE TABLE nhl.skater_game_logs_raw(
  player_id bigint NOT NULL, game_id bigint NOT NULL, game_date date NOT NULL,
  shot_attempts integer, pp_toi_minutes numeric(5,2), toi_minutes numeric(5,2),
  shots_on_goal integer, team_id bigint, opponent_id bigint, is_home boolean,
  blocks integer, hits integer, fenwick_for numeric, missed_shots integer,
  blocked_shots_taken integer, rebounds_for integer, takeaways integer,
  giveaways integer, penalties_drawn integer, penalties_taken integer,
  ev_shot_attempts numeric, pp_shot_attempts numeric, sh_shot_attempts numeric,
  ev_sog integer, pp_sog integer, sh_sog integer, goals integer,
  assists integer, points integer,
  PRIMARY KEY(player_id,game_id));
CREATE TABLE nhl.game_manpower_segments(
  game_id bigint NOT NULL, period integer NOT NULL, start_sec integer NOT NULL,
  end_sec integer NOT NULL, pp_team_id bigint NOT NULL);
CREATE TABLE nhl.shiftcharts_raw(
  game_id bigint NOT NULL, player_id bigint, team_id bigint, period integer,
  start_sec integer, end_sec integer);

GRANT USAGE ON SCHEMA nhl TO proppadia_nhl_ingest,
  proppadia_nhl_source_correction, proppadia_nhl_research_login,
  proppadia_nhl_operations_login;
GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT proppadia_nhl_ingest TO proppadia_nhl_daily_login;
GRANT INSERT (player_id,game_id,game_date,shot_attempts,pp_toi_minutes,toi_minutes,shots_on_goal)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT UPDATE (game_date,toi_minutes,shots_on_goal)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT SELECT,INSERT,UPDATE,DELETE ON nhl.import_skater_logs_stage TO proppadia_nhl_ingest;
GRANT SELECT ON nhl.games,nhl.game_manpower_segments,nhl.shiftcharts_raw TO proppadia_nhl_ingest;
GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;
GRANT UPDATE (shot_attempts,pp_toi_minutes)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;
-- Test harness only: let the fixture administrator simulate the NOLOGIN
-- daily identity with SET LOCAL ROLE. The cleanup script revokes this.
GRANT proppadia_nhl_daily_login TO postgres;
