BEGIN;

CREATE SCHEMA IF NOT EXISTS mlb_cleanroom_v1;
COMMENT ON SCHEMA mlb_cleanroom_v1 IS
  'Forward-only authoritative MLB source foundation. No inherited derived dependencies.';

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.ingestion_runs (
  ingestion_run_id uuid PRIMARY KEY,
  source_name text NOT NULL,
  started_at_utc timestamptz NOT NULL,
  completed_at_utc timestamptz,
  requested_slate_date date NOT NULL,
  rows_received integer NOT NULL DEFAULT 0 CHECK (rows_received >= 0),
  rows_written integer NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
  duplicate_rows_skipped integer NOT NULL DEFAULT 0 CHECK (duplicate_rows_skipped >= 0),
  identity_rejects integer NOT NULL DEFAULT 0 CHECK (identity_rejects >= 0),
  status text NOT NULL,
  error_summary text,
  raw_payload_location text NOT NULL,
  raw_payload_sha256 text NOT NULL CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.teams (
  team_mlb_id bigint NOT NULL,
  team_abbreviation text NOT NULL,
  team_name text NOT NULL,
  league text,
  division text,
  valid_from date NOT NULL,
  valid_to date,
  source text NOT NULL,
  source_observed_at_utc timestamptz NOT NULL,
  ingested_at_utc timestamptz NOT NULL,
  source_payload_sha256 text NOT NULL CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (team_mlb_id, valid_from, source_payload_sha256)
);

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.players (
  player_mlb_id bigint NOT NULL,
  full_name text NOT NULL,
  active_status text NOT NULL,
  primary_position text,
  current_team_mlb_id bigint,
  valid_from date NOT NULL,
  valid_to date,
  source text NOT NULL,
  source_observed_at_utc timestamptz NOT NULL,
  ingested_at_utc timestamptz NOT NULL,
  source_payload_sha256 text NOT NULL CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (player_mlb_id, valid_from, source_payload_sha256)
);

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.games (
  game_pk bigint NOT NULL,
  slate_date date NOT NULL,
  official_game_date date NOT NULL,
  home_team_mlb_id bigint NOT NULL,
  away_team_mlb_id bigint NOT NULL,
  scheduled_start_utc timestamptz NOT NULL,
  game_status text NOT NULL,
  source text NOT NULL,
  source_observed_at_utc timestamptz NOT NULL,
  ingested_at_utc timestamptz NOT NULL,
  source_payload_sha256 text NOT NULL CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (game_pk, source_payload_sha256)
);

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.lineup_snapshots (
  game_pk bigint NOT NULL,
  slate_date date NOT NULL,
  team_mlb_id bigint NOT NULL,
  player_mlb_id bigint NOT NULL,
  batting_order_position smallint CHECK (batting_order_position BETWEEN 1 AND 9),
  lineup_status text NOT NULL,
  snapshot_timestamp_utc timestamptz NOT NULL,
  source text NOT NULL,
  ingestion_run_id uuid NOT NULL REFERENCES mlb_cleanroom_v1.ingestion_runs(ingestion_run_id)
    DEFERRABLE INITIALLY DEFERRED,
  source_payload_sha256 text NOT NULL CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (game_pk, team_mlb_id, player_mlb_id, snapshot_timestamp_utc, source_payload_sha256)
);

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.odds_snapshots (
  game_pk bigint NOT NULL,
  slate_date date NOT NULL,
  book text NOT NULL,
  market text NOT NULL,
  player_mlb_id bigint NOT NULL,
  line numeric NOT NULL,
  side text NOT NULL CHECK (side IN ('Over','Under')),
  american_odds integer NOT NULL,
  snapshot_timestamp_utc timestamptz NOT NULL,
  source text NOT NULL,
  ingestion_run_id uuid NOT NULL REFERENCES mlb_cleanroom_v1.ingestion_runs(ingestion_run_id)
    DEFERRABLE INITIALLY DEFERRED,
  source_payload_sha256 text NOT NULL CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (game_pk, book, market, player_mlb_id, line, side, snapshot_timestamp_utc, source_payload_sha256)
);

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.player_game_results (
  game_pk bigint NOT NULL,
  slate_date date NOT NULL,
  player_mlb_id bigint NOT NULL,
  team_mlb_id bigint NOT NULL,
  plate_appearances integer,
  at_bats integer,
  hits integer,
  singles integer,
  doubles integer,
  triples integer,
  home_runs integer,
  total_bases integer,
  official_game_status text NOT NULL,
  source text NOT NULL,
  source_observed_at_utc timestamptz NOT NULL,
  ingested_at_utc timestamptz NOT NULL,
  source_payload_sha256 text NOT NULL CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (game_pk, player_mlb_id, source_payload_sha256)
);

CREATE OR REPLACE FUNCTION mlb_cleanroom_v1.reject_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'mlb_cleanroom_v1 source tables are append-only';
END $$;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'ingestion_runs','teams','players','games','lineup_snapshots',
    'odds_snapshots','player_game_results'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS reject_mutation ON mlb_cleanroom_v1.%I', t);
    EXECUTE format(
      'CREATE TRIGGER reject_mutation BEFORE UPDATE OR DELETE ON mlb_cleanroom_v1.%I '
      'FOR EACH ROW EXECUTE FUNCTION mlb_cleanroom_v1.reject_mutation()', t
    );
  END LOOP;
END $$;

CREATE OR REPLACE VIEW mlb_cleanroom_v1.current_games AS
SELECT DISTINCT ON (game_pk) *
FROM mlb_cleanroom_v1.games
ORDER BY game_pk, source_observed_at_utc DESC, source_payload_sha256 DESC;
COMMENT ON VIEW mlb_cleanroom_v1.current_games IS
  'Grain game_pk; deterministic latest authoritative game observation.';

CREATE OR REPLACE VIEW mlb_cleanroom_v1.latest_lineups AS
SELECT DISTINCT ON (game_pk, team_mlb_id, player_mlb_id) *
FROM mlb_cleanroom_v1.lineup_snapshots
ORDER BY game_pk, team_mlb_id, player_mlb_id,
         snapshot_timestamp_utc DESC, source_payload_sha256 DESC;
COMMENT ON VIEW mlb_cleanroom_v1.latest_lineups IS
  'Grain game_pk/team_mlb_id/player_mlb_id; exact-ID latest lineup snapshot.';

CREATE OR REPLACE VIEW mlb_cleanroom_v1.latest_bol_tb15 AS
SELECT DISTINCT ON (game_pk, player_mlb_id, line, side) *
FROM mlb_cleanroom_v1.odds_snapshots
WHERE book = 'BetOnline' AND market = 'Total Bases' AND line = 1.5
ORDER BY game_pk, player_mlb_id, line, side,
         snapshot_timestamp_utc DESC, source_payload_sha256 DESC;
COMMENT ON VIEW mlb_cleanroom_v1.latest_bol_tb15 IS
  'Grain game_pk/player_mlb_id/line/side; latest exact-ID BetOnline TB 1.5 offer.';

CREATE OR REPLACE VIEW mlb_cleanroom_v1.official_completed_player_games AS
SELECT DISTINCT ON (game_pk, player_mlb_id) *
FROM mlb_cleanroom_v1.player_game_results
WHERE official_game_status = 'Final'
ORDER BY game_pk, player_mlb_id, source_observed_at_utc DESC, source_payload_sha256 DESC;
COMMENT ON VIEW mlb_cleanroom_v1.official_completed_player_games IS
  'Grain game_pk/player_mlb_id; latest official completed player-game result.';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='mlb_cleanroom_research') THEN
    CREATE ROLE mlb_cleanroom_research NOLOGIN NOINHERIT;
  END IF;
END $$;
REVOKE ALL ON SCHEMA mlb_cleanroom_v1 FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA mlb_cleanroom_v1 FROM PUBLIC;
GRANT USAGE ON SCHEMA mlb_cleanroom_v1 TO mlb_cleanroom_research;
GRANT SELECT ON ALL TABLES IN SCHEMA mlb_cleanroom_v1 TO mlb_cleanroom_research;
ALTER DEFAULT PRIVILEGES IN SCHEMA mlb_cleanroom_v1
  GRANT SELECT ON TABLES TO mlb_cleanroom_research;
GRANT mlb_cleanroom_research TO postgres;

COMMIT;
