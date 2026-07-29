BEGIN;

CREATE TABLE IF NOT EXISTS mlb_cleanroom_v1.odds_player_identity_bridge (
  provider text NOT NULL,
  provider_event_id text NOT NULL,
  game_pk bigint NOT NULL,
  raw_player_name text NOT NULL,
  normalized_player_name text NOT NULL,
  player_mlb_id bigint NOT NULL,
  official_player_name text NOT NULL,
  normalization_version text NOT NULL,
  decision text NOT NULL CHECK (decision = 'EXACT_UNIQUE_MATCH'),
  decision_reason text NOT NULL,
  source_observed_at_utc timestamptz NOT NULL,
  ingestion_run_id uuid NOT NULL
    REFERENCES mlb_cleanroom_v1.ingestion_runs(ingestion_run_id)
    DEFERRABLE INITIALLY DEFERRED,
  raw_payload_sha256 text NOT NULL CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
  created_at_utc timestamptz NOT NULL,
  PRIMARY KEY (
    provider, provider_event_id, game_pk, raw_player_name,
    player_mlb_id, raw_payload_sha256
  )
);

DROP TRIGGER IF EXISTS reject_mutation
  ON mlb_cleanroom_v1.odds_player_identity_bridge;
CREATE TRIGGER reject_mutation
BEFORE UPDATE OR DELETE ON mlb_cleanroom_v1.odds_player_identity_bridge
FOR EACH ROW EXECUTE FUNCTION mlb_cleanroom_v1.reject_mutation();

GRANT SELECT ON mlb_cleanroom_v1.odds_player_identity_bridge
  TO mlb_cleanroom_research;

COMMIT;
