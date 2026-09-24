-- MLB_2026_EXACT_GAME_STAT_DERIVED_FOUNDATION_V1
-- PREPARED ONLY: do not apply as part of this source-only foundation task.
-- Transaction-neutral additive body.  An authorized activation must execute
-- it in an explicit transaction with ON_ERROR_STOP enabled.

CREATE TABLE IF NOT EXISTS mlb.player_game_feature_state_v1 (
  player_id bigint NOT NULL CHECK (player_id > 0),
  game_pk bigint NOT NULL CHECK (game_pk > 0),
  contract_version text NOT NULL CHECK (contract_version = 'MLB_PLAYER_GAME_FEATURE_STATE_V1'),
  official_game_date date NOT NULL,
  scheduled_start_utc timestamptz NOT NULL,
  feature_input_cutoff_utc timestamptz NOT NULL,
  phase_authority_interface text NOT NULL,
  phase_authority_snapshot_id text NOT NULL CHECK (phase_authority_snapshot_id <> ''),
  phase_authority_descriptor_sha256 text NOT NULL
    CHECK (phase_authority_descriptor_sha256 ~ '^[0-9a-f]{64}$'),
  source_season integer NOT NULL CHECK (source_season > 0),
  source_game_type text NOT NULL CHECK (source_game_type <> ''),
  season_phase text NOT NULL CHECK (season_phase IN ('PRESEASON', 'REGULAR_SEASON', 'POSTSEASON')),
  relationship_evidence jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(relationship_evidence) = 'object'),
  source_observations jsonb NOT NULL
    CHECK (jsonb_typeof(source_observations) = 'array')
    CHECK (jsonb_array_length(source_observations) > 0),
  latest_source_observed_at_utc timestamptz NOT NULL,
  source_paths text[] NOT NULL
    CHECK (cardinality(source_paths) > 0),
  source_sha256s text[] NOT NULL
    CHECK (cardinality(source_sha256s) = cardinality(source_paths))
    CHECK (array_to_string(source_sha256s, '') ~ '^[0-9a-f]+$')
    CHECK (cardinality(source_sha256s) * 64 = length(array_to_string(source_sha256s, ''))),
  feature_payload jsonb NOT NULL
    CHECK (jsonb_typeof(feature_payload) = 'object'),
  feature_payload_sha256 text NOT NULL
    CHECK (feature_payload_sha256 ~ '^[0-9a-f]{64}$'),
  deterministic_row_sha256 text NOT NULL
    CHECK (deterministic_row_sha256 ~ '^[0-9a-f]{64}$'),
  admitted_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (player_id, game_pk, contract_version),
  CONSTRAINT player_game_feature_state_v1_cutoff_pregame
    CHECK (feature_input_cutoff_utc <= scheduled_start_utc),
  CONSTRAINT player_game_feature_state_v1_sources_strict_prior
    CHECK (latest_source_observed_at_utc <= feature_input_cutoff_utc),
  CONSTRAINT player_game_feature_state_v1_row_hash_unique
    UNIQUE (deterministic_row_sha256)
);

COMMENT ON TABLE mlb.player_game_feature_state_v1 IS
  'Append-only exact-player/game strict-prior feature states; additive V1 foundation.';
COMMENT ON COLUMN mlb.player_game_feature_state_v1.official_game_date IS
  'Accepted playable-appearance attribute; never an identity key.';

CREATE INDEX IF NOT EXISTS idx_player_game_feature_state_v1_game
  ON mlb.player_game_feature_state_v1 (game_pk, player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_feature_state_v1_cutoff
  ON mlb.player_game_feature_state_v1 (player_id, feature_input_cutoff_utc, game_pk);

CREATE OR REPLACE FUNCTION mlb.reject_player_game_feature_state_v1_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  RAISE EXCEPTION 'PLAYER_GAME_FEATURE_STATE_V1_APPEND_ONLY:%', TG_OP
    USING ERRCODE = '55000';
END;
$function$;

DROP TRIGGER IF EXISTS player_game_feature_state_v1_append_only
  ON mlb.player_game_feature_state_v1;
CREATE TRIGGER player_game_feature_state_v1_append_only
BEFORE UPDATE OR DELETE ON mlb.player_game_feature_state_v1
FOR EACH ROW EXECUTE FUNCTION mlb.reject_player_game_feature_state_v1_mutation();
