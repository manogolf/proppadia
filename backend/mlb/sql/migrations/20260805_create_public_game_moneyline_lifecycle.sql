BEGIN;

CREATE TABLE IF NOT EXISTS mlb.public_game_official_finals (
  game_pk bigint PRIMARY KEY, game_date date NOT NULL, scheduled_start_utc timestamptz NOT NULL,
  game_number integer NOT NULL DEFAULT 1, home_team_id bigint NOT NULL, away_team_id bigint NOT NULL,
  home_runs integer NOT NULL CHECK (home_runs >= 0), away_runs integer NOT NULL CHECK (away_runs >= 0),
  official_status text NOT NULL CHECK (official_status IN ('Final','Game Over')),
  official_final_effective_utc timestamptz NOT NULL,
  observed_final_at_utc timestamptz NOT NULL, source_identity text NOT NULL,
  source_sha256 text NOT NULL CHECK (length(source_sha256)=64),
  content_sha256 text NOT NULL CHECK (length(content_sha256)=64), created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mlb.public_game_team_state_snapshots (
  model_version text NOT NULL, prediction_cutoff_utc timestamptz NOT NULL,
  state_through_game_date date NOT NULL, state_hash text NOT NULL CHECK (length(state_hash)=64),
  state_generated_at_utc timestamptz NOT NULL, state_payload jsonb NOT NULL,
  payload_sha256 text NOT NULL CHECK (length(payload_sha256)=64), created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (model_version,prediction_cutoff_utc)
);

CREATE TABLE IF NOT EXISTS mlb.public_game_official_final_corrections (
  correction_id bigserial PRIMARY KEY, game_pk bigint NOT NULL REFERENCES mlb.public_game_official_finals(game_pk),
  corrected_home_runs integer NOT NULL CHECK (corrected_home_runs >= 0),
  corrected_away_runs integer NOT NULL CHECK (corrected_away_runs >= 0),
  correction_reason text NOT NULL, observed_at_utc timestamptz NOT NULL,
  source_identity text NOT NULL, source_sha256 text NOT NULL CHECK (length(source_sha256)=64),
  corrected_content_sha256 text NOT NULL CHECK (length(corrected_content_sha256)=64),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mlb.public_game_moneyline_predictions (
  game_date date NOT NULL, game_id bigint NOT NULL, model_version text NOT NULL,
  prediction_snapshot_class text NOT NULL, scheduled_start_utc timestamptz NOT NULL,
  prediction_timestamp_utc timestamptz NOT NULL, prediction_cutoff_utc timestamptz NOT NULL,
  home_team text NOT NULL, away_team text NOT NULL,
  home_win_probability double precision NOT NULL CHECK (home_win_probability > 0 AND home_win_probability < 1),
  away_win_probability double precision NOT NULL CHECK (away_win_probability > 0 AND away_win_probability < 1),
  predicted_winner text NOT NULL, confidence_band text NOT NULL,
  data_quality_status text NOT NULL, model_hash text NOT NULL CHECK (length(model_hash)=64),
  scorer_hash text NOT NULL CHECK (length(scorer_hash)=64), source_schedule_hash text NOT NULL CHECK (length(source_schedule_hash)=64),
  team_state_hash text NOT NULL CHECK (length(team_state_hash)=64), admission_status text NOT NULL,
  failure_reason text, prediction_payload jsonb NOT NULL, payload_sha256 text NOT NULL CHECK (length(payload_sha256)=64),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_date,game_id,model_version,prediction_snapshot_class),
  CHECK (prediction_timestamp_utc < scheduled_start_utc), CHECK (prediction_cutoff_utc <= prediction_timestamp_utc)
);

CREATE TABLE IF NOT EXISTS mlb.public_game_moneyline_outcomes (
  game_date date NOT NULL, game_id bigint NOT NULL, model_version text NOT NULL,
  prediction_snapshot_class text NOT NULL, official_home_runs integer NOT NULL,
  official_away_runs integer NOT NULL, official_winner text NOT NULL, prediction_correct boolean NOT NULL,
  observed_outcome_probability double precision NOT NULL CHECK (observed_outcome_probability > 0 AND observed_outcome_probability < 1),
  brier_contribution double precision NOT NULL, log_loss_contribution double precision NOT NULL,
  confidence_band text NOT NULL, official_source_identity text NOT NULL,
  official_source_sha256 text NOT NULL CHECK (length(official_source_sha256)=64),
  grading_timestamp_utc timestamptz NOT NULL, outcome_payload jsonb NOT NULL,
  payload_sha256 text NOT NULL CHECK (length(payload_sha256)=64), created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_date,game_id,model_version,prediction_snapshot_class),
  FOREIGN KEY (game_date,game_id,model_version,prediction_snapshot_class)
    REFERENCES mlb.public_game_moneyline_predictions (game_date,game_id,model_version,prediction_snapshot_class)
);

CREATE TABLE IF NOT EXISTS mlb.public_game_moneyline_outcome_corrections (
  correction_id bigserial PRIMARY KEY, game_date date NOT NULL, game_id bigint NOT NULL,
  model_version text NOT NULL, prediction_snapshot_class text NOT NULL,
  prior_payload_sha256 text NOT NULL, corrected_payload jsonb NOT NULL,
  corrected_payload_sha256 text NOT NULL, correction_reason text NOT NULL,
  official_source_identity text NOT NULL, official_source_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION mlb.reject_public_game_lifecycle_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'append-only public game lifecycle'; END $$;
DO $$ DECLARE t text; BEGIN FOREACH t IN ARRAY ARRAY[
  'public_game_official_finals','public_game_team_state_snapshots','public_game_moneyline_predictions',
  'public_game_official_final_corrections','public_game_moneyline_outcomes','public_game_moneyline_outcome_corrections'] LOOP
  EXECUTE format('DROP TRIGGER IF EXISTS reject_mutation ON mlb.%I',t);
  EXECUTE format('CREATE TRIGGER reject_mutation BEFORE UPDATE OR DELETE ON mlb.%I FOR EACH ROW EXECUTE FUNCTION mlb.reject_public_game_lifecycle_mutation()',t);
END LOOP; END $$;

COMMIT;
