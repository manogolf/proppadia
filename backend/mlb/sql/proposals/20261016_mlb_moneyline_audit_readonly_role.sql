-- PROPOSAL ONLY. Do not execute without separate operational authorization.
-- Creates the shared NOLOGIN read role for the single moneyline audit caller.
-- Live catalog review on 2026-10-16 verified that the database grants CONNECT
-- to PUBLIC, mlb does not grant USAGE to PUBLIC, and both target tables have
-- RLS disabled, no policies, owner postgres, and no explicit ACL.

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'proppadia_mlb_research_ro'
    ) THEN
        RAISE EXCEPTION 'proppadia_mlb_research_ro already exists; inspect before proceeding';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_database WHERE datname = current_database()
    ) THEN
        RAISE EXCEPTION 'current database could not be verified';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'mlb' AND c.relname = 'public_game_moneyline_predictions'
          AND c.relkind IN ('r', 'p') AND NOT c.relrowsecurity
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'mlb' AND c.relname = 'public_game_moneyline_outcomes'
          AND c.relkind IN ('r', 'p') AND NOT c.relrowsecurity
    ) THEN
        RAISE EXCEPTION 'target relation shape or RLS state differs from reviewed catalog';
    END IF;
END
$$;

CREATE ROLE proppadia_mlb_research_ro
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

GRANT USAGE ON SCHEMA mlb TO proppadia_mlb_research_ro;
GRANT SELECT (
    game_date, game_id, model_version, prediction_snapshot_class,
    scheduled_start_utc, prediction_timestamp_utc, prediction_cutoff_utc,
    home_team, away_team, home_win_probability, away_win_probability,
    predicted_winner, confidence_band, model_hash, payload_sha256,
    admission_status
) ON mlb.public_game_moneyline_predictions TO proppadia_mlb_research_ro;

GRANT SELECT (
    game_date, game_id, model_version, prediction_snapshot_class,
    official_home_runs, official_away_runs, official_winner,
    prediction_correct, payload_sha256
) ON mlb.public_game_moneyline_outcomes TO proppadia_mlb_research_ro;

COMMIT;
