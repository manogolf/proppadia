-- Prepared 2026-10-19; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Restricted writer route for the MLB moneyline shadow lifecycle
-- (run_mlb_public_game_moneyline_daily_v1 via durable_store_v1). The daily hook
-- runs it with MLB_USE_RESTRICTED_INGEST=1, so it connects as the ingest login
-- and SET ROLE proppadia_mlb_ingest.
-- The lifecycle only appends: INSERT ... ON CONFLICT DO NOTHING on four
-- immutable ledgers, then reads back for conflict checks and grading. This file
-- therefore grants column SELECT and column INSERT only. No UPDATE, DELETE,
-- TRUNCATE, sequence, policy, or role change. The tables' existing
-- reject_mutation triggers (BEFORE UPDATE OR DELETE) stay in place, and the
-- correction ledger stays read-only for ingest.
BEGIN;

DO $preflight$
DECLARE
    v_group pg_catalog.pg_roles%ROWTYPE;
    v_relation text;
BEGIN
    SELECT * INTO v_group FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_mlb_ingest';
    IF NOT FOUND OR v_group.rolcanlogin OR v_group.rolsuper OR v_group.rolbypassrls
       OR v_group.rolcreaterole OR v_group.rolcreatedb THEN
        RAISE EXCEPTION 'MLB ingest group is missing or has unsafe role attributes';
    END IF;

    FOREACH v_relation IN ARRAY ARRAY[
        'mlb.public_game_official_finals', 'mlb.public_game_official_final_corrections',
        'mlb.public_game_team_state_snapshots', 'mlb.public_game_moneyline_predictions',
        'mlb.public_game_moneyline_outcomes'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
             WHERE c.oid = pg_catalog.to_regclass(v_relation) AND c.relkind = 'r'
               AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
        ) THEN
            RAISE EXCEPTION '% is missing, not a plain table, or has RLS; review row visibility first', v_relation;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger t
             WHERE t.tgrelid = pg_catalog.to_regclass(v_relation) AND t.tgname = 'reject_mutation'
               AND NOT t.tgisinternal AND t.tgenabled <> 'D'
               AND (t.tgtype & 8) <> 0 AND (t.tgtype & 16) <> 0   -- DELETE and UPDATE events
        ) THEN
            RAISE EXCEPTION '% lacks its enabled reject_mutation trigger; the ledger must stay append-only', v_relation;
        END IF;
        IF pg_catalog.has_table_privilege(v_group.oid, v_relation, 'UPDATE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation, 'DELETE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation, 'TRUNCATE')
           OR pg_catalog.has_any_column_privilege(v_group.oid, v_relation, 'UPDATE') THEN
            RAISE EXCEPTION 'ingest group already has mutating privileges on %', v_relation;
        END IF;
    END LOOP;

    -- Insert targets must not need a sequence or identity value.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
          LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
         WHERE a.attrelid IN ('mlb.public_game_official_finals'::regclass,
                              'mlb.public_game_team_state_snapshots'::regclass,
                              'mlb.public_game_moneyline_predictions'::regclass,
                              'mlb.public_game_moneyline_outcomes'::regclass)
           AND a.attnum > 0 AND NOT a.attisdropped
           AND (a.attidentity <> '' OR pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%')
    ) THEN
        RAISE EXCEPTION 'an insert target now uses a sequence or identity column; review before granting';
    END IF;
END
$preflight$;

-- Official finals: append, conflict read-back, and the cutoff-gated state read.
GRANT SELECT (
    game_pk, game_date, scheduled_start_utc, game_number, home_team_id, away_team_id,
    home_runs, away_runs, official_status, official_final_effective_utc,
    observed_final_at_utc, source_identity, source_sha256
) ON mlb.public_game_official_finals TO proppadia_mlb_ingest;
GRANT INSERT (
    game_pk, game_date, scheduled_start_utc, game_number, home_team_id, away_team_id,
    home_runs, away_runs, official_status, official_final_effective_utc,
    observed_final_at_utc, source_identity, source_sha256, content_sha256
) ON mlb.public_game_official_finals TO proppadia_mlb_ingest;

-- Corrections: read only (the lifecycle's LATERAL join reads every column).
GRANT SELECT (
    correction_id, game_pk, corrected_home_runs, corrected_away_runs, correction_reason,
    observed_at_utc, source_identity, source_sha256, corrected_content_sha256, created_at
) ON mlb.public_game_official_final_corrections TO proppadia_mlb_ingest;

-- Team-state snapshots.
GRANT SELECT (model_version, prediction_cutoff_utc, state_hash)
    ON mlb.public_game_team_state_snapshots TO proppadia_mlb_ingest;
GRANT INSERT (
    model_version, prediction_cutoff_utc, state_through_game_date, state_hash,
    state_generated_at_utc, state_payload, payload_sha256
) ON mlb.public_game_team_state_snapshots TO proppadia_mlb_ingest;

-- Moneyline predictions.
GRANT SELECT (
    game_date, game_id, model_version, prediction_snapshot_class, scheduled_start_utc,
    admission_status, prediction_payload, payload_sha256
) ON mlb.public_game_moneyline_predictions TO proppadia_mlb_ingest;
GRANT INSERT (
    game_date, game_id, model_version, prediction_snapshot_class, scheduled_start_utc,
    prediction_timestamp_utc, prediction_cutoff_utc, home_team, away_team, home_win_probability,
    away_win_probability, predicted_winner, confidence_band, data_quality_status, model_hash,
    scorer_hash, source_schedule_hash, team_state_hash, admission_status, failure_reason,
    prediction_payload, payload_sha256
) ON mlb.public_game_moneyline_predictions TO proppadia_mlb_ingest;

-- Moneyline outcomes.
GRANT SELECT (game_date, game_id, model_version, prediction_snapshot_class, payload_sha256)
    ON mlb.public_game_moneyline_outcomes TO proppadia_mlb_ingest;
GRANT INSERT (
    game_date, game_id, model_version, prediction_snapshot_class, official_home_runs,
    official_away_runs, official_winner, prediction_correct, observed_outcome_probability,
    brier_contribution, log_loss_contribution, confidence_band, official_source_identity,
    official_source_sha256, grading_timestamp_utc, outcome_payload, payload_sha256
) ON mlb.public_game_moneyline_outcomes TO proppadia_mlb_ingest;

COMMIT;

-- Verification for the Supabase SQL editor (runs as postgres; uses has_*_privilege only).
-- Expect one row: every check true.
-- SELECT
--   bool_and(has_column_privilege('proppadia_mlb_ingest', 'mlb.public_game_official_finals', c, 'INSERT'))
--     FILTER (WHERE t = 'finals_insert') AS finals_insert,
--   bool_and(has_column_privilege('proppadia_mlb_ingest', 'mlb.public_game_team_state_snapshots', c, 'INSERT'))
--     FILTER (WHERE t = 'state_insert') AS state_insert,
--   bool_and(has_column_privilege('proppadia_mlb_ingest', 'mlb.public_game_moneyline_predictions', c, 'INSERT'))
--     FILTER (WHERE t = 'prediction_insert') AS prediction_insert,
--   bool_and(has_column_privilege('proppadia_mlb_ingest', 'mlb.public_game_moneyline_outcomes', c, 'INSERT'))
--     FILTER (WHERE t = 'outcome_insert') AS outcome_insert,
--   bool_and(has_column_privilege('proppadia_mlb_ingest', 'mlb.public_game_official_final_corrections', c, 'SELECT'))
--     FILTER (WHERE t = 'corrections_select') AS corrections_select
-- FROM (VALUES
--   ('finals_insert','game_pk'), ('finals_insert','content_sha256'),
--   ('state_insert','state_payload'), ('state_insert','payload_sha256'),
--   ('prediction_insert','prediction_payload'), ('prediction_insert','failure_reason'),
--   ('outcome_insert','outcome_payload'), ('outcome_insert','grading_timestamp_utc'),
--   ('corrections_select','correction_id'), ('corrections_select','corrected_home_runs')
-- ) AS checks(t, c);
--
-- Expect zero rows (no mutating privilege, and the correction ledger stays read-only):
-- SELECT rel, priv FROM (VALUES
--   ('mlb.public_game_official_finals'), ('mlb.public_game_official_final_corrections'),
--   ('mlb.public_game_team_state_snapshots'), ('mlb.public_game_moneyline_predictions'),
--   ('mlb.public_game_moneyline_outcomes')) AS r(rel)
-- CROSS JOIN (VALUES ('UPDATE'), ('DELETE'), ('TRUNCATE')) AS p(priv)
-- WHERE has_table_privilege('proppadia_mlb_ingest', rel, priv)
-- UNION ALL
-- SELECT 'mlb.public_game_official_final_corrections', 'INSERT'
-- WHERE has_any_column_privilege('proppadia_mlb_ingest', 'mlb.public_game_official_final_corrections', 'INSERT');
