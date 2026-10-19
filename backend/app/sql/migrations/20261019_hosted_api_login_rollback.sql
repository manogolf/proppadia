-- Rollback for 20261019_hosted_api_login.sql. NOT APPLIED.
-- First remove PROPPADIA_API_DATABASE_URL from Render (the API then needs a
-- working legacy alias again; never restore the owner login to do so without
-- the operator's decision). Then run this in the Supabase SQL editor.
BEGIN;

DO $policies$
DECLARE
    v_policy record;
BEGIN
    FOR v_policy IN
        SELECT schemaname, tablename, policyname FROM pg_catalog.pg_policies
         WHERE policyname IN ('proppadia_api_select', 'proppadia_api_insert',
                              'proppadia_api_update', 'proppadia_api_delete')
           AND schemaname IN ('mlb', 'nhl')
    LOOP
        EXECUTE format('DROP POLICY %I ON %I.%I',
                       v_policy.policyname, v_policy.schemaname, v_policy.tablename);
    END LOOP;
END
$policies$;

DO $revoke$
DECLARE
    v_seq text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api') THEN
        FOR v_seq IN
            SELECT pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname)
              FROM pg_catalog.pg_attribute a
             WHERE a.attrelid IN ('mlb.player_props'::regclass, 'nhl.user_props'::regclass)
               AND a.attnum > 0 AND NOT a.attisdropped
               AND pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL
        LOOP
            EXECUTE format('REVOKE USAGE ON SEQUENCE %s FROM proppadia_api', v_seq);
        END LOOP;
        REVOKE UPDATE (status, outcome, updated_at) ON nhl.user_props FROM proppadia_api;
        REVOKE INSERT, DELETE ON nhl.user_props FROM proppadia_api;
        REVOKE INSERT ON mlb.player_props FROM proppadia_api;
        REVOKE SELECT ON
            mlb.player_ids, mlb.model_training_props, mlb.player_props,
            mlb.player_derived_stats, mlb.prop_features_precomputed,
            mlb.public_game_moneyline_predictions, mlb.public_game_moneyline_outcomes,
            mlb.today_workspace_mlb, mlb.today_odds_book_rows, mlb.today_market_snapshot,
            nhl.games, nhl.predictions, nhl.skater_game_logs_raw, nhl.goalie_game_logs_raw,
            nhl.players, nhl.teams, nhl.roster_status,
            nhl.training_features_goalie_saves_v2, nhl.user_props
          FROM proppadia_api;
        REVOKE USAGE ON SCHEMA mlb, nhl FROM proppadia_api;
    END IF;
END
$revoke$;

-- Drop the roles only after confirming no API session is connected:
--   SELECT count(*) FROM pg_stat_activity WHERE usename = 'proppadia_api_login';
DROP ROLE IF EXISTS proppadia_api_login;
DROP ROLE IF EXISTS proppadia_api;

COMMIT;
