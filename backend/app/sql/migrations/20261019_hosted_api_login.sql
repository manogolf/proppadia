-- Prepared 2026-10-19; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Hosted API (Render, backend/app/api_server.py) login that replaces the
-- production owner login. Inventory: docs/operations/hosted_api_database_access_inventory_2026-10-19.md.
--
-- Creates the NOLOGIN group proppadia_api and the login proppadia_api_login
-- (INHERIT, member of that group only, NO PASSWORD here). Grants:
--   * SELECT on the 19 relations the API reads;
--   * INSERT on mlb.player_props (POST /api/props/add);
--   * INSERT, DELETE, and UPDATE (status, outcome, updated_at) on
--     nhl.user_props (NHL prop add/delete, grading on GET props/history,
--     POST /api/ops/nhl/resolve-props).
-- No source-table DML, DDL, schema CREATE, function EXECUTE, or membership
-- in postgres, service_role, supabase_admin, or any other proppadia group.
-- Where a target table has RLS enabled, a policy for proppadia_api is added
-- that matches exactly the verbs granted above.
--
-- Set the password afterwards, outside this file (see the inventory doc).
BEGIN;

DO $preflight$
DECLARE
    v_rel text;
    v_role pg_catalog.pg_roles%ROWTYPE;
BEGIN
    FOREACH v_rel IN ARRAY ARRAY[
        'mlb.player_ids', 'mlb.model_training_props', 'mlb.player_props',
        'mlb.player_derived_stats', 'mlb.prop_features_precomputed',
        'mlb.public_game_moneyline_predictions', 'mlb.public_game_moneyline_outcomes',
        'mlb.today_workspace_mlb', 'mlb.today_odds_book_rows', 'mlb.today_market_snapshot',
        'nhl.games', 'nhl.predictions', 'nhl.skater_game_logs_raw',
        'nhl.goalie_game_logs_raw', 'nhl.players', 'nhl.teams', 'nhl.roster_status',
        'nhl.training_features_goalie_saves_v2', 'nhl.user_props'
    ] LOOP
        IF pg_catalog.to_regclass(v_rel) IS NULL THEN
            RAISE EXCEPTION 'hosted API relation is missing: %', v_rel;
        END IF;
    END LOOP;

    FOREACH v_rel IN ARRAY ARRAY['status', 'outcome', 'updated_at'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = 'nhl.user_props'::regclass AND a.attname = v_rel
               AND a.attnum > 0 AND NOT a.attisdropped
        ) THEN
            RAISE EXCEPTION 'nhl.user_props column is missing: %', v_rel;
        END IF;
    END LOOP;

    SELECT * INTO v_role FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api';
    IF FOUND AND (v_role.rolcanlogin OR v_role.rolsuper OR v_role.rolcreatedb
       OR v_role.rolcreaterole OR v_role.rolreplication OR v_role.rolbypassrls) THEN
        RAISE EXCEPTION 'existing proppadia_api group has unsafe role attributes';
    END IF;
    SELECT * INTO v_role FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api_login';
    IF FOUND AND (NOT v_role.rolcanlogin OR NOT v_role.rolinherit OR v_role.rolsuper
       OR v_role.rolcreatedb OR v_role.rolcreaterole OR v_role.rolreplication
       OR v_role.rolbypassrls) THEN
        RAISE EXCEPTION 'existing proppadia_api_login has unsafe role attributes';
    END IF;
END
$preflight$;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api') THEN
        CREATE ROLE proppadia_api NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api_login') THEN
        CREATE ROLE proppadia_api_login LOGIN INHERIT NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
END
$roles$;

GRANT proppadia_api TO proppadia_api_login;

GRANT USAGE ON SCHEMA mlb, nhl TO proppadia_api;
REVOKE CREATE ON SCHEMA mlb, nhl FROM proppadia_api;

GRANT SELECT ON
    mlb.player_ids, mlb.model_training_props, mlb.player_props,
    mlb.player_derived_stats, mlb.prop_features_precomputed,
    mlb.public_game_moneyline_predictions, mlb.public_game_moneyline_outcomes,
    mlb.today_workspace_mlb, mlb.today_odds_book_rows, mlb.today_market_snapshot,
    nhl.games, nhl.predictions, nhl.skater_game_logs_raw, nhl.goalie_game_logs_raw,
    nhl.players, nhl.teams, nhl.roster_status,
    nhl.training_features_goalie_saves_v2, nhl.user_props
  TO proppadia_api;

GRANT INSERT ON mlb.player_props TO proppadia_api;
GRANT INSERT, DELETE ON nhl.user_props TO proppadia_api;
GRANT UPDATE (status, outcome, updated_at) ON nhl.user_props TO proppadia_api;

-- Sequence-backed defaults on the two insert targets (nhl.user_props uses
-- gen_random_uuid(); mlb.player_props is checked here rather than assumed).
DO $sequences$
DECLARE
    v_seq text;
BEGIN
    FOR v_seq IN
        SELECT pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname)
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid IN ('mlb.player_props'::regclass, 'nhl.user_props'::regclass)
           AND a.attnum > 0 AND NOT a.attisdropped
           AND pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL
    LOOP
        EXECUTE format('GRANT USAGE ON SEQUENCE %s TO proppadia_api', v_seq);
    END LOOP;
END
$sequences$;

-- RLS: only where enabled, and only for the verbs granted above.
DO $policies$
DECLARE
    v_rel record;
    v_cmd text;
    v_name text;
    v_writes jsonb := jsonb_build_object(
        'mlb.player_props', jsonb_build_array('INSERT'),
        'nhl.user_props', jsonb_build_array('INSERT', 'UPDATE', 'DELETE'));
BEGIN
    FOR v_rel IN
        SELECT n.nspname, c.relname, n.nspname || '.' || c.relname AS qualified
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.oid = ANY (ARRAY[
            'mlb.player_ids', 'mlb.model_training_props', 'mlb.player_props',
            'mlb.player_derived_stats', 'mlb.prop_features_precomputed',
            'mlb.public_game_moneyline_predictions', 'mlb.public_game_moneyline_outcomes',
            'mlb.today_workspace_mlb', 'mlb.today_odds_book_rows', 'mlb.today_market_snapshot',
            'nhl.games', 'nhl.predictions', 'nhl.skater_game_logs_raw',
            'nhl.goalie_game_logs_raw', 'nhl.players', 'nhl.teams', 'nhl.roster_status',
            'nhl.training_features_goalie_saves_v2', 'nhl.user_props'
         ]::regclass[]) AND c.relrowsecurity
    LOOP
        FOR v_cmd IN
            SELECT 'SELECT' UNION ALL
            SELECT jsonb_array_elements_text(coalesce(v_writes -> v_rel.qualified, '[]'::jsonb))
        LOOP
            v_name := 'proppadia_api_' || lower(v_cmd);
            CONTINUE WHEN EXISTS (
                SELECT 1 FROM pg_catalog.pg_policies
                 WHERE schemaname = v_rel.nspname AND tablename = v_rel.relname
                   AND policyname = v_name);
            EXECUTE format(
                'CREATE POLICY %I ON %I.%I FOR %s TO proppadia_api %s',
                v_name, v_rel.nspname, v_rel.relname, v_cmd,
                CASE v_cmd WHEN 'INSERT' THEN 'WITH CHECK (true)'
                           WHEN 'UPDATE' THEN 'USING (true) WITH CHECK (true)'
                           ELSE 'USING (true)' END);
        END LOOP;
    END LOOP;
END
$policies$;

DO $postcheck$
DECLARE
    v_login oid := (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api_login');
    v_group oid := (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'proppadia_api');
BEGIN
    IF (SELECT count(*) FROM pg_catalog.pg_auth_members WHERE member = v_login) <> 1
       OR NOT pg_catalog.pg_has_role(v_login, v_group, 'MEMBER') THEN
        RAISE EXCEPTION 'proppadia_api_login must be a member of proppadia_api only';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member = v_group) THEN
        RAISE EXCEPTION 'proppadia_api must not inherit another role';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles r
         WHERE r.rolname IN ('postgres', 'service_role', 'supabase_admin')
           AND pg_catalog.pg_has_role(v_login, r.oid, 'MEMBER')
    ) THEN
        RAISE EXCEPTION 'proppadia_api_login reaches an owner or service role';
    END IF;
    IF pg_catalog.has_any_column_privilege(v_group, 'nhl.skater_game_logs_raw', 'INSERT')
       OR pg_catalog.has_any_column_privilege(v_group, 'nhl.skater_game_logs_raw', 'UPDATE')
       OR pg_catalog.has_any_column_privilege(v_group, 'mlb.player_ids', 'UPDATE')
       OR pg_catalog.has_any_column_privilege(v_group, 'mlb.model_training_props', 'UPDATE')
       OR pg_catalog.has_table_privilege(v_group, 'mlb.player_props', 'UPDATE')
       OR pg_catalog.has_table_privilege(v_group, 'mlb.player_props', 'DELETE')
       OR pg_catalog.has_column_privilege(v_group, 'nhl.user_props', 'prop_value', 'UPDATE')
       OR pg_catalog.has_schema_privilege(v_group, 'mlb', 'CREATE')
       OR pg_catalog.has_schema_privilege(v_group, 'nhl', 'CREATE') THEN
        RAISE EXCEPTION 'proppadia_api has a write or CREATE privilege outside its contract';
    END IF;
END
$postcheck$;

COMMIT;

-- Verification (run after COMMIT in the SQL editor). The editor's postgres
-- login cannot SET ROLE to proppadia_api, so check privileges directly.
-- Expect: t, t, t, f, t, t, f, f, t, f.
-- SELECT has_table_privilege('proppadia_api', 'mlb.player_ids', 'SELECT'),
--        has_table_privilege('proppadia_api', 'nhl.user_props', 'SELECT'),
--        has_table_privilege('proppadia_api', 'mlb.player_props', 'INSERT'),
--        has_table_privilege('proppadia_api', 'mlb.player_ids', 'UPDATE'),
--        has_table_privilege('proppadia_api', 'nhl.user_props', 'DELETE'),
--        has_column_privilege('proppadia_api', 'nhl.user_props', 'status', 'UPDATE'),
--        has_column_privilege('proppadia_api', 'nhl.user_props', 'prop_value', 'UPDATE'),
--        has_schema_privilege('proppadia_api', 'mlb', 'CREATE'),
--        pg_has_role('proppadia_api_login', 'proppadia_api', 'MEMBER'),
--        pg_has_role('proppadia_api_login', 'postgres', 'MEMBER');
-- SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolbypassrls
--   FROM pg_roles WHERE rolname IN ('proppadia_api', 'proppadia_api_login');
