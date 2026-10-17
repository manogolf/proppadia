-- Add only the column reads required by the BvP post-commit verifier.
-- No role, membership, connection, writer, RLS, or row policy is changed.
BEGIN;

DO $$
DECLARE
    v_group pg_catalog.pg_roles%ROWTYPE;
    v_login pg_catalog.pg_roles%ROWTYPE;
    v_relation pg_catalog.pg_class%ROWTYPE;
    v_required_columns text[] := ARRAY[
        'computed_at', 'feature_set_tag', 'features', 'game_date',
        'game_id', 'model_tag', 'player_id', 'prop_type'
    ];
    v_granted_columns text[];
    v_login_granted_columns text[];
BEGIN
    SELECT * INTO v_group
      FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_research_ro';
    IF NOT FOUND OR v_group.rolcanlogin OR v_group.rolsuper OR v_group.rolcreatedb
       OR v_group.rolcreaterole OR v_group.rolreplication OR v_group.rolbypassrls THEN
        RAISE EXCEPTION 'existing MLB research group is missing or has unsafe role attributes';
    END IF;

    SELECT * INTO v_login
      FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_research_login';
    IF NOT FOUND OR NOT v_login.rolcanlogin OR NOT v_login.rolinherit OR v_login.rolsuper OR v_login.rolcreatedb
       OR v_login.rolcreaterole OR v_login.rolreplication OR v_login.rolbypassrls
       OR NOT pg_catalog.pg_has_role(v_login.oid, v_group.oid, 'MEMBER') THEN
        RAISE EXCEPTION 'existing MLB research login is missing, unsafe, or not a member of its read group';
    END IF;

    IF NOT pg_catalog.has_schema_privilege(v_group.oid, 'mlb', 'USAGE') THEN
        RAISE EXCEPTION 'existing MLB research group lacks required mlb schema USAGE';
    END IF;

    SELECT c.* INTO v_relation
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'mlb' AND c.relname = 'prop_features_precomputed';
    IF NOT FOUND OR v_relation.relkind NOT IN ('r', 'p')
       OR v_relation.relrowsecurity OR v_relation.relforcerowsecurity THEN
        RAISE EXCEPTION 'BvP feature relation is missing or its reviewed RLS shape changed';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies
         WHERE schemaname = 'mlb' AND tablename = 'prop_features_precomputed'
    ) THEN
        RAISE EXCEPTION 'BvP feature relation has policies; stop and review row access';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM unnest(v_required_columns) AS required(column_name)
         WHERE NOT EXISTS (
             SELECT 1 FROM pg_catalog.pg_attribute a
              WHERE a.attrelid = v_relation.oid AND a.attname = required.column_name
                AND a.attnum > 0 AND NOT a.attisdropped
         )
    ) THEN
        RAISE EXCEPTION 'BvP verifier column contract differs from the deployed relation';
    END IF;

    IF pg_catalog.has_table_privilege(v_group.oid, v_relation.oid, 'SELECT')
       OR pg_catalog.has_table_privilege(v_login.oid, v_relation.oid, 'SELECT') THEN
        RAISE EXCEPTION 'research group already has table-wide SELECT; inspect before proceeding';
    END IF;

    SELECT COALESCE(array_agg(a.attname::text ORDER BY a.attname::text), ARRAY[]::text[])
      INTO v_granted_columns
      FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = v_relation.oid AND a.attnum > 0 AND NOT a.attisdropped
       AND pg_catalog.has_column_privilege(v_group.oid, v_relation.oid, a.attname, 'SELECT');

    IF v_granted_columns <> ARRAY[]::text[] THEN
        RAISE EXCEPTION 'research group already has column SELECT; inspect before proceeding';
    END IF;

    SELECT COALESCE(array_agg(a.attname::text ORDER BY a.attname::text), ARRAY[]::text[])
      INTO v_login_granted_columns
      FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = v_relation.oid AND a.attnum > 0 AND NOT a.attisdropped
       AND pg_catalog.has_column_privilege(v_login.oid, v_relation.oid, a.attname, 'SELECT');

    IF v_login_granted_columns <> ARRAY[]::text[] THEN
        RAISE EXCEPTION 'research login already has column SELECT; inspect before proceeding';
    END IF;
END
$$;

GRANT SELECT (
    prop_type, player_id, game_id, game_date, features,
    feature_set_tag, model_tag, computed_at
) ON mlb.prop_features_precomputed TO proppadia_mlb_research_ro;

COMMIT;
