-- Roll back only the role provisioned by the paired forward migration.
-- Run after rolling back the safe-write entry points and removing any login
-- membership created during a separately authorized deployment.
DO $rollback$
DECLARE
    marker text;
    c record;
BEGIN
    SELECT shobj_description(oid, 'pg_authid')
      INTO marker
      FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_ingest';
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF marker IS DISTINCT FROM 'Created by 20261016_mlb_restricted_ingest_role.sql' THEN
        RAISE EXCEPTION 'refusing to drop a role not created by the paired migration';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members
         WHERE roleid = 'proppadia_mlb_ingest'::regrole
            OR member = 'proppadia_mlb_ingest'::regrole
    ) THEN
        RAISE EXCEPTION 'remove role memberships before rolling back proppadia_mlb_ingest';
    END IF;

    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA mlb FROM proppadia_mlb_ingest';
    EXECUTE 'REVOKE ALL PRIVILEGES ON TABLE mlb.player_stats, mlb.model_training_props FROM proppadia_mlb_ingest';
    FOR c IN
        SELECT n.nspname, r.relname, a.attname
          FROM pg_catalog.pg_class r
          JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
          JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid
         WHERE n.nspname = 'mlb'
           AND r.relname IN ('player_stats', 'model_training_props')
           AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
        EXECUTE format('REVOKE INSERT (%I) ON %I.%I FROM proppadia_mlb_ingest', c.attname, c.nspname, c.relname);
        EXECUTE format('REVOKE UPDATE (%I) ON %I.%I FROM proppadia_mlb_ingest', c.attname, c.nspname, c.relname);
    END LOOP;
    EXECUTE 'DROP ROLE proppadia_mlb_ingest';
END
$rollback$;
