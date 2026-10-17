-- Provision only the routine MLB writer permission group.  Its safe-write
-- grants are applied by the following reviewed entry-point migrations.
-- Login creation and credentials remain a separate, authorized deployment step.
DO $role$
DECLARE
    existing pg_catalog.pg_roles%ROWTYPE;
BEGIN
    SELECT * INTO existing
      FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_ingest';

    IF NOT FOUND THEN
        EXECUTE 'CREATE ROLE proppadia_mlb_ingest NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS';
        EXECUTE $comment$COMMENT ON ROLE proppadia_mlb_ingest IS
            'Created by 20261016_mlb_restricted_ingest_role.sql'$comment$;
    ELSIF existing.rolcanlogin OR existing.rolsuper OR existing.rolcreatedb
       OR existing.rolcreaterole OR existing.rolreplication OR existing.rolbypassrls
       OR existing.rolinherit THEN
        RAISE EXCEPTION 'proppadia_mlb_ingest exists with unsafe role attributes';
    END IF;
END
$role$;
