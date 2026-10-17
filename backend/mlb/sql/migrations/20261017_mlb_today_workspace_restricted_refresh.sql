-- Owner-controlled capability for the four existing MLB today materialized views.
-- Apply as the owner of all four views (currently postgres) after separate review.
BEGIN;

DO $preflight$
DECLARE
    relation_name text;
    relation_oid oid;
BEGIN
    IF current_setting('server_version_num')::integer < 150000 THEN
        RAISE EXCEPTION 'PostgreSQL 15 or later is required';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
        WHERE rolname = 'proppadia_mlb_ingest'
    ) THEN
        RAISE EXCEPTION 'required role proppadia_mlb_ingest is missing';
    END IF;

    FOREACH relation_name IN ARRAY ARRAY[
        'today_market_snapshot',
        'today_market_timing_signal',
        'today_player_context',
        'today_workspace_mlb'
    ] LOOP
        relation_oid := pg_catalog.to_regclass('mlb.' || relation_name);
        IF relation_oid IS NULL OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            WHERE c.oid = relation_oid
              AND c.relkind = 'm'
              AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
        ) THEN
            RAISE EXCEPTION 'mlb.% is missing, not materialized, or not owned by current_user', relation_name;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_index i
            WHERE i.indrelid = relation_oid
              AND i.indisunique
              AND i.indisvalid
              AND i.indisready
              AND i.indpred IS NULL
        ) THEN
            RAISE EXCEPTION 'mlb.% lacks a valid non-partial unique index for concurrent refresh', relation_name;
        END IF;
    END LOOP;
END
$preflight$;

CREATE FUNCTION mlb.refresh_today_workspace_views()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $refresh$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.today_market_snapshot;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.today_market_timing_signal;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.today_player_context;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mlb.today_workspace_mlb;
END
$refresh$;

REVOKE ALL ON FUNCTION mlb.refresh_today_workspace_views() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.refresh_today_workspace_views() TO proppadia_mlb_ingest;

COMMIT;
