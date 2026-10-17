-- Row-level policies required by the already column/table-limited MLB writer
-- grants. Existing production tables have RLS enabled; without policies, the
-- grants and fixed entry points alone do not make the active invoker paths
-- usable. These policies add no ACL privileges and apply only to the ingest
-- NOLOGIN permission group.
DO $preflight$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
         WHERE rolname = 'proppadia_mlb_ingest'
           AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
           AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls
           AND NOT rolinherit
    ) THEN
        RAISE EXCEPTION 'proppadia_mlb_ingest is missing or has unsafe role attributes';
    END IF;
    IF to_regclass('mlb.player_stats') IS NULL
       OR to_regclass('mlb.model_training_props') IS NULL
       OR to_regclass('mlb.player_derived_stats') IS NULL THEN
        RAISE EXCEPTION 'required MLB writer relations are missing';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid IN ('mlb.player_stats'::regclass,
                        'mlb.model_training_props'::regclass,
                        'mlb.player_derived_stats'::regclass)
          AND (c.relforcerowsecurity OR NOT c.relrowsecurity)
    ) THEN
        RAISE EXCEPTION 'expected RLS-enabled, non-FORCE MLB writer relations are missing';
    END IF;
END
$preflight$;

DROP POLICY IF EXISTS proppadia_mlb_ingest_select ON mlb.player_stats;
CREATE POLICY proppadia_mlb_ingest_select ON mlb.player_stats
    FOR SELECT TO proppadia_mlb_ingest USING (true);

DROP POLICY IF EXISTS proppadia_mlb_ingest_select ON mlb.model_training_props;
DROP POLICY IF EXISTS proppadia_mlb_ingest_update ON mlb.model_training_props;
CREATE POLICY proppadia_mlb_ingest_select ON mlb.model_training_props
    FOR SELECT TO proppadia_mlb_ingest USING (true);
CREATE POLICY proppadia_mlb_ingest_update ON mlb.model_training_props
    FOR UPDATE TO proppadia_mlb_ingest USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS proppadia_mlb_ingest_select ON mlb.player_derived_stats;
DROP POLICY IF EXISTS proppadia_mlb_ingest_insert ON mlb.player_derived_stats;
DROP POLICY IF EXISTS proppadia_mlb_ingest_update ON mlb.player_derived_stats;
CREATE POLICY proppadia_mlb_ingest_select ON mlb.player_derived_stats
    FOR SELECT TO proppadia_mlb_ingest USING (true);
CREATE POLICY proppadia_mlb_ingest_insert ON mlb.player_derived_stats
    FOR INSERT TO proppadia_mlb_ingest WITH CHECK (true);
CREATE POLICY proppadia_mlb_ingest_update ON mlb.player_derived_stats
    FOR UPDATE TO proppadia_mlb_ingest USING (true) WITH CHECK (true);
