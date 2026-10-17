-- Allow the restricted stat-derived writer to read only the streak fields it
-- consumes, and only for StatsAPI-backed profiles. No write ACL is changed.
DO $preflight$
DECLARE
    v_role_safe boolean;
    v_rls boolean;
    v_force_rls boolean;
    v_column_count integer;
BEGIN
    SELECT NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
           AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls
           AND NOT rolinherit
      INTO v_role_safe
      FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_ingest';
    IF coalesce(v_role_safe, false) IS NOT TRUE THEN
        RAISE EXCEPTION 'proppadia_mlb_ingest is missing or has unsafe role attributes';
    END IF;

    IF to_regclass('mlb.player_streak_profiles') IS NULL THEN
        RAISE EXCEPTION 'required relation mlb.player_streak_profiles is missing';
    END IF;

    SELECT relrowsecurity, relforcerowsecurity
      INTO v_rls, v_force_rls
      FROM pg_catalog.pg_class
     WHERE oid = 'mlb.player_streak_profiles'::regclass;
    IF v_rls IS NOT TRUE OR v_force_rls IS TRUE THEN
        RAISE EXCEPTION 'player_streak_profiles must have enabled, non-FORCE RLS';
    END IF;

    SELECT count(*)
      INTO v_column_count
      FROM pg_catalog.pg_attribute
     WHERE attrelid = 'mlb.player_streak_profiles'::regclass
       AND attname = ANY(ARRAY[
           'player_id','prop_type','prop_source','last_game_date','streak_type','streak_count'
       ]::name[])
       AND attnum > 0 AND NOT attisdropped;
    IF v_column_count <> 6 THEN
        RAISE EXCEPTION 'player_streak_profiles is missing a required streak-read column';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies
         WHERE schemaname = 'mlb'
           AND tablename = 'player_streak_profiles'
           AND policyname = 'proppadia_mlb_ingest_streak_select'
           AND NOT (
               cmd = 'SELECT'
               AND roles = ARRAY['proppadia_mlb_ingest']::name[]
               AND qual = '((prop_source = ''mlb_api''::text) AND (last_game_date IS NOT NULL))'
           )
    ) THEN
        RAISE EXCEPTION 'streak read policy exists with an unexpected definition';
    END IF;
END
$preflight$;

GRANT SELECT (player_id, prop_type, prop_source, last_game_date, streak_type, streak_count)
    ON mlb.player_streak_profiles TO proppadia_mlb_ingest;

DO $policy$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies
         WHERE schemaname = 'mlb'
           AND tablename = 'player_streak_profiles'
           AND policyname = 'proppadia_mlb_ingest_streak_select'
    ) THEN
        CREATE POLICY proppadia_mlb_ingest_streak_select
            ON mlb.player_streak_profiles
            FOR SELECT TO proppadia_mlb_ingest
            USING (prop_source = 'mlb_api' AND last_game_date IS NOT NULL);
    END IF;
END
$policy$;
