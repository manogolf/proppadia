-- Revoke only the read surface introduced by 20261022. Do not alter RLS state
-- or any unrelated policies and privileges.
DO $preflight$
BEGIN
    IF to_regclass('mlb.player_streak_profiles') IS NULL THEN
        RAISE EXCEPTION 'required relation mlb.player_streak_profiles is missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies
         WHERE schemaname = 'mlb'
           AND tablename = 'player_streak_profiles'
           AND policyname = 'proppadia_mlb_ingest_streak_select'
           AND cmd = 'SELECT'
           AND roles = ARRAY['proppadia_mlb_ingest']::name[]
           AND qual = '((prop_source = ''mlb_api''::text) AND (last_game_date IS NOT NULL))'
    ) THEN
        RAISE EXCEPTION 'expected streak read policy is missing or differs; refusing rollback';
    END IF;
END
$preflight$;

DROP POLICY proppadia_mlb_ingest_streak_select ON mlb.player_streak_profiles;
REVOKE SELECT (player_id, prop_type, prop_source, last_game_date, streak_type, streak_count)
    ON mlb.player_streak_profiles FROM proppadia_mlb_ingest;
