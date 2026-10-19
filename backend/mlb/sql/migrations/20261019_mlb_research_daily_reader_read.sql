-- Prepared 2026-10-19; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Adds the columns that ordinary MLB readers select but the research group
-- cannot read yet: four mlb.game_info columns (daily slate output and book
-- upload) and mlb.player_stats.walks_allowed (starter skill/workload
-- LaunchAgent). mlb.game_info has RLS disabled; mlb.player_stats already has
-- the research SELECT policy. No write privileges, policies, or role changes.
BEGIN;

DO $preflight$
DECLARE
    v_group pg_catalog.pg_roles%ROWTYPE;
    v_login pg_catalog.pg_roles%ROWTYPE;
    v_column text;
BEGIN
    SELECT * INTO v_group FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_research_ro';
    IF NOT FOUND OR v_group.rolcanlogin OR v_group.rolsuper OR v_group.rolcreatedb
       OR v_group.rolcreaterole OR v_group.rolreplication OR v_group.rolbypassrls THEN
        RAISE EXCEPTION 'existing MLB research group is missing or has unsafe role attributes';
    END IF;

    SELECT * INTO v_login FROM pg_catalog.pg_roles
     WHERE rolname = 'proppadia_mlb_research_login';
    IF NOT FOUND OR NOT v_login.rolcanlogin OR NOT v_login.rolinherit
       OR v_login.rolsuper OR v_login.rolbypassrls
       OR NOT pg_catalog.pg_has_role(v_login.oid, v_group.oid, 'MEMBER') THEN
        RAISE EXCEPTION 'existing MLB research login is missing, unsafe, or not a member of its read group';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
         WHERE c.oid = 'mlb.game_info'::regclass AND c.relkind IN ('r','p')
           AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
    ) THEN
        RAISE EXCEPTION 'mlb.game_info is missing or its RLS shape changed; review row visibility';
    END IF;

    IF pg_catalog.has_table_privilege(v_group.oid, 'mlb.game_info'::regclass, 'SELECT')
       OR pg_catalog.has_any_column_privilege(v_group.oid, 'mlb.game_info'::regclass, 'INSERT')
       OR pg_catalog.has_any_column_privilege(v_group.oid, 'mlb.game_info'::regclass, 'UPDATE')
       OR pg_catalog.has_table_privilege(v_group.oid, 'mlb.game_info'::regclass, 'DELETE')
       OR pg_catalog.has_table_privilege(v_group.oid, 'mlb.game_info'::regclass, 'TRUNCATE') THEN
        RAISE EXCEPTION 'research group already has table-wide or write-capable ACLs on mlb.game_info';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies p
         WHERE p.schemaname = 'mlb' AND p.tablename = 'player_stats'
           AND p.policyname = 'proppadia_mlb_research_select' AND p.cmd = 'SELECT'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'mlb.player_stats'::regclass AND a.attname = 'walks_allowed'
           AND a.attnum > 0 AND NOT a.attisdropped
    ) THEN
        RAISE EXCEPTION 'mlb.player_stats research policy or walks_allowed column is missing';
    END IF;

    FOREACH v_column IN ARRAY ARRAY['game_id','game_date','home_team_abbr','away_team_abbr'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = 'mlb.game_info'::regclass AND a.attname = v_column
               AND a.attnum > 0 AND NOT a.attisdropped
        ) THEN
            RAISE EXCEPTION 'required research column is missing: mlb.game_info.%', v_column;
        END IF;
    END LOOP;
END
$preflight$;

GRANT SELECT (game_id, game_date, home_team_abbr, away_team_abbr)
    ON mlb.game_info TO proppadia_mlb_research_ro;
GRANT SELECT (walks_allowed)
    ON mlb.player_stats TO proppadia_mlb_research_ro;

COMMIT;

-- Verification (run after COMMIT; expects four rows, all true, then true):
-- SELECT attname, has_column_privilege('proppadia_mlb_research_ro', 'mlb.game_info'::regclass, attname, 'SELECT')
--   FROM pg_attribute WHERE attrelid = 'mlb.game_info'::regclass AND attnum > 0 AND NOT attisdropped
--    AND attname IN ('game_id','game_date','home_team_abbr','away_team_abbr');
-- SELECT has_column_privilege('proppadia_mlb_research_ro', 'mlb.player_stats'::regclass, 'walks_allowed', 'SELECT');
