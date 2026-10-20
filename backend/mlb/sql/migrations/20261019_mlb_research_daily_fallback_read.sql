-- Prepared 2026-10-19; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Adds the columns read by the daily MLB readers that degraded silently once
-- the owner aliases were removed (run 97; inventory:
-- docs/operations/mlb_owner_alias_inventory_2026-10-19.md). Column SELECT only;
-- no write privileges, policies, or role changes.
-- Row visibility: player_ids and player_derived_stats already have the research
-- SELECT policy; model_training_props has the PUBLIC SELECT USING (true)
-- policy; game_info, player_props, and the two public-game moneyline tables
-- have no RLS.
-- Review note: mlb.player_props holds user-added props. The prediction-flow
-- audit reads user_id (an account identifier) to group duplicate user rows;
-- granting it lets the research login read that identifier. Remove user_id
-- from the player_props grant to keep it out; the audit then groups duplicates
-- without it.
-- Whole-row reads: the exact-game shadow reads to_jsonb(player_ids) and
-- prop_workflow reads row_to_json(player_derived_stats), so every column of
-- those two tables must be readable; this grant completes both.
BEGIN;

DO $preflight$
DECLARE
    v_group pg_catalog.pg_roles%ROWTYPE;
    v_login pg_catalog.pg_roles%ROWTYPE;
    v_required record;
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

    FOR v_required IN
        SELECT * FROM (VALUES
            ('mlb.public_game_moneyline_predictions', false), ('mlb.public_game_moneyline_outcomes', false),
            ('mlb.player_ids', true), ('mlb.game_info', false), ('mlb.player_props', false),
            ('mlb.model_training_props', true), ('mlb.player_derived_stats', true)
        ) AS expected(relation_name, rls_enabled)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
             WHERE c.oid = pg_catalog.to_regclass(v_required.relation_name)
               AND c.relrowsecurity = v_required.rls_enabled AND NOT c.relforcerowsecurity
        ) THEN
            RAISE EXCEPTION 'relation is missing or its RLS shape changed: %', v_required.relation_name;
        END IF;
        IF pg_catalog.has_table_privilege(v_group.oid, v_required.relation_name, 'INSERT')
           OR pg_catalog.has_table_privilege(v_group.oid, v_required.relation_name, 'UPDATE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_required.relation_name, 'DELETE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_required.relation_name, 'TRUNCATE')
           OR pg_catalog.has_any_column_privilege(v_group.oid, v_required.relation_name, 'INSERT')
           OR pg_catalog.has_any_column_privilege(v_group.oid, v_required.relation_name, 'UPDATE') THEN
            RAISE EXCEPTION 'research group already has write-capable ACLs on %', v_required.relation_name;
        END IF;
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies p
         WHERE p.schemaname = 'mlb' AND p.tablename = 'model_training_props'
           AND p.policyname = 'Enable read access for all users' AND p.cmd = 'SELECT'
           AND p.roles @> ARRAY['public']::name[] AND p.qual = 'true'
    ) OR (
        SELECT count(*) FROM pg_catalog.pg_policies p
         WHERE p.schemaname = 'mlb' AND p.tablename IN ('player_ids', 'player_derived_stats')
           AND p.policyname = 'proppadia_mlb_research_select' AND p.cmd = 'SELECT'
    ) <> 2 THEN
        RAISE EXCEPTION 'expected research row-visibility policies changed; review before granting';
    END IF;
END
$preflight$;

GRANT SELECT (created_at, prediction_payload)
    ON mlb.public_game_moneyline_predictions TO proppadia_mlb_research_ro;
GRANT SELECT (grading_timestamp_utc)
    ON mlb.public_game_moneyline_outcomes TO proppadia_mlb_research_ro;
GRANT SELECT (id, created_at, updated_at)
    ON mlb.player_ids TO proppadia_mlb_research_ro;
GRANT SELECT (starting_pitcher_id_home, starting_pitcher_id_away)
    ON mlb.game_info TO proppadia_mlb_research_ro;
GRANT SELECT (
    prop_type, game_date, over_under, outcome, predicted_outcome, was_correct,
    confidence_score, prop_source, game_id, player_id, created_at, status,
    prop_value, user_id
) ON mlb.player_props TO proppadia_mlb_research_ro;
GRANT SELECT (
    over_under, outcome, predicted_outcome, was_correct, confidence_score,
    player_name, team
) ON mlb.model_training_props TO proppadia_mlb_research_ro;
GRANT SELECT (id, is_home, created_at, updated_at)
    ON mlb.player_derived_stats TO proppadia_mlb_research_ro;

COMMIT;

-- Verification (run after COMMIT; expect zero rows):
-- SELECT rel, attname FROM (VALUES ('mlb.player_ids'), ('mlb.player_derived_stats')) AS r(rel)
--   JOIN pg_attribute a ON a.attrelid = r.rel::regclass AND a.attnum > 0 AND NOT a.attisdropped
--  WHERE NOT has_column_privilege('proppadia_mlb_research_ro', a.attrelid, a.attname, 'SELECT')
-- UNION ALL
-- SELECT rel, col FROM (VALUES
--   ('mlb.public_game_moneyline_predictions','created_at'), ('mlb.public_game_moneyline_predictions','prediction_payload'),
--   ('mlb.public_game_moneyline_outcomes','grading_timestamp_utc'), ('mlb.game_info','starting_pitcher_id_home'),
--   ('mlb.game_info','starting_pitcher_id_away'), ('mlb.player_props','outcome'), ('mlb.player_props','user_id'),
--   ('mlb.model_training_props','outcome'), ('mlb.model_training_props','player_name')
-- ) AS expected(rel, col)
--  WHERE NOT has_column_privilege('proppadia_mlb_research_ro', rel::regclass, col, 'SELECT');
