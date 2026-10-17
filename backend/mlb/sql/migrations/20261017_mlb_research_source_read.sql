-- Add the inventoried read scope for the existing MLB research group.
-- This migration preserves NOBYPASSRLS and grants no write privileges.
BEGIN;

DO $preflight$
DECLARE
    v_group pg_catalog.pg_roles%ROWTYPE;
    v_login pg_catalog.pg_roles%ROWTYPE;
    v_relation record;
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
       OR v_login.rolsuper OR v_login.rolcreatedb OR v_login.rolcreaterole
       OR v_login.rolreplication OR v_login.rolbypassrls
       OR NOT pg_catalog.pg_has_role(v_login.oid, v_group.oid, 'MEMBER') THEN
        RAISE EXCEPTION 'existing MLB research login is missing, unsafe, or not a member of its read group';
    END IF;

    IF NOT pg_catalog.has_schema_privilege(v_group.oid, 'mlb', 'USAGE')
       OR NOT pg_catalog.has_schema_privilege(v_group.oid, 'public', 'USAGE') THEN
        RAISE EXCEPTION 'existing MLB research group lacks required schema USAGE';
    END IF;

    FOR v_relation IN
        SELECT * FROM (VALUES
            ('mlb.player_stats'::regclass, true, false),
            ('mlb.player_derived_stats'::regclass, true, false),
            ('mlb.player_ids'::regclass, true, false),
            ('mlb.player_team_by_game'::regclass, false, false),
            ('mlb.model_training_props'::regclass, true, false),
            ('public.mlb_team_map'::regclass, true, false)
        ) AS expected(relation_oid, rls_enabled, force_rls)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
             WHERE c.oid = v_relation.relation_oid AND c.relkind IN ('r','p')
               AND c.relrowsecurity = v_relation.rls_enabled
               AND c.relforcerowsecurity = v_relation.force_rls
        ) THEN
            RAISE EXCEPTION 'research read relation is missing or its RLS shape changed: %', v_relation.relation_oid::regclass;
        END IF;

        IF pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'SELECT')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'SELECT') THEN
            RAISE EXCEPTION 'research role already has table-wide SELECT on %', v_relation.relation_oid::regclass;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = v_relation.relation_oid AND a.attnum > 0 AND NOT a.attisdropped
               AND (pg_catalog.has_column_privilege(v_group.oid, v_relation.relation_oid, a.attname, 'SELECT')
                    OR pg_catalog.has_column_privilege(v_login.oid, v_relation.relation_oid, a.attname, 'SELECT'))
        ) THEN
            RAISE EXCEPTION 'research role already has column SELECT on %', v_relation.relation_oid::regclass;
        END IF;
        IF pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'INSERT')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'UPDATE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'DELETE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'TRUNCATE')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'REFERENCES')
           OR pg_catalog.has_table_privilege(v_group.oid, v_relation.relation_oid, 'TRIGGER')
           OR pg_catalog.has_any_column_privilege(v_group.oid, v_relation.relation_oid, 'INSERT')
           OR pg_catalog.has_any_column_privilege(v_group.oid, v_relation.relation_oid, 'UPDATE')
           OR pg_catalog.has_any_column_privilege(v_group.oid, v_relation.relation_oid, 'REFERENCES')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'INSERT')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'UPDATE')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'DELETE')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'TRUNCATE')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'REFERENCES')
           OR pg_catalog.has_table_privilege(v_login.oid, v_relation.relation_oid, 'TRIGGER')
           OR pg_catalog.has_any_column_privilege(v_login.oid, v_relation.relation_oid, 'INSERT')
           OR pg_catalog.has_any_column_privilege(v_login.oid, v_relation.relation_oid, 'UPDATE')
           OR pg_catalog.has_any_column_privilege(v_login.oid, v_relation.relation_oid, 'REFERENCES') THEN
            RAISE EXCEPTION 'research group already has write-capable ACLs on %', v_relation.relation_oid::regclass;
        END IF;
    END LOOP;

    FOR v_required IN
        SELECT * FROM (VALUES
            ('mlb.player_stats','player_id'),('mlb.player_stats','game_id'),('mlb.player_stats','game_date'),
            ('mlb.player_stats','team'),('mlb.player_stats','opponent'),('mlb.player_stats','position'),
            ('mlb.player_stats','hits'),('mlb.player_stats','hits_allowed'),('mlb.player_stats','outs_recorded'),
            ('mlb.player_stats','is_starter'),('mlb.player_stats','earned_runs'),('mlb.player_stats','strikeouts_pitching'),
            ('mlb.player_stats','at_bats'),('mlb.player_stats','plate_appearances'),
            ('mlb.player_derived_stats','player_id'),('mlb.player_derived_stats','game_id'),
            ('mlb.player_derived_stats','game_date'),('mlb.player_derived_stats','team'),
            ('mlb.player_ids','player_id'),('mlb.player_ids','player_name'),('mlb.player_ids','team'),('mlb.player_ids','team_id'),
            ('mlb.player_team_by_game','player_id'),('mlb.player_team_by_game','game_id'),('mlb.player_team_by_game','team_id'),
            ('mlb.model_training_props','game_date'),('mlb.model_training_props','game_id'),
            ('mlb.model_training_props','player_id'),('mlb.model_training_props','prop_type'),
            ('mlb.model_training_props','rolling_result_avg_7'),
            ('public.mlb_team_map','team_id'),('public.mlb_team_map','abbr')
        ) AS required(relation_name, column_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = pg_catalog.to_regclass(v_required.relation_name)
               AND a.attname = v_required.column_name AND a.attnum > 0 AND NOT a.attisdropped
        ) THEN
            RAISE EXCEPTION 'required research column is missing: %.%', v_required.relation_name, v_required.column_name;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
          FROM (VALUES ('d7'),('d15'),('d30')) AS windows(prefix)
          CROSS JOIN unnest(ARRAY[
              'hits','runs_scored','rbis','home_runs','singles','doubles','triples',
              'walks','strikeouts_batting','stolen_bases','total_bases','hits_runs_rbis',
              'runs_rbis','outs_recorded','strikeouts_pitching','walks_allowed',
              'earned_runs','hits_allowed','at_bats','plate_appearances'
          ]::text[]) AS metrics(metric)
         WHERE NOT EXISTS (
             SELECT 1 FROM pg_catalog.pg_attribute a
              WHERE a.attrelid = 'mlb.player_derived_stats'::regclass
                AND a.attname::text = windows.prefix || '_' || metrics.metric
                AND a.attnum > 0 AND NOT a.attisdropped
         )
    ) THEN
        RAISE EXCEPTION 'player_derived_stats inventoried rolling column contract changed';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies p
         WHERE p.schemaname='mlb' AND p.tablename='model_training_props'
           AND p.policyname='Enable read access for all users' AND p.cmd='SELECT'
           AND p.roles @> ARRAY['public']::name[] AND p.qual='true'
    ) THEN
        RAISE EXCEPTION 'model_training_props public all-row SELECT policy changed; review row visibility';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_policies p
        WHERE ((p.schemaname='mlb' AND p.tablename IN ('player_stats','player_derived_stats','player_ids'))
            OR (p.schemaname='public' AND p.tablename='mlb_team_map'))
           AND (p.roles @> ARRAY['proppadia_mlb_research_ro']::name[]
                OR p.roles @> ARRAY['proppadia_mlb_research_login']::name[])
    ) THEN
        RAISE EXCEPTION 'research SELECT policy already exists; inspect before proceeding';
    END IF;
END
$preflight$;

-- The only all-row exception is model_training_props, whose existing PUBLIC
-- SELECT USING (true) policy is accepted for the five inventoried columns.
GRANT SELECT (
    player_id, game_id, game_date, team, opponent, position, hits, hits_allowed,
    outs_recorded, is_starter, earned_runs, strikeouts_pitching, at_bats, plate_appearances
) ON mlb.player_stats TO proppadia_mlb_research_ro;

GRANT SELECT (
    player_id, game_id, game_date, team,
    d7_hits, d7_runs_scored, d7_rbis, d7_home_runs, d7_singles, d7_doubles,
    d7_triples, d7_walks, d7_strikeouts_batting, d7_stolen_bases, d7_total_bases,
    d7_hits_runs_rbis, d7_runs_rbis, d7_outs_recorded, d7_strikeouts_pitching,
    d7_walks_allowed, d7_earned_runs, d7_hits_allowed, d7_at_bats, d7_plate_appearances,
    d15_hits, d15_runs_scored, d15_rbis, d15_home_runs, d15_singles, d15_doubles,
    d15_triples, d15_walks, d15_strikeouts_batting, d15_stolen_bases, d15_total_bases,
    d15_hits_runs_rbis, d15_runs_rbis, d15_outs_recorded, d15_strikeouts_pitching,
    d15_walks_allowed, d15_earned_runs, d15_hits_allowed, d15_at_bats, d15_plate_appearances,
    d30_hits, d30_runs_scored, d30_rbis, d30_home_runs, d30_singles, d30_doubles,
    d30_triples, d30_walks, d30_strikeouts_batting, d30_stolen_bases, d30_total_bases,
    d30_hits_runs_rbis, d30_runs_rbis, d30_outs_recorded, d30_strikeouts_pitching,
    d30_walks_allowed, d30_earned_runs, d30_hits_allowed, d30_at_bats, d30_plate_appearances
) ON mlb.player_derived_stats TO proppadia_mlb_research_ro;

GRANT SELECT (player_id, player_name, team, team_id)
    ON mlb.player_ids TO proppadia_mlb_research_ro;
GRANT SELECT (player_id, game_id, team_id)
    ON mlb.player_team_by_game TO proppadia_mlb_research_ro;
GRANT SELECT (team_id, abbr)
    ON public.mlb_team_map TO proppadia_mlb_research_ro;
GRANT SELECT (game_date, game_id, player_id, prop_type, rolling_result_avg_7)
    ON mlb.model_training_props TO proppadia_mlb_research_ro;

CREATE POLICY proppadia_mlb_research_select ON mlb.player_stats
    FOR SELECT TO proppadia_mlb_research_ro USING (true);
CREATE POLICY proppadia_mlb_research_select ON mlb.player_derived_stats
    FOR SELECT TO proppadia_mlb_research_ro USING (true);
CREATE POLICY proppadia_mlb_research_select ON mlb.player_ids
    FOR SELECT TO proppadia_mlb_research_ro USING (true);
CREATE POLICY proppadia_mlb_research_select ON public.mlb_team_map
    FOR SELECT TO proppadia_mlb_research_ro USING (true);

COMMIT;
