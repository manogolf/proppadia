-- Prepared 2026-10-19; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Adds the columns that the remaining nine daily MLB readers select but the
-- research group cannot read yet (inventory:
-- docs/operations/mlb_owner_alias_inventory_2026-10-19.md). Column SELECT only;
-- no write privileges, policies, or role changes.
-- Row visibility: player_stats and player_ids already have the research SELECT
-- policy; model_training_props has the PUBLIC SELECT USING (true) policy;
-- game_info, prop_features_precomputed, today_odds_book_rows, and the two
-- materialized views have no RLS.
-- today_workspace_mlb is read as w.* by the ops-brief workspace probe, so all
-- 39 current columns are listed. A rebuild (DROP/CREATE) of that view drops
-- these grants; a REFRESH keeps them.
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

    -- Expected RLS shape; a change means row visibility needs review first.
    FOR v_required IN
        SELECT * FROM (VALUES
            ('mlb.model_training_props', true), ('mlb.player_stats', true),
            ('mlb.player_ids', true), ('mlb.game_info', false),
            ('mlb.prop_features_precomputed', false), ('mlb.today_odds_book_rows', false),
            ('mlb.today_market_snapshot', false), ('mlb.today_workspace_mlb', false)
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
         WHERE p.schemaname = 'mlb' AND p.tablename IN ('player_stats', 'player_ids')
           AND p.policyname = 'proppadia_mlb_research_select' AND p.cmd = 'SELECT'
    ) <> 2 THEN
        RAISE EXCEPTION 'expected research row-visibility policies changed; review before granting';
    END IF;
END
$preflight$;

GRANT SELECT (id, prop_source, prop_value, line)
    ON mlb.model_training_props TO proppadia_mlb_research_ro;
GRANT SELECT (
    is_home, singles, doubles, triples, home_runs, total_bases, runs_scored, rbis,
    walks, strikeouts_batting, stolen_bases, pa_source, pa_backfilled_at,
    hit_by_pitch, sacrifice_flies, sacrifice_hits, catcher_interference
) ON mlb.player_stats TO proppadia_mlb_research_ro;
GRANT SELECT (game_time, home_team_id, away_team_id)
    ON mlb.game_info TO proppadia_mlb_research_ro;
GRANT SELECT (is_placeholder)
    ON mlb.player_ids TO proppadia_mlb_research_ro;
GRANT SELECT (lineup_slot)
    ON mlb.prop_features_precomputed TO proppadia_mlb_research_ro;
GRANT SELECT (slate_date, snapshot_ts)
    ON mlb.today_odds_book_rows TO proppadia_mlb_research_ro;
GRANT SELECT (game_date, last_snapshot_ts)
    ON mlb.today_market_snapshot TO proppadia_mlb_research_ro;
GRANT SELECT (
    game_date, game_id, player_id, player_name, team, opponent, is_home, prop_type, line, side,
    best_price, best_price_book, market_median, market_range, value_vs_market, open_price,
    latest_price, minutes_since_open, num_snapshots, price_change_from_open, book_count,
    price_dispersion, coverage_quality_label, coverage_quality_reason, timing_signal,
    timing_reason, streak_context_label, consistency_score, last_5_avg, last_10_avg,
    season_avg, hit_rate_last_5, hit_rate_last_10, hit_rate_season, streak_type,
    streak_count, baseline_delta, decision_label, decision_reason
) ON mlb.today_workspace_mlb TO proppadia_mlb_research_ro;

COMMIT;

-- Verification (run after COMMIT; expect zero rows):
-- SELECT rel, col FROM (VALUES
--   ('mlb.model_training_props','id'), ('mlb.model_training_props','prop_source'), ('mlb.model_training_props','prop_value'),
--   ('mlb.model_training_props','line'), ('mlb.player_stats','is_home'), ('mlb.player_stats','singles'),
--   ('mlb.player_stats','doubles'), ('mlb.player_stats','triples'), ('mlb.player_stats','home_runs'),
--   ('mlb.player_stats','total_bases'), ('mlb.player_stats','runs_scored'), ('mlb.player_stats','rbis'),
--   ('mlb.player_stats','walks'), ('mlb.player_stats','strikeouts_batting'), ('mlb.player_stats','stolen_bases'),
--   ('mlb.player_stats','pa_source'), ('mlb.player_stats','pa_backfilled_at'), ('mlb.player_stats','hit_by_pitch'),
--   ('mlb.player_stats','sacrifice_flies'), ('mlb.player_stats','sacrifice_hits'), ('mlb.player_stats','catcher_interference'),
--   ('mlb.game_info','game_time'), ('mlb.game_info','home_team_id'), ('mlb.game_info','away_team_id'),
--   ('mlb.player_ids','is_placeholder'), ('mlb.prop_features_precomputed','lineup_slot'), ('mlb.today_odds_book_rows','slate_date'),
--   ('mlb.today_odds_book_rows','snapshot_ts'), ('mlb.today_market_snapshot','game_date'), ('mlb.today_market_snapshot','last_snapshot_ts')
-- ) AS expected(rel, col)
--  WHERE NOT has_column_privilege('proppadia_mlb_research_ro', rel::regclass, col, 'SELECT')
-- UNION ALL
-- SELECT 'mlb.today_workspace_mlb', attname FROM pg_attribute
--  WHERE attrelid = 'mlb.today_workspace_mlb'::regclass AND attnum > 0 AND NOT attisdropped
--    AND NOT has_column_privilege('proppadia_mlb_research_ro', attrelid, attname, 'SELECT');
