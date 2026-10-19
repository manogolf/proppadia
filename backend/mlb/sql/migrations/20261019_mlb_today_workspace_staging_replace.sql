-- Prepared 2026-10-19; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Owner-controlled capability for build_mlb_today_workspace.py: replace one
-- slate date's rows in the three /mlb/today staging tables in one call, so the
-- daily build can run on the restricted ingest login. Ingest gets EXECUTE on
-- this function only; it gets no direct INSERT, DELETE, or DDL on the tables.
-- The function takes no table or column names; it can touch only rows whose
-- slate_date equals p_slate_date, and every supplied row must carry that date.
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
         WHERE rolname = 'proppadia_mlb_ingest' AND NOT rolcanlogin
    ) THEN
        RAISE EXCEPTION 'required group proppadia_mlb_ingest is missing or can log in';
    END IF;

    IF pg_catalog.to_regprocedure('mlb.replace_today_workspace_staging(date,jsonb,jsonb,jsonb)') IS NOT NULL THEN
        RAISE EXCEPTION 'mlb.replace_today_workspace_staging already exists; review before replacing';
    END IF;

    FOREACH relation_name IN ARRAY ARRAY['today_slate_rows', 'today_wide_rows', 'today_odds_book_rows'] LOOP
        relation_oid := pg_catalog.to_regclass('mlb.' || relation_name);
        IF relation_oid IS NULL OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
             WHERE c.oid = relation_oid AND c.relkind = 'r'
               AND NOT c.relrowsecurity
               AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
        ) THEN
            RAISE EXCEPTION 'mlb.% is missing, not a plain table, has RLS, or is not owned by current_user', relation_name;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = relation_oid AND NOT t.tgisinternal) THEN
            RAISE EXCEPTION 'mlb.% has user triggers; review before granting a definer write path', relation_name;
        END IF;
    END LOOP;
END
$preflight$;

CREATE FUNCTION mlb.replace_today_workspace_staging(
    p_slate_date date,
    p_slate_rows jsonb,
    p_wide_rows jsonb,
    p_odds_rows jsonb
)
RETURNS TABLE (slate_rows_written integer, wide_rows_written integer, odds_rows_written integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $replace$
DECLARE
    n_slate integer;
    n_wide integer;
    n_odds integer;
BEGIN
    IF p_slate_date IS NULL
       OR jsonb_typeof(p_slate_rows) IS DISTINCT FROM 'array'
       OR jsonb_typeof(p_wide_rows) IS DISTINCT FROM 'array'
       OR jsonb_typeof(p_odds_rows) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'slate date and three JSON arrays are required' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM (
            SELECT e FROM jsonb_array_elements(p_slate_rows) AS e
            UNION ALL SELECT e FROM jsonb_array_elements(p_wide_rows) AS e
            UNION ALL SELECT e FROM jsonb_array_elements(p_odds_rows) AS e
        ) AS rows(e)
        WHERE jsonb_typeof(e) IS DISTINCT FROM 'object'
           OR (e ->> 'slate_date') IS NULL
           OR (e ->> 'slate_date')::date <> p_slate_date
    ) THEN
        RAISE EXCEPTION 'every staging row must be an object for the requested slate date' USING ERRCODE = '22023';
    END IF;

    DELETE FROM mlb.today_odds_book_rows WHERE slate_date = p_slate_date;
    DELETE FROM mlb.today_slate_rows WHERE slate_date = p_slate_date;
    DELETE FROM mlb.today_wide_rows WHERE slate_date = p_slate_date;

    INSERT INTO mlb.today_slate_rows (
        slate_date, game_date, game_id, player_id, player_name, player_name_norm,
        home_team_code, away_team_code, prop_type, market_key, line, prob_over, prob_under
    )
    SELECT slate_date, game_date, game_id, player_id, player_name, player_name_norm,
           home_team_code, away_team_code, prop_type, market_key, line, prob_over, prob_under
      FROM jsonb_populate_recordset(NULL::mlb.today_slate_rows, p_slate_rows);
    GET DIAGNOSTICS n_slate = ROW_COUNT;

    INSERT INTO mlb.today_wide_rows (
        slate_date, game_date, game_id, player_id, player_name, player_name_norm,
        prop_type, team, opponent, is_home
    )
    SELECT slate_date, game_date, game_id, player_id, player_name, player_name_norm,
           prop_type, team, opponent, is_home
      FROM jsonb_populate_recordset(NULL::mlb.today_wide_rows, p_wide_rows);
    GET DIAGNOSTICS n_wide = ROW_COUNT;

    INSERT INTO mlb.today_odds_book_rows (
        slate_date, game_date, snapshot_ts, snapshot_file, event_id, game_id, player_id,
        player_name, player_name_norm, home_team_code, away_team_code, team, opponent,
        is_home, prop_type, market_key, line, bookmaker_key, price_over_american,
        price_under_american
    )
    SELECT slate_date, game_date, snapshot_ts, snapshot_file, event_id, game_id, player_id,
           player_name, player_name_norm, home_team_code, away_team_code, team, opponent,
           is_home, prop_type, market_key, line, bookmaker_key, price_over_american,
           price_under_american
      FROM jsonb_populate_recordset(NULL::mlb.today_odds_book_rows, p_odds_rows);
    GET DIAGNOSTICS n_odds = ROW_COUNT;

    RETURN QUERY SELECT n_slate, n_wide, n_odds;
END
$replace$;

REVOKE ALL ON FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb) TO proppadia_mlb_ingest;

COMMIT;

-- Verification (run after COMMIT; expect t, t, f, f):
-- SELECT p.prosecdef, has_function_privilege('proppadia_mlb_ingest', p.oid, 'EXECUTE'),
--        has_function_privilege('proppadia_mlb_research_ro', p.oid, 'EXECUTE'),
--        has_function_privilege('public', p.oid, 'EXECUTE')
--   FROM pg_proc p WHERE p.oid = 'mlb.replace_today_workspace_staging(date,jsonb,jsonb,jsonb)'::regprocedure;
