-- Read-only check of the P2.31 verified-zero PP-TOI fill entry point.
-- Run before (expect installed=false and the rest NULL/false) and after the
-- operator applies operator_sql_editor_pp_toi_verified_zero_fill.sql (expect
-- installed=true, daily_can_execute=true, research_can_execute=false,
-- anon_can_execute=false, public_execute=false, security_definer=true,
-- search_path_fixed=true).
SELECT to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NOT NULL AS installed,
       CASE WHEN to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NULL THEN NULL
            ELSE has_function_privilege('proppadia_nhl_daily_login', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') END AS daily_can_execute,
       CASE WHEN to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NULL THEN NULL
            ELSE has_function_privilege('proppadia_nhl_research_login', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') END AS research_can_execute,
       CASE WHEN to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NULL THEN NULL
            ELSE has_function_privilege('anon', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') END AS anon_can_execute,
       EXISTS (SELECT 1 FROM pg_proc p, aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
                WHERE p.oid = to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)')
                  AND a.grantee = 0 AND a.privilege_type = 'EXECUTE') AS public_execute,
       (SELECT prosecdef FROM pg_proc WHERE oid = to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)')) AS security_definer,
       (SELECT 'search_path=pg_catalog, nhl' = ANY(proconfig) FROM pg_proc
         WHERE oid = to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)')) AS search_path_fixed;
