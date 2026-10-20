-- Read-only checks. Run in the Supabase SQL editor after
-- 20261020_mlb_phase_authority_db_chain.sql has committed. The editor's
-- postgres login cannot SET ROLE to these groups, so privileges are checked directly.

-- Query 1. Expect: t, f, f, f, f, t, f, f, f, f, t, t, t, 0.
SELECT has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT'),
       has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'INSERT'),
       has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'UPDATE'),
       has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'DELETE'),
       has_function_privilege('proppadia_api', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)', 'EXECUTE'),
       has_function_privilege('proppadia_mlb_ingest', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)', 'EXECUTE'),
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'INSERT'),
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'DELETE'),
       has_table_privilege('anon', 'mlb.phase_authority_chain_entries', 'SELECT'),
       has_table_privilege('authenticated', 'mlb.phase_authority_chain_entries', 'SELECT'),
       (SELECT prosecdef FROM pg_proc WHERE oid = 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)'::regprocedure),
       (SELECT relrowsecurity FROM pg_class WHERE oid = 'mlb.phase_authority_chain_entries'::regclass),
       (SELECT count(*) = 2 FROM pg_trigger WHERE tgrelid = 'mlb.phase_authority_chain_entries'::regclass AND NOT tgisinternal),
       (SELECT count(*) FROM mlb.phase_authority_chain_entries);

-- Query 2. Expect: {search_path=pg_catalog}.
SELECT proconfig FROM pg_proc
 WHERE oid = 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)'::regprocedure;
