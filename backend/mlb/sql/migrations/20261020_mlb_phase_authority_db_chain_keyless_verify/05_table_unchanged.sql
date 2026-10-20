-- Table guards and grants are unchanged and it is still empty.
-- Expect: api_select = true, ingest_insert = false, rls_enabled = true, trigger_count = 2, entry_count = 0.
SELECT has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT') AS api_select,
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'INSERT') AS ingest_insert,
       (SELECT relrowsecurity FROM pg_class WHERE oid = 'mlb.phase_authority_chain_entries'::regclass) AS rls_enabled,
       (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'mlb.phase_authority_chain_entries'::regclass AND NOT tgisinternal) AS trigger_count,
       (SELECT count(*) FROM mlb.phase_authority_chain_entries) AS entry_count;
