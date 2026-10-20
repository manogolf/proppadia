-- Row-level security is on and both append-only triggers exist.
-- Expect: rls_enabled = true, trigger_count = 2.
SELECT (SELECT relrowsecurity FROM pg_class WHERE oid = 'mlb.phase_authority_chain_entries'::regclass) AS rls_enabled,
       (SELECT count(*) FROM pg_trigger
         WHERE tgrelid = 'mlb.phase_authority_chain_entries'::regclass AND NOT tgisinternal) AS trigger_count;
