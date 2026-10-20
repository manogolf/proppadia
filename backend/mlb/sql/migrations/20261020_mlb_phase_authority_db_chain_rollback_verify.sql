-- Read-only check. Run in the Supabase SQL editor after
-- 20261020_mlb_phase_authority_db_chain_rollback.sql has committed.
-- Expect: f, f, f. The last two are NULL if the empty table was dropped.
SELECT (to_regprocedure('mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)') IS NOT NULL
        OR to_regprocedure('mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)') IS NOT NULL) AS append_function_exists,
       CASE WHEN to_regclass('mlb.phase_authority_chain_entries') IS NULL THEN NULL
            ELSE has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT') END,
       CASE WHEN to_regclass('mlb.phase_authority_chain_entries') IS NULL THEN NULL
            ELSE has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'SELECT') END;
