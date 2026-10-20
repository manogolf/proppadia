-- MLB ingest group: append through the function only.
-- Expect: ingest_execute_append = true, ingest_select = true; the rest = false.
SELECT has_function_privilege('proppadia_mlb_ingest', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)', 'EXECUTE') AS ingest_execute_append,
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'SELECT') AS ingest_select,
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'INSERT') AS ingest_insert,
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'UPDATE') AS ingest_update,
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'DELETE') AS ingest_delete,
       has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'TRUNCATE') AS ingest_truncate;
