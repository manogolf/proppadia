-- Hosted API group: SELECT only.
-- Expect: api_select = true; every other column = false.
SELECT has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT') AS api_select,
       has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'INSERT') AS api_insert,
       has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'UPDATE') AS api_update,
       has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'DELETE') AS api_delete,
       has_function_privilege('proppadia_api', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)', 'EXECUTE') AS api_execute_append;
