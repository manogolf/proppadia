-- Only the ingest group can call the new function.
-- Expect: ingest_execute = true, api_execute = false, public_execute = false.
SELECT has_function_privilege('proppadia_mlb_ingest', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)', 'EXECUTE') AS ingest_execute,
       has_function_privilege('proppadia_api', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)', 'EXECUTE') AS api_execute,
       has_function_privilege('anon', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)', 'EXECUTE') AS public_execute;
