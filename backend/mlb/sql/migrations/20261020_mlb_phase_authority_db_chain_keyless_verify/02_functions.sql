-- Only the new 6-argument append function exists.
-- Expect: new_function_exists = true, old_function_exists = false.
SELECT to_regprocedure('mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)') IS NOT NULL AS new_function_exists,
       to_regprocedure('mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)') IS NOT NULL AS old_function_exists;
