-- Supabase public API roles cannot read the table.
-- Expect: anon_select = false, authenticated_select = false.
SELECT has_table_privilege('anon', 'mlb.phase_authority_chain_entries', 'SELECT') AS anon_select,
       has_table_privilege('authenticated', 'mlb.phase_authority_chain_entries', 'SELECT') AS authenticated_select;
