Read-only checks for `20261020_mlb_phase_authority_db_chain.sql`. Run each file
on its own in the Supabase SQL editor, in order, after the migration has
committed. Each file is one query and states its expected result. The editor's
postgres login cannot `SET ROLE` to these groups, so privileges are checked
directly.
