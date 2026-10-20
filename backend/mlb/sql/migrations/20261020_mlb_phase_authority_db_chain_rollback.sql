-- Prepared 2026-10-20; NOT APPLIED. Rolls back 20261020_mlb_phase_authority_db_chain.sql.
-- Stops new appends and all reads at once. The hosted API then serves only the
-- committed snapshot; games known only to the chain are withheld again.
-- An empty table is dropped. A populated table is kept, with no grants, so the
-- signed entries are not lost; drop it separately only after review.
BEGIN;

DO $rollback$
BEGIN
    IF pg_catalog.to_regprocedure(
           'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)') IS NOT NULL THEN
        DROP FUNCTION mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text);
    END IF;
    IF pg_catalog.to_regclass('mlb.phase_authority_chain_entries') IS NULL THEN
        RETURN;
    END IF;
    REVOKE ALL ON TABLE mlb.phase_authority_chain_entries FROM proppadia_api, proppadia_mlb_ingest;
    DROP POLICY IF EXISTS phase_authority_chain_select_api ON mlb.phase_authority_chain_entries;
    DROP POLICY IF EXISTS phase_authority_chain_select_ingest ON mlb.phase_authority_chain_entries;
    IF EXISTS (SELECT 1 FROM mlb.phase_authority_chain_entries) THEN
        RAISE NOTICE 'mlb.phase_authority_chain_entries has rows; kept without grants';
    ELSE
        DROP TABLE mlb.phase_authority_chain_entries;
        DROP FUNCTION IF EXISTS mlb.refuse_phase_authority_chain_change();
    END IF;
END
$rollback$;

COMMIT;

-- Verification (SQL editor). Expect: f, f, f (or NULL for the table checks if it was dropped).
-- SELECT to_regprocedure('mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)') IS NOT NULL,
--        CASE WHEN to_regclass('mlb.phase_authority_chain_entries') IS NULL THEN NULL
--             ELSE has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT') END,
--        CASE WHEN to_regclass('mlb.phase_authority_chain_entries') IS NULL THEN NULL
--             ELSE has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'SELECT') END;
