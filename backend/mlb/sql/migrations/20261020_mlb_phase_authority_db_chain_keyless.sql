-- Prepared 2026-10-20; NOT APPLIED. Operator runs it in the Supabase SQL editor
-- after 20261020_mlb_phase_authority_db_chain.sql (already applied).
-- Removes HMAC signing from the DB chain: drops the two signature columns and
-- replaces the append function with one that takes no signature or key id.
-- Every other guard is unchanged: sequence and previous-hash link checks,
-- anchor check, entry-hash constraint, append-only triggers, RLS, and grants
-- (proppadia_mlb_ingest EXECUTE only; proppadia_api and ingest SELECT only).
-- Runs only while the table is empty. Rollback: the existing
-- 20261020_mlb_phase_authority_db_chain_rollback.sql, which now also handles
-- this function signature.
BEGIN;

DO $preflight$
BEGIN
    IF pg_catalog.to_regclass('mlb.phase_authority_chain_entries') IS NULL
       OR pg_catalog.to_regprocedure('mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)') IS NULL THEN
        RAISE EXCEPTION 'apply 20261020_mlb_phase_authority_db_chain.sql first';
    END IF;
    IF EXISTS (SELECT 1 FROM mlb.phase_authority_chain_entries) THEN
        RAISE EXCEPTION 'phase authority chain already has entries; review before removing signatures';
    END IF;
END
$preflight$;

DROP FUNCTION mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text);
ALTER TABLE mlb.phase_authority_chain_entries
    DROP COLUMN signature_hmac_sha256,
    DROP COLUMN signing_key_id;

CREATE FUNCTION mlb.append_phase_authority_chain_entry(
    p_chain_id text,
    p_sequence bigint,
    p_game_pk bigint,
    p_prev_entry_sha256 text,
    p_entry_sha256 text,
    p_entry_text text
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_entry jsonb;
    v_head mlb.phase_authority_chain_entries%ROWTYPE;
    v_first jsonb;
BEGIN
    IF p_chain_id IS NULL OR p_sequence IS NULL OR p_game_pk IS NULL OR p_entry_text IS NULL THEN
        RAISE EXCEPTION 'invalid phase authority chain entry' USING ERRCODE = '22023';
    END IF;
    BEGIN
        v_entry := p_entry_text::jsonb;
    EXCEPTION WHEN others THEN
        RAISE EXCEPTION 'phase authority chain entry is not JSON' USING ERRCODE = '22023';
    END;
    IF v_entry ->> 'contract_name' IS DISTINCT FROM 'MLB_PHASE_AUTHORITY_DB_CHAIN_ENTRY_V1'
       OR v_entry ->> 'schema_version' IS DISTINCT FROM '1'
       OR v_entry ->> 'chain_id' IS DISTINCT FROM p_chain_id
       OR v_entry ->> 'sequence' IS DISTINCT FROM p_sequence::text
       OR v_entry ->> 'prev_entry_sha256' IS DISTINCT FROM p_prev_entry_sha256
       OR v_entry ? 'signing_key_id'
       OR v_entry -> 'record' ->> 'game_pk' IS DISTINCT FROM p_game_pk::text
       OR v_entry -> 'record' ->> 'season_phase' IS DISTINCT FROM 'POSTSEASON'
       OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p_entry_text, 'UTF8')), 'hex')
          IS DISTINCT FROM p_entry_sha256 THEN
        RAISE EXCEPTION 'phase authority chain entry fields do not match its payload' USING ERRCODE = '22023';
    END IF;

    -- One appender at a time per chain.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('mlb.phase_authority_chain:' || p_chain_id, 0));

    IF EXISTS (SELECT 1 FROM mlb.phase_authority_chain_entries
                WHERE chain_id = p_chain_id AND game_pk = p_game_pk) THEN
        RAISE EXCEPTION 'game % is already in phase authority chain', p_game_pk USING ERRCODE = '23505';
    END IF;

    SELECT * INTO v_head FROM mlb.phase_authority_chain_entries
     WHERE chain_id = p_chain_id ORDER BY sequence DESC LIMIT 1;
    IF NOT FOUND THEN
        IF p_sequence <> 1 OR p_prev_entry_sha256 IS DISTINCT FROM v_entry ->> 'anchor_descriptor_sha256' THEN
            RAISE EXCEPTION 'first chain entry must link to its anchor descriptor' USING ERRCODE = '23514';
        END IF;
    ELSE
        SELECT entry_text::jsonb INTO v_first FROM mlb.phase_authority_chain_entries
         WHERE chain_id = p_chain_id AND sequence = 1;
        IF p_sequence <> v_head.sequence + 1
           OR p_prev_entry_sha256 IS DISTINCT FROM v_head.entry_sha256
           OR v_entry ->> 'anchor_descriptor_sha256' IS DISTINCT FROM v_first ->> 'anchor_descriptor_sha256' THEN
            RAISE EXCEPTION 'chain entry does not extend the current head' USING ERRCODE = '23514';
        END IF;
    END IF;

    INSERT INTO mlb.phase_authority_chain_entries (
        chain_id, sequence, game_pk, prev_entry_sha256, entry_sha256, entry_text
    ) VALUES (
        p_chain_id, p_sequence, p_game_pk, p_prev_entry_sha256, p_entry_sha256, p_entry_text
    );
    RETURN 1;
END
$function$;

REVOKE ALL ON FUNCTION mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text) TO proppadia_mlb_ingest;

DO $postcheck$
BEGIN
    IF NOT pg_catalog.has_function_privilege('proppadia_mlb_ingest', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)', 'EXECUTE')
       OR pg_catalog.has_function_privilege('proppadia_api', 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)', 'EXECUTE')
       OR pg_catalog.has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'INSERT')
       OR NOT pg_catalog.has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT') THEN
        RAISE EXCEPTION 'phase authority chain grants are not as expected';
    END IF;
END
$postcheck$;

COMMIT;

-- Verification queries: 20261020_mlb_phase_authority_db_chain_keyless_verify/ (one query per file, run in order)
