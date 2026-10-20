-- Prepared 2026-10-20; NOT APPLIED. Operator runs it in the Supabase SQL editor.
-- Append-only, hash-chained MLB phase-authority entries (DB chain V1).
-- Contract: docs/contracts/mlb_phase_authority_snapshot_only_read_v1.md.
--
-- Writer: proppadia_mlb_ingest, through mlb.append_phase_authority_chain_entry
--   only (SECURITY DEFINER, fixed search_path). It gets no direct INSERT,
--   UPDATE, DELETE, or TRUNCATE on the table.
-- Reader: proppadia_api (hosted API) and proppadia_mlb_ingest (the writer
--   re-verifies the chain before appending): SELECT only.
-- The function checks sequence, previous-hash link, anchor, entry hash and
--   chain-wide signing key id. It cannot check the HMAC signature (the key
--   never enters the database); every reader re-verifies signatures.
-- Triggers refuse UPDATE, DELETE and TRUNCATE for every role, the owner included.
BEGIN;

DO $preflight$
BEGIN
    IF current_setting('server_version_num')::integer < 150000 THEN
        RAISE EXCEPTION 'PostgreSQL 15 or later is required';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = 'proppadia_mlb_ingest' AND NOT rolcanlogin) THEN
        RAISE EXCEPTION 'required group proppadia_mlb_ingest is missing or can log in';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = 'proppadia_api' AND NOT rolcanlogin) THEN
        RAISE EXCEPTION 'required group proppadia_api is missing or can log in (apply 20261019_hosted_api_login.sql first)';
    END IF;
    IF pg_catalog.to_regclass('mlb.phase_authority_chain_entries') IS NOT NULL
       OR pg_catalog.to_regprocedure(
           'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'phase authority chain objects already exist; review before replacing';
    END IF;
END
$preflight$;

CREATE TABLE mlb.phase_authority_chain_entries (
    chain_id               text        NOT NULL CHECK (chain_id ~ '^[A-Z0-9_]{1,80}$'),
    sequence               bigint      NOT NULL CHECK (sequence >= 1),
    game_pk                bigint      NOT NULL CHECK (game_pk > 0),
    prev_entry_sha256      text        NOT NULL CHECK (prev_entry_sha256 ~ '^[0-9a-f]{64}$'),
    entry_sha256           text        NOT NULL CHECK (entry_sha256 ~ '^[0-9a-f]{64}$'),
    entry_text             text        NOT NULL,
    signature_hmac_sha256  text        NOT NULL CHECK (signature_hmac_sha256 ~ '^[0-9a-f]{64}$'),
    signing_key_id         text        NOT NULL CHECK (signing_key_id ~ '^[0-9a-f]{64}$'),
    appended_at            timestamptz NOT NULL DEFAULT pg_catalog.now(),
    appended_by            text        NOT NULL DEFAULT session_user,
    PRIMARY KEY (chain_id, sequence),
    UNIQUE (chain_id, game_pk),
    UNIQUE (entry_sha256),
    CHECK (entry_sha256 = pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(entry_text, 'UTF8')), 'hex'))
);

COMMENT ON TABLE mlb.phase_authority_chain_entries IS
    'Append-only signed MLB phase-authority entries; see docs/contracts/mlb_phase_authority_snapshot_only_read_v1.md';

CREATE FUNCTION mlb.refuse_phase_authority_chain_change() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    RAISE EXCEPTION 'mlb.phase_authority_chain_entries is append-only (%)', TG_OP
        USING ERRCODE = '42501';
END
$function$;

CREATE TRIGGER phase_authority_chain_no_update_delete
    BEFORE UPDATE OR DELETE ON mlb.phase_authority_chain_entries
    FOR EACH ROW EXECUTE FUNCTION mlb.refuse_phase_authority_chain_change();
CREATE TRIGGER phase_authority_chain_no_truncate
    BEFORE TRUNCATE ON mlb.phase_authority_chain_entries
    FOR EACH STATEMENT EXECUTE FUNCTION mlb.refuse_phase_authority_chain_change();

CREATE FUNCTION mlb.append_phase_authority_chain_entry(
    p_chain_id text,
    p_sequence bigint,
    p_game_pk bigint,
    p_prev_entry_sha256 text,
    p_entry_sha256 text,
    p_entry_text text,
    p_signature_hmac_sha256 text,
    p_signing_key_id text
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
       OR v_entry ->> 'signing_key_id' IS DISTINCT FROM p_signing_key_id
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
           OR v_entry ->> 'anchor_descriptor_sha256' IS DISTINCT FROM v_first ->> 'anchor_descriptor_sha256'
           OR p_signing_key_id IS DISTINCT FROM v_head.signing_key_id THEN
            RAISE EXCEPTION 'chain entry does not extend the current head' USING ERRCODE = '23514';
        END IF;
    END IF;

    INSERT INTO mlb.phase_authority_chain_entries (
        chain_id, sequence, game_pk, prev_entry_sha256, entry_sha256, entry_text,
        signature_hmac_sha256, signing_key_id
    ) VALUES (
        p_chain_id, p_sequence, p_game_pk, p_prev_entry_sha256, p_entry_sha256, p_entry_text,
        p_signature_hmac_sha256, p_signing_key_id
    );
    RETURN 1;
END
$function$;

REVOKE ALL ON TABLE mlb.phase_authority_chain_entries FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.refuse_phase_authority_chain_change() FROM PUBLIC;
REVOKE ALL ON FUNCTION mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)
    TO proppadia_mlb_ingest;
GRANT SELECT ON TABLE mlb.phase_authority_chain_entries TO proppadia_api, proppadia_mlb_ingest;

-- RLS keeps anon/authenticated API roles out even if the schema is exposed.
-- The SECURITY DEFINER owner bypasses RLS for the append.
ALTER TABLE mlb.phase_authority_chain_entries ENABLE ROW LEVEL SECURITY;
CREATE POLICY phase_authority_chain_select_api ON mlb.phase_authority_chain_entries
    FOR SELECT TO proppadia_api USING (true);
CREATE POLICY phase_authority_chain_select_ingest ON mlb.phase_authority_chain_entries
    FOR SELECT TO proppadia_mlb_ingest USING (true);

DO $postcheck$
BEGIN
    IF NOT pg_catalog.has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'SELECT')
       OR pg_catalog.has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'INSERT')
       OR pg_catalog.has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'UPDATE')
       OR pg_catalog.has_table_privilege('proppadia_api', 'mlb.phase_authority_chain_entries', 'DELETE')
       OR pg_catalog.has_function_privilege('proppadia_api',
           'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)', 'EXECUTE') THEN
        RAISE EXCEPTION 'proppadia_api must have SELECT only on the phase authority chain';
    END IF;
    IF NOT pg_catalog.has_function_privilege('proppadia_mlb_ingest',
           'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text,text,text)', 'EXECUTE')
       OR pg_catalog.has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'INSERT')
       OR pg_catalog.has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'UPDATE')
       OR pg_catalog.has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'DELETE')
       OR pg_catalog.has_table_privilege('proppadia_mlb_ingest', 'mlb.phase_authority_chain_entries', 'TRUNCATE') THEN
        RAISE EXCEPTION 'proppadia_mlb_ingest must append only through the function';
    END IF;
END
$postcheck$;

COMMIT;

-- Verification queries: 20261020_mlb_phase_authority_db_chain_verify.sql
