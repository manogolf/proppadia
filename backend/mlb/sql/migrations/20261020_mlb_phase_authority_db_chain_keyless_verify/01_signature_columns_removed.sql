-- The two signature columns are gone.
-- Expect: signature_columns = 0.
SELECT count(*) AS signature_columns
  FROM pg_attribute
 WHERE attrelid = 'mlb.phase_authority_chain_entries'::regclass AND attnum > 0 AND NOT attisdropped
   AND attname IN ('signature_hmac_sha256', 'signing_key_id');
