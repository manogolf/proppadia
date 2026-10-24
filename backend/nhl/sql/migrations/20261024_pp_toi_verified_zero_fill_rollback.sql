-- ROLLBACK ONLY with separate operator approval. Removes the verified-zero
-- fill entry point. Zero values already written stay in place; reverting a
-- backfill uses its own receipt-bound rollback file, not this one.
BEGIN;

REVOKE EXECUTE ON FUNCTION nhl.fill_pp_toi_zero_from_shift_overlap(bigint)
  FROM proppadia_nhl_ingest;
DROP FUNCTION nhl.fill_pp_toi_zero_from_shift_overlap(bigint);

COMMIT;
