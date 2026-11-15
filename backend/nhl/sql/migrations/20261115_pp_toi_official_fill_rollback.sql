-- ROLLBACK ONLY with separate operator approval. Removes the official PP TOI
-- fill entry point; the daily fill then falls back to shift-chart overlap.
-- Values already written stay in place.
BEGIN;

REVOKE EXECUTE ON FUNCTION nhl.fill_pp_toi_minutes_from_official(bigint, jsonb)
  FROM proppadia_nhl_ingest;
DROP FUNCTION nhl.fill_pp_toi_minutes_from_official(bigint, jsonb);

COMMIT;
