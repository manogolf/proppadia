-- Remove only policies introduced by 20261021. Preserve table RLS state and
-- all pre-existing API policies; do not grant broad access as a fallback.
DROP POLICY IF EXISTS proppadia_mlb_ingest_select ON mlb.player_stats;
DROP POLICY IF EXISTS proppadia_mlb_ingest_select ON mlb.model_training_props;
DROP POLICY IF EXISTS proppadia_mlb_ingest_update ON mlb.model_training_props;
DROP POLICY IF EXISTS proppadia_mlb_ingest_select ON mlb.player_derived_stats;
DROP POLICY IF EXISTS proppadia_mlb_ingest_insert ON mlb.player_derived_stats;
DROP POLICY IF EXISTS proppadia_mlb_ingest_update ON mlb.player_derived_stats;
