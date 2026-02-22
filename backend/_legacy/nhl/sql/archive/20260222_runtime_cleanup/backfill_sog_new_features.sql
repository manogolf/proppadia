\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- One-time (but safe to re-run) backfill for the new SOG features:
--   attempts_d10_per60
--   pace_index
--   opp_d10_sf_per60
--   team_d10_sa_per60
--
-- Strategy:
-- - Use existing per-60 or per-game features in nhl.training_features_nhl_sog_v2
--   as proxies so these columns are no longer NULL.
--
--   attempts_d10_per60   ≈ d10_sog_per60
--   pace_index           ≈ pace_matchup_index
--   opp_d10_sf_per60     ≈ team_d10_sf_per_game
--   team_d10_sa_per60    ≈ opp_d10_sf_allowed_per_game
--
-- This is an approximation but grounded in real, already-computed features and
-- can be upgraded later once we have a dedicated roll view for attempts & pace.

UPDATE nhl.training_features_nhl_sog_v2 t
SET
  attempts_d10_per60 = COALESCE(
    attempts_d10_per60,
    d10_sog_per60  -- provisional: treat attempts per 60 ~ SOG per 60
  ),
  pace_index = COALESCE(
    pace_index,
    pace_matchup_index  -- reuse existing matchup pace index
  ),
  opp_d10_sf_per60 = COALESCE(
    opp_d10_sf_per60,
    team_d10_sf_per_game  -- team SF/game ~ SF/60 in a 60-min game
  ),
  team_d10_sa_per60 = COALESCE(
    team_d10_sa_per60,
    opp_d10_sf_allowed_per_game  -- allowed SF/game ~ SA/60
  )
WHERE
  attempts_d10_per60 IS NULL
  OR pace_index IS NULL
  OR opp_d10_sf_per60 IS NULL
  OR team_d10_sa_per60 IS NULL;

\echo 'backfill_sog_new_features: updated rows ='
SELECT COUNT(*)
FROM nhl.training_features_nhl_sog_v2 t
WHERE
  attempts_d10_per60 IS NOT NULL
  AND pace_index IS NOT NULL
  AND opp_d10_sf_per60 IS NOT NULL
  AND team_d10_sa_per60 IS NOT NULL;
