-- backend/nhl/sql/fix_sog_per60_null_if_stub.sql
-- Option A: If the "TOI-missing fallback" path is being used (per60 == count * 6),
-- return NULL instead of a fake per-60 value.

BEGIN;

DO $$
DECLARE
  k char;
BEGIN
  -- Find what kind of relation v2 is (view or matview)
  SELECT c.relkind INTO k
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'nhl'
    AND c.relname = 'training_features_nhl_sog_enriched_pregame_v2';

  IF k IS NULL THEN
    RAISE EXCEPTION 'nhl.training_features_nhl_sog_enriched_pregame_v2 does not exist';
  END IF;

  -- If _raw doesn't exist yet, rename v2 -> v2_raw using the right ALTER
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'nhl'
      AND c.relname = 'training_features_nhl_sog_enriched_pregame_v2_raw'
  ) THEN
    IF k = 'v' THEN
      EXECUTE 'ALTER VIEW nhl.training_features_nhl_sog_enriched_pregame_v2 RENAME TO training_features_nhl_sog_enriched_pregame_v2_raw';
    ELSIF k = 'm' THEN
      EXECUTE 'ALTER MATERIALIZED VIEW nhl.training_features_nhl_sog_enriched_pregame_v2 RENAME TO training_features_nhl_sog_enriched_pregame_v2_raw';
    ELSE
      RAISE EXCEPTION 'Unsupported relkind=% for v2; expected view (v) or matview (m)', k;
    END IF;
  END IF;
END $$;

-- Recreate v2 as a VIEW wrapper that nulls the sentinel "*6" values
CREATE OR REPLACE VIEW nhl.training_features_nhl_sog_enriched_pregame_v2 AS
SELECT
  player_id,
  game_id,
  team_id,
  opponent_id,
  is_home,
  game_date,
  shots_on_goal,

  CASE
    WHEN num_shotwasongoal_last5 IS NOT NULL
     AND num_shotwasongoal_last5 <> 0
     AND d5_sog_per60 IS NOT NULL
     AND d5_sog_per60 = (num_shotwasongoal_last5::numeric * 6)
    THEN NULL
    ELSE d5_sog_per60
  END AS d5_sog_per60,

  CASE
    WHEN num_shotwasongoal_last10 IS NOT NULL
     AND num_shotwasongoal_last10 <> 0
     AND d10_sog_per60 IS NOT NULL
     AND d10_sog_per60 = (num_shotwasongoal_last10::numeric * 6)
    THEN NULL
    ELSE d10_sog_per60
  END AS d10_sog_per60,

  -- leave d20 as-is (no last20 count column available here)
  d20_sog_per60,

  team_d10_sf_per_game,
  opp_d10_sf_allowed_per_game,
  pace_matchup_index,
  role_pp_share,
  rest_days,
  b2b_flag,
  season,

  CASE
    WHEN num_event_shot_last10 IS NOT NULL
     AND num_event_shot_last10 <> 0
     AND attempts_d10_per60 IS NOT NULL
     AND attempts_d10_per60 = (num_event_shot_last10::numeric * 6)
    THEN NULL
    ELSE attempts_d10_per60
  END AS attempts_d10_per60,

  last10_team_sog_share,
  hot_last5_flag,
  num_shotwasongoal_last5,
  num_shotwasongoal_last10,
  num_shotwasongoal_season_to_date,
  num_event_shot_last5,
  num_event_shot_last10,
  num_event_shot_season_to_date,
  team_num_event_shot_for_last10,
  team_num_shotwasongoal_for_last10
FROM nhl.training_features_nhl_sog_enriched_pregame_v2_raw;

COMMIT;
