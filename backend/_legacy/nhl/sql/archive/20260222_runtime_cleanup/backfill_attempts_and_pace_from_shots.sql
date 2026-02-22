\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- ----------------------------------------------------------------------
-- 1) Build a roll-10 *attempts* materialized view from nhl.shots_all
-- ----------------------------------------------------------------------
-- This view produces, for each (player_id, game_id), the average shot
-- attempts per game over the last 10 games (or fewer if <10 so far).
-- We treat "attempts" as any event row where there is a shooter and
-- the event looks like a shot (SHOT / GOAL / MISS / BLOCK), without
-- making assumptions about TOI. This is strictly better than SOG-only.
-- ----------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS nhl.tf_skater_attempts_roll10;

CREATE MATERIALIZED VIEW nhl.tf_skater_attempts_roll10 AS
WITH per_game AS (
  SELECT
    s.game_id,
    s.shooterplayerid AS player_id,
    COUNT(*) FILTER (
      WHERE s.shooterplayerid IS NOT NULL
        AND (
              s.event IN ('SHOT','GOAL','MISS','BLOCK')
           OR s.shotwasongoal = 1
           OR s.goal = 1
        )
    ) AS attempts
  FROM nhl.shots_all s
  WHERE s.shooterplayerid IS NOT NULL
  GROUP BY s.game_id, s.shooterplayerid
),
joined AS (
  SELECT
    g.game_date::date AS game_date,
    g.game_id,
    p.player_id,
    p.attempts
  FROM per_game p
  JOIN nhl.games g USING (game_id)
)
SELECT
  j.player_id,
  j.game_id,
  j.game_date,
  -- roll-10 attempts per game (not TOI-normalized; per-game is what we
  -- have without pulling in shift/TOI yet)
  SUM(j.attempts) OVER w
    / LEAST(10, COUNT(*) OVER w)::numeric AS attempts_d10_per_game
FROM joined j
WINDOW w AS (
  PARTITION BY j.player_id
  ORDER BY j.game_date, j.game_id
  ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tf_skater_attempts_roll10
  ON nhl.tf_skater_attempts_roll10 (player_id, game_id);

\echo 'Created nhl.tf_skater_attempts_roll10'

-- ----------------------------------------------------------------------
-- 2) Backfill attempts_d10_per60 in nhl.training_features_nhl_sog_v2
--    using the roll-10 attempts per game from the MV above.
--
-- NOTE: This writes a *real* attempts-based number into the column,
--       even though the column is named "per60". We can later upgrade
--       to true per-60 using TOI once the shift/TOI roll view is wired.
-- ----------------------------------------------------------------------

WITH updated AS (
  UPDATE nhl.training_features_nhl_sog_v2 t
  SET attempts_d10_per60 = r.attempts_d10_per_game
  FROM nhl.tf_skater_attempts_roll10 r
  WHERE t.player_id = r.player_id
    AND t.game_id   = r.game_id
  RETURNING 1
)
SELECT COUNT(*) AS attempts_rows_updated FROM updated;

-- ----------------------------------------------------------------------
-- 3) Fill pace_* columns from existing team roll-10 SOG stats.
--
-- We already have:
--   - team_d10_sf_per_game
--   - opp_d10_sf_allowed_per_game
--
-- Here we simply mirror them into the newer "*_per60" columns that
-- were left NULL. This keeps the richer team/opponent context without
-- inventing new "fake" numbers.
-- ----------------------------------------------------------------------

WITH updated_pace AS (
  UPDATE nhl.training_features_nhl_sog_v2 t
  SET
    opp_d10_sf_per60  = COALESCE(t.opp_d10_sf_per60,  t.opp_d10_sf_allowed_per_game),
    team_d10_sa_per60 = COALESCE(t.team_d10_sa_per60, t.team_d10_sf_per_game)
  WHERE
      t.opp_d10_sf_per60  IS NULL
   OR t.team_d10_sa_per60 IS NULL
  RETURNING 1
)
SELECT COUNT(*) AS pace_rows_updated FROM updated_pace;

-- ----------------------------------------------------------------------
-- 4) Quick coverage check for sanity.
-- ----------------------------------------------------------------------

SELECT json_build_object(
  'total_rows',        COUNT(*)::int,
  'attempts_non_null', COUNT(*) FILTER (WHERE attempts_d10_per60 IS NOT NULL)::int,
  'pace_non_null',     COUNT(*) FILTER (
                          WHERE pace_index IS NOT NULL
                            AND opp_d10_sf_per60 IS NOT NULL
                            AND team_d10_sa_per60 IS NOT NULL
                        )::int,
  'opp_sf_non_null',   COUNT(*) FILTER (WHERE opp_d10_sf_per60  IS NOT NULL)::int,
  'team_sa_non_null',  COUNT(*) FILTER (WHERE team_d10_sa_per60 IS NOT NULL)::int
) AS coverage
FROM nhl.training_features_nhl_sog_v2;
