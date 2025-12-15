BEGIN;

-- If older/incorrect views exist, remove them so nothing “refreezes” via stale definitions.
DO $$
BEGIN
  IF to_regclass('nhl.sog_denali_rollups_v') IS NOT NULL THEN
    EXECUTE 'DROP VIEW nhl.sog_denali_rollups_v';
  END IF;

  IF to_regclass('nhl.sog_denali_features_v') IS NOT NULL THEN
    EXECUTE 'DROP VIEW nhl.sog_denali_features_v';
  END IF;
END $$;

-- Base per-game skater logs we trust for rollups
-- Note: normalize text blanks safely and keep TOI numeric.
CREATE VIEW nhl.sog_denali_rollups_v AS
WITH base AS (
  SELECT
    l.player_id::bigint                       AS player_id,
    g.game_id::bigint                         AS game_id,
    g.game_date::date                         AS game_date,
    l.team_id::bigint                         AS team_id,

    -- normalize: shots_on_goal/attempts sometimes come through as '' (text) in raw
    COALESCE(NULLIF(l.shots_on_goal::text, ''), '0')::numeric     AS sog,
    COALESCE(NULLIF(l.shot_attempts::text,  ''), '0')::numeric     AS attempts,

    NULLIF(l.toi_minutes::text, '')::numeric                      AS toi_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date >= DATE '2025-10-07'
),
ordered AS (
  SELECT
    *,
    -- rolling sums over *prior* games only (exclude current game to avoid leakage)
    SUM(sog)      OVER w5  AS sog_5,
    SUM(attempts) OVER w10 AS att_10,
    SUM(sog)      OVER w10 AS sog_10,
    SUM(sog)      OVER w20 AS sog_20,
    SUM(toi_min)  OVER w5  AS toi_5,
    SUM(toi_min)  OVER w10 AS toi_10,
    SUM(toi_min)  OVER w20 AS toi_20
  FROM base
  WINDOW
    w5  AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
    w10 AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
    w20 AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
)
SELECT
  player_id,
  game_id,
  team_id,
  game_date,

  -- Per60 rollups: (sum stat / sum toi) * 60; NULL if no TOI history in window
  CASE WHEN toi_5  > 0 THEN (sog_5  / toi_5)  * 60 ELSE NULL END AS d5_sog_per60,
  CASE WHEN toi_10 > 0 THEN (sog_10 / toi_10) * 60 ELSE NULL END AS d10_sog_per60,
  CASE WHEN toi_20 > 0 THEN (sog_20 / toi_20) * 60 ELSE NULL END AS d20_sog_per60,

  CASE WHEN toi_10 > 0 THEN (att_10 / toi_10) * 60 ELSE NULL END AS attempts_d10_per60
FROM ordered;

-- Optional: a “features view” hook you can point exports at later if needed.
-- (Keeps naming consistent with what your Denali SOG export expects, but does not overwrite it.)
CREATE VIEW nhl.sog_denali_features_v AS
SELECT
  r.player_id,
  r.game_id,
  r.team_id,
  g.game_date,
  r.d5_sog_per60,
  r.d10_sog_per60,
  r.d20_sog_per60,
  r.attempts_d10_per60
FROM nhl.sog_denali_rollups_v r
JOIN nhl.games g USING (game_id);

COMMIT;

-- quick proof-of-life: show that values are not frozen for a known player (last ~90d)
DO $$
DECLARE
  pid bigint := 8479385;
  distinct_ct int;
BEGIN
  SELECT COUNT(DISTINCT d10_sog_per60)
    INTO distinct_ct
  FROM nhl.sog_denali_rollups_v
  WHERE player_id = pid
    AND game_date >= (CURRENT_DATE - INTERVAL '90 days');

  RAISE NOTICE 'player_id=% distinct d10_sog_per60 (last 90d) = %', pid, distinct_ct;
END $$;
