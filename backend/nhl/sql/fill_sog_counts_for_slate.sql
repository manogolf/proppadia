-- ============================================================
-- fill_sog_counts_for_slate.sql
-- Fills Denali count features on nhl.training_features_nhl_sog_enriched_pregame_v2
-- for a single slate_date using realized history in nhl.skater_game_logs_raw.
--
-- Assumptions:
--   - "num_sog_*" comes from shots_on_goal
--   - "num_event_*" comes from shot_attempts (your current best proxy)
--   - last10_team_sog_share = player_last10_sog / team_last10_sog
--   - pace_index should equal pace_matchup_index (export alias; we also set it in-table)
-- ============================================================

\set ON_ERROR_STOP on
BEGIN;

-- 1) Ensure the columns exist (idempotent)
ALTER TABLE nhl.training_features_nhl_sog_enriched_pregame_v2
  ADD COLUMN IF NOT EXISTS num_sog_last5 numeric,
  ADD COLUMN IF NOT EXISTS num_sog_last10 numeric,
  ADD COLUMN IF NOT EXISTS num_sog_szn_to_date numeric,
  ADD COLUMN IF NOT EXISTS num_event_last5 numeric,
  ADD COLUMN IF NOT EXISTS num_event_last10 numeric,
  ADD COLUMN IF NOT EXISTS num_event_szn_to_date numeric,
  ADD COLUMN IF NOT EXISTS team_num_sog_last10 numeric,
  ADD COLUMN IF NOT EXISTS team_num_event_last10 numeric,
  ADD COLUMN IF NOT EXISTS pace_index numeric;

-- 2) Always keep pace_index aligned (cheap + deterministic)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET pace_index = t.pace_matchup_index
WHERE t.game_date = DATE :'slate_date';

-- 3) Player rolling + season-to-date aggregates (exclude slate day itself)
WITH params AS (
  SELECT (:'slate_date')::date AS slate_date
),
slate AS (
  SELECT
    t.player_id::bigint AS player_id,
    t.team_id::bigint   AS team_id,
    t.season::int       AS season
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
  JOIN params p ON TRUE
  WHERE t.game_date = p.slate_date
),
hist AS (
  SELECT
    l.player_id::bigint AS player_id,
    g.season::int       AS season,
    g.game_date::date   AS game_date,
    g.game_id::bigint   AS game_id,
    COALESCE(l.shots_on_goal, 0)::numeric AS sog,
    COALESCE(l.shot_attempts, 0)::numeric AS ev
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  JOIN params p ON TRUE
  WHERE g.game_date < p.slate_date
),
hist_ranked AS (
  SELECT
    h.*,
    ROW_NUMBER() OVER (
      PARTITION BY h.player_id, h.season
      ORDER BY h.game_date DESC, h.game_id DESC
    ) AS rn_desc
  FROM hist h
  JOIN slate s
    ON s.player_id = h.player_id
   AND s.season    = h.season
),
player_aggs AS (
  SELECT
    player_id,
    season,
    SUM(CASE WHEN rn_desc <= 5  THEN sog ELSE 0 END) AS num_sog_last5,
    SUM(CASE WHEN rn_desc <= 10 THEN sog ELSE 0 END) AS num_sog_last10,
    SUM(sog)                                         AS num_sog_szn_to_date,
    SUM(CASE WHEN rn_desc <= 5  THEN ev  ELSE 0 END) AS num_event_last5,
    SUM(CASE WHEN rn_desc <= 10 THEN ev  ELSE 0 END) AS num_event_last10,
    SUM(ev)                                          AS num_event_szn_to_date
  FROM hist_ranked
  GROUP BY 1,2
),
team_aggs AS (
  SELECT
    s.team_id,
    s.season,
    SUM(pa.num_sog_last10)   AS team_num_sog_last10,
    SUM(pa.num_event_last10) AS team_num_event_last10
  FROM slate s
  JOIN player_aggs pa
    ON pa.player_id = s.player_id
   AND pa.season    = s.season
  GROUP BY 1,2
)
UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
SET
  -- New column family (explicit num_sog_*)
  num_sog_last5          = pa.num_sog_last5,
  num_sog_last10         = pa.num_sog_last10,
  num_sog_szn_to_date    = pa.num_sog_szn_to_date,
  num_event_last5        = pa.num_event_last5,
  num_event_last10       = pa.num_event_last10,
  num_event_szn_to_date  = pa.num_event_szn_to_date,
  team_num_sog_last10    = ta.team_num_sog_last10,
  team_num_event_last10  = ta.team_num_event_last10,
  last10_team_sog_share  = CASE
                             WHEN ta.team_num_sog_last10 IS NULL OR ta.team_num_sog_last10 = 0 THEN NULL
                             ELSE pa.num_sog_last10 / ta.team_num_sog_last10
                           END,

  -- Legacy/export column family (what export_sog_denali_pregame.sql uses today)
  num_shotwasongoal_last5           = pa.num_sog_last5,
  num_shotwasongoal_last10          = pa.num_sog_last10,
  team_num_shotwasongoal_for_last10 = ta.team_num_sog_last10
FROM params p
JOIN slate s       ON TRUE
JOIN player_aggs pa ON pa.player_id = s.player_id AND pa.season = s.season
JOIN team_aggs ta   ON ta.team_id   = s.team_id   AND ta.season = s.season
WHERE
  t.game_date  = p.slate_date
  AND t.player_id::bigint = s.player_id
  AND t.season::int       = s.season;

COMMIT;

-- Optional quick check (run separately if you want it after commit):
-- SELECT
--   COUNT(*) AS rows_slate,
--   COUNT(num_sog_last10) AS nn_num_sog_last10,
--   COUNT(num_event_last10) AS nn_num_event_last10,
--   COUNT(team_num_sog_last10) AS nn_team_num_sog_last10,
--   COUNT(last10_team_sog_share) AS nn_share,
--   MIN(last10_team_sog_share) AS min_share,
--   MAX(last10_team_sog_share) AS max_share
-- FROM nhl.training_features_nhl_sog_enriched_pregame_v2
-- WHERE game_date = DATE :'slate_date';
