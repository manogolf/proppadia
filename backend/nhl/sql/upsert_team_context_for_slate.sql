-- backend/nhl/sql/upsert_team_context_for_slate.sql
-- Computes team rolling context for a slate using CURRENT raw skater logs + games.
-- Terminal usage:
--   psql "$DB_URL" --no-psqlrc -v ON_ERROR_STOP=1 -v slate_date=YYYY-MM-DD \
--     -f backend/nhl/sql/upsert_team_context_for_slate.sql

\set ON_ERROR_STOP on

BEGIN;

WITH
params AS (
  SELECT (:'slate_date')::date AS slate_date
),

-- Slate games and team/opponent pairs
slate_games AS (
  SELECT
    g.game_id::bigint      AS game_id,
    g.game_date::date      AS game_date,
    g.home_team_id::bigint AS home_team_id,
    g.away_team_id::bigint AS away_team_id
  FROM nhl.games g
  JOIN params p ON g.game_date::date = p.slate_date
),
slate_teams AS (
  SELECT game_id, game_date, home_team_id AS team_id, away_team_id AS opponent_id
  FROM slate_games
  UNION ALL
  SELECT game_id, game_date, away_team_id AS team_id, home_team_id AS opponent_id
  FROM slate_games
),

-- Build team-per-game SF/SA per60 from skater logs:
--   SF = sum(shot_attempts) for that team's skaters in the game
--   TOI = sum(toi_minutes) for that team's skaters in the game
--   SF/60 = SF / TOI * 60
--
-- Then compute SA/60 by joining the opponent team’s SF/60 in the same game.
team_game AS (
  SELECT
    l.team_id::bigint AS team_id,
    g.game_id::bigint AS game_id,
    g.game_date::date AS game_date,

    SUM(COALESCE(NULLIF(BTRIM(l.shot_attempts::text), ''), '0')::numeric) AS sf_att,
    SUM(NULLIF(COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric, 0)) AS toi_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  JOIN params p ON TRUE
  -- only use games strictly before slate_date (pregame context)
  WHERE g.game_date::date < p.slate_date
  GROUP BY 1,2,3
),
team_game_per60 AS (
  SELECT
    tg.team_id,
    tg.game_id,
    tg.game_date,
    CASE WHEN tg.toi_min IS NULL OR tg.toi_min <= 0 THEN NULL
         ELSE (tg.sf_att / tg.toi_min) * 60
    END AS sf_per60
  FROM team_game tg
),
team_game_with_sa AS (
  SELECT
    a.team_id,
    a.game_id,
    a.game_date,
    a.sf_per60,
    b.sf_per60 AS sa_per60
  FROM team_game_per60 a
  JOIN nhl.games g ON g.game_id = a.game_id
  JOIN team_game_per60 b
    ON b.game_id = a.game_id
   AND b.team_id = CASE
     WHEN a.team_id = g.home_team_id THEN g.away_team_id
     WHEN a.team_id = g.away_team_id THEN g.home_team_id
     ELSE NULL
   END
),

-- Rolling 10-game averages per team (using per-game SF/60 & SA/60)
roll10 AS (
  SELECT
    t.team_id,
    t.game_id,
    t.game_date,
    AVG(t.sf_per60) OVER w10 AS d10_sf_per60,
    AVG(t.sa_per60) OVER w10 AS d10_sa_per60
  FROM team_game_with_sa t
  WINDOW w10 AS (
    PARTITION BY t.team_id
    ORDER BY t.game_date, t.game_id
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  )
),

-- For each team, grab the latest roll10 row BEFORE the slate date
latest_before_slate AS (
  SELECT DISTINCT ON (r.team_id)
    r.team_id,
    r.game_date,
    r.d10_sf_per60,
    r.d10_sa_per60
  FROM roll10 r
  ORDER BY r.team_id, r.game_date DESC, r.game_id DESC
),

-- League baseline for pace index (average of teams' latest d10_sf_per60)
league_avg AS (
  SELECT
    AVG(d10_sf_per60)::numeric AS league_d10_sf_per60
  FROM latest_before_slate
  WHERE d10_sf_per60 IS NOT NULL
),

-- Join slate teams to their own and opponent rolling context
joined AS (
  SELECT
    st.team_id,
    st.game_id,
    st.opponent_id,

    me.d10_sf_per60::numeric(6,3)  AS d10_sf_per60,
    me.d10_sa_per60::numeric(6,3)  AS d10_sa_per60,

    opp.d10_sf_per60::numeric(6,3) AS opp_d10_sf_per60,
    opp.d10_sa_per60::numeric(6,3) AS opp_d10_sa_per60,

    CASE
      WHEN la.league_d10_sf_per60 IS NULL OR la.league_d10_sf_per60 <= 0 THEN NULL
      ELSE (
        (COALESCE(me.d10_sf_per60, 0) + COALESCE(opp.d10_sa_per60, 0)) / 2.0
      ) / la.league_d10_sf_per60
    END::numeric(6,3) AS pace_matchup_index

  FROM slate_teams st
  LEFT JOIN latest_before_slate me  ON me.team_id  = st.team_id
  LEFT JOIN latest_before_slate opp ON opp.team_id = st.opponent_id
  CROSS JOIN league_avg la
)

INSERT INTO nhl.team_context_rolling (
  team_id,
  game_id,
  d10_sf_per60,
  d10_sa_per60,
  opp_d10_sf_per60,
  opp_d10_sa_per60,
  pace_matchup_index
)
SELECT
  team_id,
  game_id,
  d10_sf_per60,
  d10_sa_per60,
  opp_d10_sf_per60,
  opp_d10_sa_per60,
  pace_matchup_index
FROM joined
ON CONFLICT (team_id, game_id) DO UPDATE
SET
  d10_sf_per60       = EXCLUDED.d10_sf_per60,
  d10_sa_per60       = EXCLUDED.d10_sa_per60,
  opp_d10_sf_per60   = EXCLUDED.opp_d10_sf_per60,
  opp_d10_sa_per60   = EXCLUDED.opp_d10_sa_per60,
  pace_matchup_index = EXCLUDED.pace_matchup_index;

COMMIT;
