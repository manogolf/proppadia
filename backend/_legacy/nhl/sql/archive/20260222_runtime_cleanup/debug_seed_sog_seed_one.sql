EXPLAIN (ANALYZE, BUFFERS)
WITH params AS (
  SELECT :'slate_date'::date AS d
),
g AS (
  SELECT game_id, home_team_id, away_team_id, game_date::date AS game_date
  FROM nhl.games
  WHERE game_date = (SELECT d FROM params)
),
r AS (
  SELECT DISTINCT r.player_id, r.team_id, r.game_id
  FROM nhl.roster_status r
  JOIN g USING (game_id)
),
enrich AS (
  SELECT
    r.player_id,
    r.game_id,
    r.team_id,
    CASE WHEN r.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END AS opponent_id,
    (r.team_id = g.home_team_id) AS is_home,
    g.game_date
  FROM r
  JOIN g USING (game_id)
),
skaters AS (
  SELECT DISTINCT e.*
  FROM enrich e
  JOIN nhl.players p ON p.player_id = e.player_id
  WHERE COALESCE(p.position,'') <> 'G'
),
logs AS (
  SELECT
    l.player_id,
    l.game_id,
    l.game_date::date AS game_date,
    COALESCE(l.shots_on_goal,  0)::int     AS shots_on_goal,
    COALESCE(l.shot_attempts,  0)::int     AS shot_attempts,
    COALESCE(l.toi_minutes,    0)::numeric AS toi_minutes,
    COALESCE(l.pp_toi_minutes, 0)::numeric AS pp_toi_minutes
  FROM nhl.skater_game_logs_raw l
  JOIN skaters s ON s.player_id = l.player_id
  JOIN nhl.games gg ON gg.game_id = l.game_id
  WHERE l.game_date < (SELECT d FROM params)
    AND substring(gg.game_id::text, 5, 2) = '02'
),
logs_roll AS (
  SELECT
    player_id,
    game_id,
    game_date,
    shots_on_goal,
    shot_attempts,
    toi_minutes,
    pp_toi_minutes,

    CASE WHEN toi_minutes > 0
         THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
         ELSE NULL END AS sog_per60,
    CASE WHEN toi_minutes > 0
         THEN (shot_attempts::double precision / toi_minutes::double precision) * 60.0
         ELSE NULL END AS attempts_per60,

    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS d5_sog_per60,

    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS d10_sog_per60,

    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shots_on_goal::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    ) AS d20_sog_per60,

    AVG(
      CASE WHEN toi_minutes > 0
           THEN (shot_attempts::double precision / toi_minutes::double precision) * 60.0
      END
    ) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS attempts_d10_per60,

    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS num_sog_last5,

    SUM(shots_on_goal) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS num_sog_last10,

    COUNT(*) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS num_event_last5,

    COUNT(*) OVER (
      PARTITION BY player_id
      ORDER BY game_date, game_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS num_event_last10
  FROM logs
),
season_key AS (
  SELECT
    lr.player_id,
    lr.game_id,
    lr.game_date,
    CASE
      WHEN EXTRACT(MONTH FROM lr.game_date) >= 7
        THEN EXTRACT(YEAR FROM lr.game_date) + 1
      ELSE EXTRACT(YEAR FROM lr.game_date)
    END AS nhl_season_key,
    lr.shots_on_goal,
    lr.shot_attempts
  FROM logs_roll lr
),
season_agg AS (
  SELECT
    player_id,
    nhl_season_key,
    SUM(shots_on_goal) AS num_sog_szn_to_date,
    COUNT(*)           AS num_event_szn_to_date
  FROM season_key
  GROUP BY player_id, nhl_season_key
),
log_snap AS (
  SELECT DISTINCT ON (lr.player_id)
    lr.player_id,
    lr.game_date AS prev_game_date,
    lr.d5_sog_per60,
    lr.d10_sog_per60,
    lr.d20_sog_per60,
    lr.attempts_d10_per60,
    lr.num_sog_last5,
    lr.num_sog_last10,
    lr.num_event_last5,
    lr.num_event_last10,
    sa.num_sog_szn_to_date,
    sa.num_event_szn_to_date
  FROM logs_roll lr
  LEFT JOIN season_key sk
    ON sk.player_id = lr.player_id
   AND sk.game_id   = lr.game_id
  LEFT JOIN season_agg sa
    ON sa.player_id      = sk.player_id
   AND sa.nhl_season_key = sk.nhl_season_key
  ORDER BY lr.player_id, lr.game_date DESC, lr.game_id DESC
),
last_feat AS (
  SELECT DISTINCT ON (t.player_id)
    t.player_id,
    t.d5_sog_per60,
    t.d10_sog_per60,
    t.d20_sog_per60,
    t.attempts_d10_per60,

    t.rest_days,
    t.b2b_flag,

    t.pace_index,
    t.pace_matchup_index,

    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game,
    t.opp_d10_sf_per60,
    t.team_d10_sa_per60,
    t.opp_d10_sa_per60,

    t.role_pp_share,

    t.szn_toi_per_game_5on5,
    t.szn_toi_per_game_pp,
    t.szn_toi_per_game_pk,
    t.szn_shifts_per_game_5on5,
    t.szn_shifts_per_game_pp,
    t.szn_shifts_per_game_pk,

    t.season_5on5_icetime_per_game,
    t.season_5on4_icetime_per_game,
    t.season_4on5_icetime_per_game,
    t.season_5on5_shifts_per_game,
    t.season_5on4_shifts_per_game,
    t.season_4on5_shifts_per_game,

    t.team_szn_5on5_top_line_xgf_share,
    t.team_5v5_top_line_icetime_share,
    t.team_5v5_top_line_shotattempts_share,

    t.last10_team_sog_share,
    t.team_num_sog_last10,
    t.team_num_event_last10,

    t.num_sog_last5,
    t.num_sog_last10,
    t.num_sog_szn_to_date,

    t.num_event_last5,
    t.num_event_last10,
    t.num_event_szn_to_date,

    t.hot_last5_flag,

    t.game_date AS prev_game_date
  FROM nhl.training_features_sog_denali t
  JOIN skaters s ON s.player_id = t.player_id
  WHERE t.game_date < (SELECT d FROM params)
  ORDER BY t.player_id, t.game_date DESC, t.game_id DESC
),
team_roll AS (
  SELECT DISTINCT ON (t.team_id)
    t.team_id,
    t.team_d10_sf_per_game,
    t.opp_d10_sf_allowed_per_game
  FROM nhl.tf_team_roll10 t
  ORDER BY t.team_id, t.game_id DESC
),
seed AS (
  SELECT
    s.player_id,
    s.game_id,
    s.team_id,
    s.opponent_id,
    s.is_home,
    (SELECT d FROM params) AS game_date,

    NULL::numeric AS shots_on_goal,

    COALESCE(ls.d5_sog_per60,  lf.d5_sog_per60)  AS d5_sog_per60,
    COALESCE(ls.d10_sog_per60, lf.d10_sog_per60) AS d10_sog_per60,
    COALESCE(ls.d20_sog_per60, lf.d20_sog_per60) AS d20_sog_per60,
    COALESCE(ls.attempts_d10_per60, lf.attempts_d10_per60) AS attempts_d10_per60,

    CASE
      WHEN ls.prev_game_date IS NOT NULL
        THEN GREATEST(0, ((SELECT d FROM params) - ls.prev_game_date))::int
      WHEN lf.prev_game_date IS NOT NULL
        THEN GREATEST(0, ((SELECT d FROM params) - lf.prev_game_date))::int
      ELSE NULL
    END AS rest_days,
    CASE
      WHEN ls.prev_game_date IS NOT NULL
        THEN ((SELECT d FROM params) - ls.prev_game_date = 1)
      WHEN lf.prev_game_date IS NOT NULL
        THEN ((SELECT d FROM params) - lf.prev_game_date = 1)
      ELSE NULL
    END AS b2b_flag,

    lf.pace_index,
    lf.pace_matchup_index,

    COALESCE(tr.team_d10_sf_per_game,        lf.team_d10_sf_per_game)        AS team_d10_sf_per_game,
    COALESCE(tr.opp_d10_sf_allowed_per_game, lf.opp_d10_sf_allowed_per_game) AS opp_d10_sf_allowed_per_game,
    lf.opp_d10_sf_per60,
    lf.team_d10_sa_per60,
    lf.opp_d10_sa_per60,

    lf.role_pp_share,

    lf.szn_toi_per_game_5on5,
    lf.szn_toi_per_game_pp,
    lf.szn_toi_per_game_pk,
    lf.szn_shifts_per_game_5on5,
    lf.szn_shifts_per_game_pp,
    lf.szn_shifts_per_game_pk,

    lf.season_5on5_icetime_per_game,
    lf.season_5on4_icetime_per_game,
    lf.season_4on5_icetime_per_game,
    lf.season_5on5_shifts_per_game,
    lf.season_5on4_shifts_per_game,
    lf.season_4on5_shifts_per_game,

    lf.team_szn_5on5_top_line_xgf_share,
    lf.team_5v5_top_line_icetime_share,
    lf.team_5v5_top_line_shotattempts_share,

    lf.last10_team_sog_share,
    lf.team_num_sog_last10,
    lf.team_num_event_last10,

    COALESCE(ls.num_sog_last5,  lf.num_sog_last5)  AS num_sog_last5,
    COALESCE(ls.num_sog_last10, lf.num_sog_last10) AS num_sog_last10,
    COALESCE(ls.num_sog_szn_to_date, lf.num_sog_szn_to_date) AS num_sog_szn_to_date,

    COALESCE(ls.num_event_last5,  lf.num_event_last5)  AS num_event_last5,
    COALESCE(ls.num_event_last10, lf.num_event_last10) AS num_event_last10,
    COALESCE(ls.num_event_szn_to_date, lf.num_event_szn_to_date) AS num_event_szn_to_date,

    lf.hot_last5_flag
  FROM skaters s
  LEFT JOIN last_feat lf ON lf.player_id = s.player_id
  LEFT JOIN team_roll tr ON tr.team_id = s.team_id
  LEFT JOIN log_snap ls ON ls.player_id = s.player_id
),
seed_one AS (
  SELECT *
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY player_id, game_id ORDER BY player_id, game_id) AS rn
    FROM seed s
  ) x
  WHERE rn = 1
)
SELECT COUNT(*) FROM seed_one;
