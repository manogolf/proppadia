-- Eligibility guard (Option A):
-- Hard-exclude players with insufficient prior SHIFTCHARTS history, since
-- shift-derived features (season TOI, pairings, overlap) are low-signal / unstable
-- with very small samples.
-- min_shift_games=3 excludes only extreme thin-history tail (0–2 prior games).


\set ON_ERROR_STOP on

COPY (
WITH
params AS (
  SELECT 3::int AS min_shift_games
),
slate_players AS (
  SELECT DISTINCT
    v.player_id,
    v.game_date
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 v
  WHERE v.game_date = :'slate_date'::date
),
eligible AS (
  SELECT
    sp.player_id
  FROM slate_players sp
  JOIN params p ON TRUE
  LEFT JOIN (
    SELECT DISTINCT
      sh.player_id,
      g.game_id,
      g.game_date::date AS game_date
    FROM nhl.shiftcharts_shifts sh
    JOIN nhl.games g
      ON g.game_id = sh.game_id
  ) pg
    ON pg.player_id = sp.player_id
   AND pg.game_date < sp.game_date::date
  GROUP BY 1, p.min_shift_games
  HAVING COUNT(DISTINCT pg.game_id) >= p.min_shift_games
),

base AS (
  SELECT
    v.player_id,
    v.game_id,
    v.team_id,
    v.opponent_id,
    v.is_home,
    v.game_date,
    v.season,
    v.shots_on_goal,
    v.team_d10_sf_per_game,
    v.opp_d10_sf_allowed_per_game,
    v.pace_matchup_index,
    v.rest_days,
    v.b2b_flag,
    role_pp_share,

    -- Pairings availability / coverage
    v.d10_pairings_available,
    v.d20_pairings_available,
    v.d10_pairings_cov_bucket,
    v.d20_pairings_cov_bucket,
    v.d20_top_mate_repeat_rate,

    -- Pairings overlap
    v.d10_top_mate_overlap_share_avg,
    v.d10_top_mate_overlap_share_std,
    v.d10_top3_mates_overlap_share_avg,
    v.d10_top3_mates_overlap_share_std,
    v.d10_shiftcharts_games,
    v.d10_shiftcharts_coverage_rate,

    v.d20_top_mate_overlap_share_avg,
    v.d20_top_mate_overlap_share_std,
    v.d20_top3_mates_overlap_share_avg,
    v.d20_top3_mates_overlap_share_std,
    v.d20_shiftcharts_games,
    v.d20_shiftcharts_coverage_rate,

    -- Season TOI features
    v.szn_toi_per_game_5on5,
    v.szn_toi_per_game_pp,
    v.szn_toi_per_game_pk,
    v.szn_shifts_per_game_5on5,
    v.szn_shifts_per_game_pp,
    v.szn_shifts_per_game_pk,

    v.season_5on5_icetime_per_game,
    v.season_5on5_shifts_per_game,
    v.season_5on4_icetime_per_game,
    v.season_4on5_icetime_per_game,
    v.d5_toi_min_avg,
    v.d10_toi_min_avg,
    v.d20_toi_min_avg,


    v.last10_team_sog_share,
    v.hot_last5_flag,
    v.num_shotwasongoal_last5,
    v.num_shotwasongoal_last10,
    v.num_shotwasongoal_season_to_date,
    v.num_event_shot_last5,
    v.num_event_shot_last10,
    v.num_event_shot_season_to_date,
    v.team_num_shotwasongoal_for_last10,
    v.team_num_event_shot_for_last10
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 v
  LEFT JOIN eligible e
    ON e.player_id = v.player_id

  WHERE v.game_date = :'slate_date'::date
)

SELECT
  -- IDs / context
  b.player_id,
  b.game_id,
  b.team_id,
  b.opponent_id,
  b.is_home,
  b.game_date,
  b.season,

  -- target (realized SOG; may be NULL pregame)
  b.shots_on_goal,

  -- core rolling SOG features (computed from prior logs; anchored to slate_date)
  calc.d5_sog_per60,
  calc.d10_sog_per60,
  calc.d20_sog_per60,
  calc.attempts_d10_per60,
  b.d5_toi_min_avg,
  b.d10_toi_min_avg,
  b.d20_toi_min_avg,

  -- team shot / allowed context
  b.team_d10_sf_per_game,
  b.opp_d10_sf_allowed_per_game,

  -- pace features
  b.pace_matchup_index,
  b.pace_matchup_index AS pace_index,

  -- rest / usage
  b.rest_days,
  b.b2b_flag,
  b.role_pp_share,

  -- shift teammate overlap (D10, prior-games only; NULL when missing)
  b.d10_top_mate_overlap_share_avg,
  b.d10_top_mate_overlap_share_std,
  b.d10_top3_mates_overlap_share_avg,
  b.d10_top3_mates_overlap_share_std,
  b.d10_shiftcharts_games,
  b.d10_shiftcharts_coverage_rate,

  -- shift teammate overlap (D20, prior-games only; NULL when missing)
  b.d20_top_mate_overlap_share_avg,
  b.d20_top_mate_overlap_share_std,
  b.d20_top3_mates_overlap_share_avg,
  b.d20_top3_mates_overlap_share_std,
  b.d20_shiftcharts_games,
  b.d20_shiftcharts_coverage_rate,

  -- pairings availability / buckets (required for pairings_v1 model)
  b.d10_pairings_available,
  b.d20_pairings_available,
  b.d10_pairings_cov_bucket,
  b.d20_pairings_cov_bucket,
  b.d20_top_mate_repeat_rate,

  -- Denali extras (team context)
  tc.opp_d10_sf_per60 AS opp_d10_sf_per60,
  tc.d10_sa_per60     AS team_d10_sa_per60,
  tc.opp_d10_sa_per60 AS opp_d10_sa_per60,

  -- season TOI / shifts
  b.szn_toi_per_game_5on5,
  b.szn_toi_per_game_pp,
  b.szn_toi_per_game_pk,
  b.szn_shifts_per_game_5on5,
  b.szn_shifts_per_game_pp,
  b.szn_shifts_per_game_pk,

  b.season_5on5_icetime_per_game,
  b.season_5on4_icetime_per_game,
  b.season_4on5_icetime_per_game,
  b.season_5on5_shifts_per_game,
  NULL::numeric AS season_5on4_shifts_per_game,
  NULL::numeric AS season_4on5_shifts_per_game,

  NULL::numeric AS team_szn_5on5_top_line_xgf_share,
  NULL::numeric AS team_5v5_top_line_icetime_share,
  NULL::numeric AS team_5v5_top_line_shotattempts_share,

  -- team + streak context
  b.last10_team_sog_share,
  b.hot_last5_flag,

  -- alias shotwasongoal → Denali SOG names
  b.num_shotwasongoal_last5          AS num_sog_last5,
  b.num_shotwasongoal_last10         AS num_sog_last10,
  b.num_shotwasongoal_season_to_date AS num_sog_szn_to_date,

  -- alias event-shot counts → Denali event names
  b.num_event_shot_last5          AS num_event_last5,
  b.num_event_shot_last10         AS num_event_last10,
  b.num_event_shot_season_to_date AS num_event_szn_to_date,

  -- team-level counts
  b.team_num_shotwasongoal_for_last10 AS team_num_sog_last10,
  b.team_num_event_shot_for_last10    AS team_num_event_last10

FROM base b
LEFT JOIN nhl.team_context_rolling tc
  ON tc.game_id = b.game_id
 AND tc.team_id = b.team_id
CROSS JOIN LATERAL (
  WITH last20 AS (
    SELECT
      COALESCE(NULLIF(BTRIM(l.shots_on_goal::text), ''), '0')::numeric AS sog,
      COALESCE(NULLIF(BTRIM(l.shot_attempts::text), ''), '0')::numeric AS att,
      NULLIF(COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric, 0) AS toi_min,
      ROW_NUMBER() OVER (ORDER BY g.game_date DESC, g.game_id DESC) AS rn
    FROM nhl.skater_game_logs_raw l
    JOIN nhl.games g USING (game_id)
    WHERE l.player_id::bigint = b.player_id::bigint
      AND g.game_date::date < (:'slate_date')::date
    ORDER BY g.game_date DESC, g.game_id DESC
    LIMIT 20
  ),
  sums AS (
    SELECT
      SUM(sog)     FILTER (WHERE rn <= 5)  AS sog_5,
      SUM(toi_min) FILTER (WHERE rn <= 5)  AS toi_5,
      SUM(sog)     FILTER (WHERE rn <= 10) AS sog_10,
      SUM(att)     FILTER (WHERE rn <= 10) AS att_10,
      SUM(toi_min) FILTER (WHERE rn <= 10) AS toi_10,
      SUM(sog)     FILTER (WHERE rn <= 20) AS sog_20,
      SUM(toi_min) FILTER (WHERE rn <= 20) AS toi_20
    FROM last20
  )
  SELECT
    CASE WHEN sums.toi_5  IS NULL OR sums.toi_5  <= 0 THEN NULL ELSE (sums.sog_5  / sums.toi_5 ) * 60 END AS d5_sog_per60,
    CASE WHEN sums.toi_10 IS NULL OR sums.toi_10 <= 0 THEN NULL ELSE (sums.sog_10 / sums.toi_10) * 60 END AS d10_sog_per60,
    CASE WHEN sums.toi_20 IS NULL OR sums.toi_20 <= 0 THEN NULL ELSE (sums.sog_20 / sums.toi_20) * 60 END AS d20_sog_per60,
    CASE WHEN sums.toi_10 IS NULL OR sums.toi_10 <= 0 THEN NULL ELSE (sums.att_10 / sums.toi_10) * 60 END AS attempts_d10_per60
  FROM sums
) AS calc
ORDER BY b.game_date, b.game_id, b.player_id
) TO STDOUT WITH (FORMAT csv, HEADER true);
