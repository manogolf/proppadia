-- Read-only source export for the Points architecture bakeoff.
-- Official target: skater_points_raw.goals + assists. Regular season only.
-- History features and the three player-history arms are built locally by
-- build_nhl_points_architecture_bakeoff.py with strict-prior dates.
COPY (
  SELECT g.season::int AS season, g.game_date, g.start_time_utc,
    p.game_id, p.player_id, COALESCE(p.team_id, l.team_id) AS team_id,
    COALESCE(p.is_home, l.is_home)::int AS is_home,
    p.goals::int AS realized_goals, p.assists::int AS realized_assists,
    (p.goals + p.assists)::int AS realized_points,
    l.toi_minutes::float AS toi_minutes,
    l.pp_toi_minutes::float AS pp_toi_minutes,
    COALESCE(l.shots_on_goal, 0)::float AS shots_on_goal,
    COALESCE(l.shot_attempts, 0)::float AS shot_attempts
  FROM nhl.skater_points_raw p
  JOIN nhl.games g ON g.game_id = p.game_id
  LEFT JOIN nhl.skater_game_logs_raw l
    ON l.player_id = p.player_id AND l.game_id = p.game_id
  WHERE substring(g.game_id::text, 5, 2) = '02'
    AND p.goals IS NOT NULL AND p.assists IS NOT NULL
    AND g.season IN (2023, 2024)
  ORDER BY g.season, g.game_date, g.game_id, p.player_id
) TO STDOUT WITH CSV HEADER;
