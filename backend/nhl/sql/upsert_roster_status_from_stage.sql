-- Ensure a stable UPSERT target
CREATE UNIQUE INDEX IF NOT EXISTS roster_status_uk
  ON nhl.roster_status (game_id, team_id, player_id);

-- Temp stage (session-scoped); caller will fill it
CREATE TEMP TABLE IF NOT EXISTS tmp_roster_stage (
  game_date date,
  team_id   bigint,
  player_id bigint,
  active_flag boolean,
  pp_unit text
) ON COMMIT DROP;

-- Merge stage -> roster_status
INSERT INTO nhl.roster_status AS tgt (
  game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
)
SELECT
  g.game_id,
  s.team_id,
  s.player_id,
  COALESCE(s.active_flag, true),
  NULL::text AS line_role,
  COALESCE(s.pp_unit, 'None'),
  now()
FROM tmp_roster_stage s
JOIN nhl.games g
  ON g.game_date = s.game_date
 AND (g.home_team_id = s.team_id OR g.away_team_id = s.team_id)
ON CONFLICT (game_id, team_id, player_id) DO UPDATE
SET active_flag = EXCLUDED.active_flag,
    pp_unit     = EXCLUDED.pp_unit,
    asof_ts     = now();
