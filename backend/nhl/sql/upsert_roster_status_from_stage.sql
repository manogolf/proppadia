-- Expects a TEMP table tmp_roster_stage
-- (game_date date, team_id bigint, player_id bigint, active_flag boolean, pp_unit text)
-- filled by the caller. This will upsert into nhl.roster_status, de-duping input rows.

WITH new_rows AS (
  SELECT DISTINCT ON (g.game_id, s.team_id, s.player_id)
         g.game_id,
         s.team_id,
         s.player_id,
         COALESCE(s.active_flag, TRUE) AS active_flag,
         NULL::text                    AS line_role,
         COALESCE(s.pp_unit, 'None')   AS pp_unit
  FROM tmp_roster_stage s
  JOIN nhl.games g
    ON g.game_date = s.game_date
   AND (g.home_team_id = s.team_id OR g.away_team_id = s.team_id)
  -- DISTINCT ON keeps the first row per key; ordering here is stable per key
  ORDER BY g.game_id, s.team_id, s.player_id
)
INSERT INTO nhl.roster_status (
  game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
)
SELECT
  n.game_id, n.team_id, n.player_id, n.active_flag, n.line_role, n.pp_unit, now()
FROM new_rows n
ON CONFLICT (game_id, team_id, player_id)
DO UPDATE
SET active_flag = EXCLUDED.active_flag,
    line_role   = COALESCE(EXCLUDED.line_role, nhl.roster_status.line_role),
    pp_unit     = EXCLUDED.pp_unit,
    asof_ts     = now();
