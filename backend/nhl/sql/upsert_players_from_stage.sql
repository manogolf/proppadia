-- Merge staged players into nhl.players
INSERT INTO nhl.players AS tgt (
  player_id, full_name, current_team_id, "position", shoots_catches, active,
  first_name, last_name, updated_at
)
SELECT
  s.player_id,
  NULLIF(trim(both ' ' FROM concat_ws(' ', s.first_name, s.last_name)), '') AS full_name,
  s.team_id, s."position", s.shoots_catches, COALESCE(s.active, true),
  s.first_name, s.last_name, now()
FROM nhl.import_players_stage s
WHERE s.player_id IS NOT NULL
ON CONFLICT (player_id) DO UPDATE
SET full_name       = COALESCE(EXCLUDED.full_name, tgt.full_name),
    current_team_id = EXCLUDED.current_team_id,
    "position"      = COALESCE(EXCLUDED."position", tgt."position"),
    shoots_catches  = COALESCE(EXCLUDED.shoots_catches, tgt.shoots_catches),
    active          = COALESCE(EXCLUDED.active, tgt.active),
    first_name      = COALESCE(EXCLUDED.first_name, tgt.first_name),
    last_name       = COALESCE(EXCLUDED.last_name, tgt.last_name),
    updated_at      = now();
