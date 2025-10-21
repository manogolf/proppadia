-- Upsert staged players into nhl.players.
-- - Normalizes position to F/D/G and shoots_catches to L/R
-- - Builds full_name from first/last (fallback: 'Player <id>')
-- - Never overwrite a real name with a placeholder
-- - Does not overwrite current_team_id with NULL

WITH norm AS (
  SELECT
    s.player_id::bigint                             AS player_id,
    s.team_id::bigint                               AS team_id,
    NULLIF(btrim(s.first_name), '')                 AS first_name,
    NULLIF(btrim(s.last_name),  '')                 AS last_name,

    -- Best-effort full name from stage
    NULLIF(
      btrim(
        COALESCE(
          NULLIF(btrim(concat_ws(' ', s.first_name, s.last_name)), ''),
          NULLIF(btrim(s.first_name), ''),
          NULLIF(btrim(s.last_name),  '')
        )
      ), ''
    )                                               AS full_name_raw,

    -- Normalize position to {F,D,G}
    CASE UPPER(COALESCE(s.position, ''))
      WHEN 'G' THEN 'G'
      WHEN 'GOALIE' THEN 'G'
      WHEN 'D' THEN 'D'
      WHEN 'LD' THEN 'D'
      WHEN 'RD' THEN 'D'
      WHEN 'DEF' THEN 'D'
      WHEN 'DEFENSE' THEN 'D'
      WHEN 'DEFENCE' THEN 'D'
      WHEN 'C' THEN 'F'
      WHEN 'LW' THEN 'F'
      WHEN 'RW' THEN 'F'
      WHEN 'F' THEN 'F'
      WHEN 'W' THEN 'F'
      WHEN 'CENTER' THEN 'F'
      WHEN 'LEFT WING' THEN 'F'
      WHEN 'RIGHT WING' THEN 'F'
      WHEN 'FORWARD' THEN 'F'
      ELSE 'F'
    END                                             AS position_norm,

    -- Normalize shoots_catches to {L,R}
    CASE UPPER(COALESCE(s.shoots_catches, ''))
      WHEN 'L' THEN 'L'
      WHEN 'R' THEN 'R'
      ELSE NULL
    END                                             AS shoots_catches_norm,

    COALESCE(s.active, TRUE)                        AS active
  FROM nhl.import_players_stage s
  WHERE s.player_id IS NOT NULL
),
prep AS (
  SELECT
    player_id,
    team_id,
    first_name,
    last_name,
    COALESCE(full_name_raw, 'Player ' || player_id::text) AS full_name_final,
    position_norm,
    shoots_catches_norm,
    active
  FROM norm
)
INSERT INTO nhl.players AS tgt (
  player_id, full_name, current_team_id, "position",
  shoots_catches, active, first_name, last_name, updated_at
)
SELECT
  p.player_id,
  p.full_name_final,
  p.team_id,
  p.position_norm,
  p.shoots_catches_norm,
  p.active,
  p.first_name,
  p.last_name,
  now()
FROM prep p
ON CONFLICT (player_id) DO UPDATE
SET
  -- Prefer real names over placeholders
  full_name = CASE
    WHEN tgt.full_name ~ '^[Pp]layer [0-9]+$' THEN EXCLUDED.full_name
    WHEN EXCLUDED.full_name ~ '^[Pp]layer [0-9]+$' THEN tgt.full_name
    ELSE COALESCE(EXCLUDED.full_name, tgt.full_name)
  END,
  current_team_id = COALESCE(EXCLUDED.current_team_id, tgt.current_team_id),
  "position"      = COALESCE(EXCLUDED."position",      tgt."position"),
  shoots_catches  = COALESCE(EXCLUDED.shoots_catches,  tgt.shoots_catches),
  active          = COALESCE(EXCLUDED.active,          tgt.active),
  first_name      = COALESCE(EXCLUDED.first_name,      tgt.first_name),
  last_name       = COALESCE(EXCLUDED.last_name,       tgt.last_name),
  -- keep status aligned with active (optional; remove if you prefer to manage status elsewhere)
  status          = CASE WHEN COALESCE(EXCLUDED.active, tgt.active) THEN 'active' ELSE 'inactive' END,
  updated_at      = now();
