WITH norm AS (
  SELECT
    s.player_id::bigint AS player_id,
    s.team_id::bigint   AS team_id,

    -- Sanitize names coming from stage:
    -- - first_name of 'Player' or 'Unknown' → NULL
    -- - last_name that is purely digits → NULL
    CASE
      WHEN s.first_name ~* '^(player|unknown)$' THEN NULL
      ELSE NULLIF(btrim(s.first_name), '')
    END AS first_name,
    CASE
      WHEN s.last_name ~ '^[0-9]+$' THEN NULL
      ELSE NULLIF(btrim(s.last_name), '')
    END AS last_name,

    -- Normalize position to {F,D,G}
    CASE UPPER(COALESCE(s.position, ''))
      WHEN 'G' THEN 'G'
      WHEN 'GOALIE' THEN 'G'
      WHEN 'D' THEN 'D'  WHEN 'LD' THEN 'D'  WHEN 'RD' THEN 'D'
      WHEN 'DEF' THEN 'D'  WHEN 'DEFENSE' THEN 'D'  WHEN 'DEFENCE' THEN 'D'
      WHEN 'C' THEN 'F'  WHEN 'LW' THEN 'F'  WHEN 'RW' THEN 'F'
      WHEN 'F' THEN 'F'  WHEN 'W' THEN 'F'  WHEN 'CENTER' THEN 'F'
      WHEN 'LEFT WING' THEN 'F'  WHEN 'RIGHT WING' THEN 'F'  WHEN 'FORWARD' THEN 'F'
      ELSE 'F'
    END AS position_norm,

    -- Normalize shoots_catches to {L,R}
    CASE UPPER(COALESCE(s.shoots_catches, ''))
      WHEN 'L' THEN 'L'
      WHEN 'R' THEN 'R'
      ELSE NULL
    END AS shoots_catches_norm,

    COALESCE(s.active, TRUE) AS active
  FROM nhl.import_players_stage s
  WHERE s.player_id IS NOT NULL
),
prep AS (
  SELECT
    player_id,
    team_id,
    first_name,
    last_name,

    -- Build a real full name (reject placeholder-shaped results)
    CASE
      WHEN btrim(concat_ws(' ', first_name, last_name)) ~* '^(player|unknown)\s+\d+$'
        THEN NULL
      ELSE NULLIF(btrim(concat_ws(' ', first_name, last_name)), '')
    END AS full_name_final,

    position_norm,
    shoots_catches_norm,
    active
  FROM norm
),
src AS (
  -- only rows with a real name survive
  SELECT *
  FROM prep
  WHERE full_name_final IS NOT NULL AND btrim(full_name_final) <> ''
)
INSERT INTO nhl.players AS tgt (
  player_id, full_name, current_team_id, "position",
  shoots_catches, active, first_name, last_name, updated_at
)
SELECT
  s.player_id,
  s.full_name_final,
  s.team_id,
  s.position_norm,
  s.shoots_catches_norm,
  s.active,
  s.first_name,
  s.last_name,
  now()
FROM src s
ON CONFLICT (player_id) DO UPDATE
SET
  -- Prefer real names over placeholders/blank
  full_name = CASE
    WHEN tgt.full_name ~* '^(player|unknown)\s+[0-9]+$' OR btrim(tgt.full_name) = ''
      THEN EXCLUDED.full_name
    ELSE tgt.full_name
  END,
  current_team_id = COALESCE(EXCLUDED.current_team_id, tgt.current_team_id),
  "position"      = COALESCE(EXCLUDED."position",      tgt."position"),
  shoots_catches  = COALESCE(EXCLUDED.shoots_catches,  tgt.shoots_catches),
  active          = COALESCE(EXCLUDED.active,          tgt.active),
  first_name      = COALESCE(EXCLUDED.first_name,      tgt.first_name),
  last_name       = COALESCE(EXCLUDED.last_name,       tgt.last_name),
  updated_at      = now();
