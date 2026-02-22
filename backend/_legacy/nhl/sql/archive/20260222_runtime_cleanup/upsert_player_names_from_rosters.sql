-- save as backend/nhl/sql/upsert_player_names_from_rosters.sql
\set ON_ERROR_STOP on
BEGIN;

CREATE TEMP TABLE _names_stg(
  nhl_id text,
  full_name text,
  team_abbr text
);

\copy _names_stg (nhl_id, full_name, team_abbr) FROM 'backend/nhl/data/external/roster_names.csv' CSV HEADER

-- Normalize to bigint where possible
WITH norm AS (
  SELECT NULLIF(nhl_id,'')::bigint AS nhl_id,
         NULLIF(btrim(full_name), '') AS full_name
  FROM _names_stg
  WHERE nhl_id ~ '^\d+$' AND full_name IS NOT NULL
)
UPDATE nhl.players p
SET full_name = n.full_name
FROM norm n
JOIN nhl.player_external_ids e
  ON e.provider = 'nhl'
 AND e.provider_player_id = n.nhl_id::text
WHERE p.player_id = e.player_id
  AND (p.full_name IS NULL OR p.full_name ~* '^(unknown|player)\s*\d+$');

COMMIT;
