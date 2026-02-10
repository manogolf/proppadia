\set QUIET on
\set ON_ERROR_STOP on
\pset pager off
\pset tuples_only on

-- Expect: psql -v slate_date=YYYY-MM-DD -f _export_names.sql
-- Outputs a CSV of players on the slate with names and team codes.

COPY (
  WITH params AS (
    SELECT :'slate_date'::date AS d
  ),
  g AS (
    SELECT
      game_id,
      game_date::date AS game_date,
      home_team_id,
      away_team_id
    FROM nhl.games
    WHERE game_date::date = (SELECT d FROM params)
  ),
  r AS (
    SELECT DISTINCT
      r.player_id,
      r.team_id,
      r.game_id
    FROM nhl.roster_status r
    JOIN g USING (game_id)
  )
  SELECT
    r.player_id,

    -- Existing display name (kept for backwards compatibility)
    COALESCE(NULLIF(btrim(p.full_name), ''), 'Player ' || r.player_id::text) AS full_name,

    -- New: canonical components
    NULLIF(btrim(p.first_name), '') AS first_name,
    NULLIF(btrim(p.last_name),  '') AS last_name,

    -- New: canonical full name (First Last) when available
    CASE
      WHEN NULLIF(btrim(p.first_name), '') IS NOT NULL
       AND NULLIF(btrim(p.last_name),  '') IS NOT NULL
      THEN btrim(p.first_name) || ' ' || btrim(p.last_name)
      ELSE NULL
    END AS full_name_canonical,

    r.team_id,
    COALESCE(t.team, '') AS team_code,
    r.game_id,
    g.game_date
  FROM r
  LEFT JOIN nhl.players p ON p.player_id = r.player_id
  LEFT JOIN nhl.teams   t ON t.team_id   = r.team_id
  JOIN g USING (game_id)
  ORDER BY team_code, full_name, r.player_id
) TO STDOUT WITH CSV HEADER;
