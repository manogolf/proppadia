-- PREPARED ONLY. Do not apply before the activation preflight in the V1 runbook.
-- Canonical game tables retain authoritative source type once; downstream
-- lanes join mlb.canonical_game_phase_v1 by exact gamePk/game_id.
--
-- TRANSACTION-NEUTRAL BODY: the governed activation loader executes this file
-- inside the same transaction as the exact-gamePk backfill. Manual execution
-- must use psql --single-transaction --set ON_ERROR_STOP=1. Never execute this
-- file without an enclosing transaction.

ALTER TABLE mlb.game_info
  ADD COLUMN IF NOT EXISTS source_season integer,
  ADD COLUMN IF NOT EXISTS source_game_type text,
  ADD COLUMN IF NOT EXISTS season_phase text,
  ADD COLUMN IF NOT EXISTS postseason_round text,
  ADD COLUMN IF NOT EXISTS season_name text,
  ADD COLUMN IF NOT EXISTS source_round text,
  ADD COLUMN IF NOT EXISTS schedule_relationships jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS game_type_source_sha256 text;

ALTER TABLE mlb_cleanroom_v1.games
  ADD COLUMN IF NOT EXISTS source_season integer,
  ADD COLUMN IF NOT EXISTS source_game_type text,
  ADD COLUMN IF NOT EXISTS season_phase text,
  ADD COLUMN IF NOT EXISTS postseason_round text,
  ADD COLUMN IF NOT EXISTS season_name text,
  ADD COLUMN IF NOT EXISTS source_round text,
  ADD COLUMN IF NOT EXISTS schedule_relationships jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE mlb.game_info
  DROP CONSTRAINT IF EXISTS game_info_season_phase_v1,
  DROP CONSTRAINT IF EXISTS game_info_phase_round_v1,
  DROP CONSTRAINT IF EXISTS game_info_phase_contract_v1,
  ADD CONSTRAINT game_info_phase_contract_v1 CHECK (
    (
      source_game_type IS NULL
      AND source_season IS NULL
      AND season_phase IS NULL
      AND postseason_round IS NULL
      AND season_name IS NULL
      AND source_round IS NULL
      AND game_type_source_sha256 IS NULL
    )
    OR (
      source_game_type IN ('S','E','I','R','F','D','L','W','P','C','A','N')
      AND source_season IS NOT NULL
      AND source_season > 0
      AND game_type_source_sha256 IS NOT NULL
      AND (
        (source_game_type IN ('S','E','I') AND season_phase = 'PRESEASON' AND postseason_round IS NULL)
        OR (source_game_type = 'R' AND season_phase = 'REGULAR_SEASON' AND postseason_round IS NULL)
        OR (source_game_type = 'F' AND season_phase = 'POSTSEASON' AND postseason_round = 'WILD_CARD')
        OR (source_game_type = 'D' AND season_phase = 'POSTSEASON' AND postseason_round = 'DIVISION_SERIES')
        OR (source_game_type = 'L' AND season_phase = 'POSTSEASON' AND postseason_round = 'LEAGUE_CHAMPIONSHIP_SERIES')
        OR (source_game_type = 'W' AND season_phase = 'POSTSEASON' AND postseason_round = 'WORLD_SERIES')
        OR (source_game_type = 'P' AND season_phase = 'POSTSEASON' AND postseason_round = 'PLAYOFFS_UNSPECIFIED')
        OR (source_game_type = 'C' AND season_phase = 'POSTSEASON' AND postseason_round = 'CHAMPIONSHIP_UNSPECIFIED')
        OR (source_game_type IN ('A','N') AND season_phase IS NULL AND postseason_round IS NULL)
      )
      AND season_name IS NOT DISTINCT FROM (
        CASE WHEN season_phase IS NULL THEN NULL
             ELSE 'MLB_' || source_season::text || '_' || season_phase END
      )
    )
  ),
  DROP CONSTRAINT IF EXISTS game_info_phase_source_sha256_v1,
  ADD CONSTRAINT game_info_phase_source_sha256_v1 CHECK (
    game_type_source_sha256 IS NULL OR game_type_source_sha256 ~ '^[0-9a-f]{64}$'
  ),
  DROP CONSTRAINT IF EXISTS game_info_schedule_relationships_object_v1,
  ADD CONSTRAINT game_info_schedule_relationships_object_v1 CHECK (
    jsonb_typeof(schedule_relationships) = 'object'
  );

ALTER TABLE mlb_cleanroom_v1.games
  DROP CONSTRAINT IF EXISTS games_season_phase_v1,
  DROP CONSTRAINT IF EXISTS games_phase_round_v1,
  DROP CONSTRAINT IF EXISTS games_phase_contract_v1,
  ADD CONSTRAINT games_phase_contract_v1 CHECK (
    (
      source_game_type IS NULL
      AND source_season IS NULL
      AND season_phase IS NULL
      AND postseason_round IS NULL
      AND season_name IS NULL
      AND source_round IS NULL
    )
    OR (
      source_game_type IN ('S','E','I','R','F','D','L','W','P','C','A','N')
      AND source_season IS NOT NULL
      AND source_season > 0
      AND (
        (source_game_type IN ('S','E','I') AND season_phase = 'PRESEASON' AND postseason_round IS NULL)
        OR (source_game_type = 'R' AND season_phase = 'REGULAR_SEASON' AND postseason_round IS NULL)
        OR (source_game_type = 'F' AND season_phase = 'POSTSEASON' AND postseason_round = 'WILD_CARD')
        OR (source_game_type = 'D' AND season_phase = 'POSTSEASON' AND postseason_round = 'DIVISION_SERIES')
        OR (source_game_type = 'L' AND season_phase = 'POSTSEASON' AND postseason_round = 'LEAGUE_CHAMPIONSHIP_SERIES')
        OR (source_game_type = 'W' AND season_phase = 'POSTSEASON' AND postseason_round = 'WORLD_SERIES')
        OR (source_game_type = 'P' AND season_phase = 'POSTSEASON' AND postseason_round = 'PLAYOFFS_UNSPECIFIED')
        OR (source_game_type = 'C' AND season_phase = 'POSTSEASON' AND postseason_round = 'CHAMPIONSHIP_UNSPECIFIED')
        OR (source_game_type IN ('A','N') AND season_phase IS NULL AND postseason_round IS NULL)
      )
      AND season_name IS NOT DISTINCT FROM (
        CASE WHEN season_phase IS NULL THEN NULL
             ELSE 'MLB_' || source_season::text || '_' || season_phase END
      )
    )
  ),
  DROP CONSTRAINT IF EXISTS games_schedule_relationships_object_v1,
  ADD CONSTRAINT games_schedule_relationships_object_v1 CHECK (
    jsonb_typeof(schedule_relationships) = 'object'
  );

CREATE INDEX IF NOT EXISTS idx_game_info_season_phase_v1
  ON mlb.game_info (season_name, season_phase, game_id);
CREATE INDEX IF NOT EXISTS idx_cleanroom_games_season_phase_v1
  ON mlb_cleanroom_v1.games (season_name, season_phase, game_pk);

CREATE OR REPLACE VIEW mlb.canonical_game_phase_v1 AS
WITH candidates AS (
  SELECT
    game_id AS game_pk,
    source_season,
    source_game_type,
    season_phase,
    postseason_round,
    season_name,
    'mlb.game_info'::text AS authority_source
  FROM mlb.game_info
  WHERE source_game_type IS NOT NULL
  UNION ALL
  SELECT
    game_pk,
    source_season,
    source_game_type,
    season_phase,
    postseason_round,
    season_name,
    'mlb_cleanroom_v1.games'::text AS authority_source
  FROM mlb_cleanroom_v1.games
  WHERE source_game_type IS NOT NULL
), unambiguous AS (
  SELECT game_pk
  FROM candidates
  GROUP BY game_pk
  HAVING COUNT(DISTINCT ROW(
    source_season, source_game_type, season_phase, postseason_round, season_name
  )) = 1
)
SELECT
  c.game_pk,
  (array_agg(c.source_season))[1] AS source_season,
  (array_agg(c.source_game_type))[1] AS source_game_type,
  (array_agg(c.season_phase))[1] AS season_phase,
  (array_agg(c.postseason_round))[1] AS postseason_round,
  (array_agg(c.season_name))[1] AS season_name,
  'AUTHORITATIVE_UNAMBIGUOUS'::text AS phase_authority_status,
  COUNT(*)::integer AS authority_row_count,
  array_agg(DISTINCT c.authority_source ORDER BY c.authority_source) AS authority_sources
FROM candidates c
JOIN unambiguous u USING (game_pk)
GROUP BY c.game_pk;

COMMENT ON VIEW mlb.canonical_game_phase_v1 IS
  'Exact-gamePk phase join. Conflicting canonical candidates are omitted so downstream left joins fail closed.';
COMMENT ON COLUMN mlb.game_info.source_game_type IS
  'Exact authoritative MLB StatsAPI gameType. Never reconstructed from a date.';
COMMENT ON COLUMN mlb.game_info.schedule_relationships IS
  'Raw rescheduled/resumed relationship fields retained from authoritative schedule source.';
COMMENT ON COLUMN mlb_cleanroom_v1.games.source_game_type IS
  'Exact authoritative MLB StatsAPI gameType. Never reconstructed from a date.';
COMMENT ON COLUMN mlb_cleanroom_v1.games.schedule_relationships IS
  'Raw rescheduled/resumed relationship fields retained from authoritative schedule source.';
