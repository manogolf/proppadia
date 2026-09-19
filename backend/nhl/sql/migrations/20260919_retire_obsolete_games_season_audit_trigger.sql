-- Retire the redundant legacy season-audit trigger whose function targets
-- columns removed from nhl.games_season_audit. The authoritative
-- trg_audit_games_season_write trigger remains active.
--
-- Pre-change trigger definition / exact rollback SQL:
--   CREATE TRIGGER trg_games_season_audit
--   AFTER INSERT OR UPDATE OF season ON nhl.games
--   FOR EACH ROW EXECUTE FUNCTION nhl.log_games_season_change();

BEGIN;

DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'nhl.games'::regclass
      AND tgname = 'trg_audit_games_season_write'
      AND NOT tgisinternal
  ) THEN
    RAISE EXCEPTION
      'authoritative trigger trg_audit_games_season_write is absent; refusing legacy-trigger retirement';
  END IF;
END
$migration$;

DROP TRIGGER IF EXISTS trg_games_season_audit ON nhl.games;

COMMIT;

-- Exact rollback (apply only after reviewed authorization):
-- BEGIN;
-- CREATE TRIGGER trg_games_season_audit
-- AFTER INSERT OR UPDATE OF season ON nhl.games
-- FOR EACH ROW EXECUTE FUNCTION nhl.log_games_season_change();
-- COMMIT;
