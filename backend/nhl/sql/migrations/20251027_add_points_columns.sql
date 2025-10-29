-- Add goals/assists/points to staging and raw (idempotent).

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='nhl' AND table_name='import_skater_logs_stage' AND column_name='goals') THEN
    ALTER TABLE nhl.import_skater_logs_stage ADD COLUMN goals int;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='nhl' AND table_name='import_skater_logs_stage' AND column_name='assists') THEN
    ALTER TABLE nhl.import_skater_logs_stage ADD COLUMN assists int;
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='nhl' AND table_name='skater_game_logs_raw' AND column_name='goals') THEN
    ALTER TABLE nhl.skater_game_logs_raw ADD COLUMN goals int;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='nhl' AND table_name='skater_game_logs_raw' AND column_name='assists') THEN
    ALTER TABLE nhl.skater_game_logs_raw ADD COLUMN assists int;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='nhl' AND table_name='skater_game_logs_raw' AND column_name='points') THEN
    ALTER TABLE nhl.skater_game_logs_raw ADD COLUMN points int;
  END IF;
END$$;

-- Keep points consistent
UPDATE nhl.skater_game_logs_raw
SET points = COALESCE(goals,0) + COALESCE(assists,0)
WHERE points IS DISTINCT FROM COALESCE(goals,0) + COALESCE(assists,0);
