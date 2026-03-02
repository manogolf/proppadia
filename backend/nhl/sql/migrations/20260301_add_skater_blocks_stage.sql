DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'nhl'
      AND table_name = 'import_skater_logs_stage'
      AND column_name = 'blocks'
  ) THEN
    ALTER TABLE nhl.import_skater_logs_stage ADD COLUMN blocks integer;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'nhl'
      AND table_name = 'skater_game_logs_raw'
      AND column_name = 'blocks'
  ) THEN
    ALTER TABLE nhl.skater_game_logs_raw ADD COLUMN blocks smallint;
  END IF;
END $$;
