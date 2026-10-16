-- Run only after nhl_safe_promotion_fixture_setup.sql and the prepared
-- migration, in the disposable fixture database. All row changes roll back.
BEGIN;

INSERT INTO nhl.games VALUES (2026020001, DATE '2026-10-15');
INSERT INTO nhl.skater_game_logs_raw
  (player_id,game_id,game_date,shot_attempts,pp_toi_minutes,toi_minutes,shots_on_goal)
VALUES
  (1,2026020001,DATE '2026-10-15',7,0.00,12.00,2),
  (2,2026020001,DATE '2026-10-15',NULL,NULL,12.00,1),
  (4,2026020001,DATE '2026-10-15',NULL,NULL,12.00,0);
INSERT INTO nhl.import_skater_logs_stage VALUES
  (1,2026020001,DATE '2026-10-15',99,3.25),
  (2,2026020001,DATE '2026-10-15',0,0.00);
INSERT INTO nhl.game_manpower_segments VALUES
  (2026020001,1,0,600,18);
INSERT INTO nhl.shiftcharts_raw VALUES
  (2026020001,2,18,1,0,120),
  (2026020001,4,18,1,0,120);

-- Exercise the effective permissions inherited by the real daily login.
SET LOCAL ROLE proppadia_nhl_daily_login;

-- New official source observations can still be inserted.
INSERT INTO nhl.skater_game_logs_raw
  (player_id,game_id,game_date,shot_attempts,pp_toi_minutes,toi_minutes,shots_on_goal)
VALUES (3,2026020001,DATE '2026-10-15',4,1.50,10.00,1);

-- Same-value INSERT replay is harmless; the conflict clause updates only an
-- ordinary field and leaves the protected values untouched.
INSERT INTO nhl.skater_game_logs_raw
  (player_id,game_id,game_date,shot_attempts,pp_toi_minutes,toi_minutes,shots_on_goal)
VALUES (1,2026020001,DATE '2026-10-15',7,0.00,12.00,2)
ON CONFLICT (player_id,game_id) DO UPDATE SET shots_on_goal=EXCLUDED.shots_on_goal;

DO $$
DECLARE denied boolean;
BEGIN
  denied := false;
  BEGIN
    UPDATE nhl.skater_game_logs_raw SET shot_attempts=99
     WHERE player_id=1 AND game_id=2026020001;
  EXCEPTION WHEN insufficient_privilege THEN denied := true;
  END;
  IF NOT denied THEN RAISE EXCEPTION 'FIXTURE_DIRECT_ATTEMPTS_UPDATE_WAS_ALLOWED'; END IF;

  denied := false;
  BEGIN
    UPDATE nhl.skater_game_logs_raw SET pp_toi_minutes=NULL
     WHERE player_id=1 AND game_id=2026020001;
  EXCEPTION WHEN insufficient_privilege THEN denied := true;
  END;
  IF NOT denied THEN RAISE EXCEPTION 'FIXTURE_DIRECT_PP_TOI_UPDATE_WAS_ALLOWED'; END IF;

  denied := false;
  BEGIN
    DELETE FROM nhl.skater_game_logs_raw WHERE player_id=1 AND game_id=2026020001;
  EXCEPTION WHEN insufficient_privilege THEN denied := true;
  END;
  IF NOT denied THEN RAISE EXCEPTION 'FIXTURE_RAW_DELETE_WAS_ALLOWED'; END IF;

  denied := false;
  BEGIN
    TRUNCATE nhl.skater_game_logs_raw;
  EXCEPTION WHEN insufficient_privilege THEN denied := true;
  END;
  IF NOT denied THEN RAISE EXCEPTION 'FIXTURE_RAW_TRUNCATE_WAS_ALLOWED'; END IF;
END
$$;

SELECT nhl.promote_skater_game_log_protected_fields(DATE '2026-10-15');
SELECT nhl.fill_pp_toi_minutes_from_shift_overlap(2026020001);
SELECT nhl.fill_pp_toi_minutes_from_shift_overlap(2026020001);

DO $$
DECLARE existing_attempts integer; existing_pp numeric; filled_attempts integer;
        filled_pp numeric; replay_fills integer; pp_fills integer; overlap_fill numeric;
BEGIN
  SELECT shot_attempts,pp_toi_minutes INTO existing_attempts,existing_pp
    FROM nhl.skater_game_logs_raw WHERE player_id=1 AND game_id=2026020001;
  IF existing_attempts<>7 OR existing_pp<>0.00 THEN
    RAISE EXCEPTION 'FIXTURE_CONFLICT_OVERWROTE_POPULATED_SOURCE';
  END IF;
  SELECT shot_attempts,pp_toi_minutes INTO filled_attempts,filled_pp
    FROM nhl.skater_game_logs_raw WHERE player_id=2 AND game_id=2026020001;
  IF filled_attempts<>0 OR filled_pp<>0.00 THEN
    RAISE EXCEPTION 'FIXTURE_VERIFIED_NULL_FILL_FAILED';
  END IF;
  SELECT pp_toi_minutes INTO overlap_fill FROM nhl.skater_game_logs_raw
   WHERE player_id=4 AND game_id=2026020001;
  IF overlap_fill<>2.00 THEN RAISE EXCEPTION 'FIXTURE_SHIFT_OVERLAP_FILL_FAILED'; END IF;
  SELECT nhl.promote_skater_game_log_protected_fields(DATE '2026-10-15') INTO replay_fills;
  IF replay_fills<>0 THEN RAISE EXCEPTION 'FIXTURE_STAGE_REPLAY_NOT_IDEMPOTENT'; END IF;
  SELECT nhl.fill_pp_toi_minutes_from_shift_overlap(2026020001) INTO pp_fills;
  IF pp_fills<>0 THEN RAISE EXCEPTION 'FIXTURE_PP_TOI_REPLAY_NOT_IDEMPOTENT'; END IF;
  IF pg_has_role(current_user,'proppadia_nhl_source_correction','MEMBER') THEN
    RAISE EXCEPTION 'FIXTURE_INGEST_CAN_ASSUME_CORRECTION_ROLE';
  END IF;
END
$$;

RESET ROLE;
ROLLBACK;
