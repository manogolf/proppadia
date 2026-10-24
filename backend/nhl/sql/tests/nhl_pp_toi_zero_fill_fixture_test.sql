-- Run only after nhl_safe_promotion_fixture_setup.sql, the 20261017 safe
-- promotion migration and the 20261024 verified-zero migration, in the
-- disposable fixture database. All row changes roll back.
BEGIN;

INSERT INTO nhl.games VALUES (2026020101, DATE '2026-10-20'), (2026020102, DATE '2026-10-20');
-- Team 18 had a PP window in period 1 from 0 to 120 seconds in game 101.
INSERT INTO nhl.game_manpower_segments VALUES (2026020101,1,0,120,18);
INSERT INTO nhl.skater_game_logs_raw
  (player_id,game_id,game_date,shot_attempts,pp_toi_minutes,toi_minutes,shots_on_goal,team_id,pp_sog)
VALUES
  (10,2026020101,DATE '2026-10-20',1,NULL,10.00,0,18,0),   -- complete shifts, no PP overlap
  (11,2026020101,DATE '2026-10-20',1,NULL,10.00,0,18,0),   -- overlaps the PP window
  (12,2026020101,DATE '2026-10-20',1,NULL,10.00,0,18,0),   -- no shift rows
  (13,2026020101,DATE '2026-10-20',1,NULL,10.00,0,18,0),   -- shifts cover only 5 of 10 minutes
  (14,2026020101,DATE '2026-10-20',1,1.25,10.00,0,18,0),   -- populated value
  (15,2026020101,DATE '2026-10-20',1,NULL,10.00,1,18,1),   -- recorded a PP shot on goal
  (16,2026020101,DATE '2026-10-20',1,NULL,10.00,0,19,0),   -- opponent, no PP windows of its own
  (20,2026020102,DATE '2026-10-20',1,NULL,10.00,0,18,0);   -- game without manpower segments
INSERT INTO nhl.shiftcharts_raw VALUES
  (2026020101,10,18,1,300,900),
  (2026020101,11,18,1,0,600),
  (2026020101,13,18,1,300,600),
  (2026020101,14,18,1,300,900),
  (2026020101,15,18,1,300,900),
  (2026020101,16,19,1,0,600),
  (2026020101,16,19,1,0,600),        -- duplicate interval counts once
  (2026020102,20,18,1,300,900);

SET LOCAL ROLE proppadia_nhl_daily_login;

DO $$
DECLARE filled integer; replay integer; v numeric;
BEGIN
  IF NOT has_function_privilege(current_user,
       'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') THEN
    RAISE EXCEPTION 'FIXTURE_DAILY_ZERO_FILL_EXECUTE_MISSING';
  END IF;
  IF has_function_privilege('proppadia_nhl_source_correction',
       'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE')
     OR has_function_privilege('anon', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') THEN
    RAISE EXCEPTION 'FIXTURE_ZERO_FILL_EXECUTE_TOO_BROAD';
  END IF;

  SELECT nhl.fill_pp_toi_zero_from_shift_overlap(2026020101) INTO filled;
  IF filled <> 2 THEN RAISE EXCEPTION 'FIXTURE_ZERO_FILL_COUNT_MISMATCH:%', filled; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=10 AND game_id=2026020101;
  IF v IS DISTINCT FROM 0.00 THEN RAISE EXCEPTION 'FIXTURE_VERIFIED_ZERO_NOT_WRITTEN'; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=16 AND game_id=2026020101;
  IF v IS DISTINCT FROM 0.00 THEN RAISE EXCEPTION 'FIXTURE_OPPONENT_ZERO_NOT_WRITTEN'; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=11 AND game_id=2026020101;
  IF v IS NOT NULL THEN RAISE EXCEPTION 'FIXTURE_ZERO_WRITTEN_DESPITE_PP_OVERLAP'; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=12 AND game_id=2026020101;
  IF v IS NOT NULL THEN RAISE EXCEPTION 'FIXTURE_ZERO_WRITTEN_WITHOUT_SHIFTS'; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=13 AND game_id=2026020101;
  IF v IS NOT NULL THEN RAISE EXCEPTION 'FIXTURE_ZERO_WRITTEN_WITH_INCOMPLETE_SHIFTS'; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=14 AND game_id=2026020101;
  IF v IS DISTINCT FROM 1.25 THEN RAISE EXCEPTION 'FIXTURE_ZERO_FILL_OVERWROTE_POPULATED_SOURCE'; END IF;
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=15 AND game_id=2026020101;
  IF v IS NOT NULL THEN RAISE EXCEPTION 'FIXTURE_ZERO_WRITTEN_DESPITE_PP_SOG'; END IF;

  SELECT nhl.fill_pp_toi_zero_from_shift_overlap(2026020102) INTO filled;
  IF filled <> 0 THEN RAISE EXCEPTION 'FIXTURE_ZERO_WRITTEN_WITHOUT_MANPOWER_SEGMENTS'; END IF;

  SELECT nhl.fill_pp_toi_zero_from_shift_overlap(2026020101) INTO replay;
  IF replay <> 0 THEN RAISE EXCEPTION 'FIXTURE_ZERO_FILL_REPLAY_NOT_IDEMPOTENT'; END IF;

  -- The positive fill still owns the overlapping player.
  PERFORM nhl.fill_pp_toi_minutes_from_shift_overlap(2026020101);
  SELECT pp_toi_minutes INTO v FROM nhl.skater_game_logs_raw WHERE player_id=11 AND game_id=2026020101;
  IF v IS DISTINCT FROM 2.00 THEN RAISE EXCEPTION 'FIXTURE_POSITIVE_FILL_AFTER_ZERO_FAILED'; END IF;
END
$$;

DO $$
DECLARE denied boolean := false;
BEGIN
  BEGIN
    UPDATE nhl.skater_game_logs_raw SET pp_toi_minutes=0 WHERE player_id=12 AND game_id=2026020101;
  EXCEPTION WHEN insufficient_privilege THEN denied := true;
  END;
  IF NOT denied THEN RAISE EXCEPTION 'FIXTURE_DIRECT_PP_TOI_UPDATE_WAS_ALLOWED'; END IF;
END
$$;

RESET ROLE;
ROLLBACK;
