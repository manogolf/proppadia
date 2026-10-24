-- Read-only scope check for NULL pp_toi_minutes since 2026-01-01 (P2.31 source fix).
-- Classes: a no TOI; b game without manpower segments; c no shift charts for the player;
-- d shift total differs from TOI by more than 1 minute; e positive PP overlap left unfilled;
-- f verified-zero candidate (shifts complete, segments present, zero overlap).
-- Run as the research login. 2026-10-24: f=7,414 (5,794 season 2025, 1,620 season 2026), e=2,101 (2026-01-09..01-25), c=1,949, b=107, a+d=4.
WITH nulls AS (
  SELECT l.game_id, l.player_id, l.team_id, l.toi_minutes, g.game_date, g.season
  FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
  WHERE substring(g.game_id::text,5,2)='02' AND l.pp_toi_minutes IS NULL AND g.game_date >= DATE '2026-01-01'
), seg AS (
  SELECT game_id, count(*) n_seg, count(DISTINCT pp_team_id) pp_teams FROM nhl.game_manpower_segments GROUP BY 1
), sh AS (
  SELECT game_id, player_id, sum(end_sec-start_sec) shift_sec, count(*) n_shift
  FROM (SELECT DISTINCT game_id, player_id, team_id, period, start_sec, end_sec FROM nhl.shiftcharts_raw
        WHERE end_sec > start_sec AND game_id IN (SELECT game_id FROM nulls)) s GROUP BY 1,2
), ov AS (
  SELECT s.game_id, s.player_id, sum(GREATEST(0, LEAST(s.end_sec,p.end_sec)-GREATEST(s.start_sec,p.start_sec))) ov_sec
  FROM (SELECT DISTINCT game_id, player_id, team_id, period, start_sec, end_sec FROM nhl.shiftcharts_raw
        WHERE end_sec > start_sec AND game_id IN (SELECT game_id FROM nulls)) s
  JOIN (SELECT DISTINCT game_id, period, start_sec, end_sec, pp_team_id FROM nhl.game_manpower_segments) p
    ON p.game_id=s.game_id AND p.period=s.period AND p.pp_team_id=s.team_id
  GROUP BY 1,2
)
SELECT
  CASE WHEN n.toi_minutes IS NULL OR n.toi_minutes = 0 THEN 'a_no_toi'
       WHEN seg.game_id IS NULL THEN 'b_game_without_segments'
       WHEN sh.player_id IS NULL THEN 'c_no_shifts_for_player'
       WHEN abs(sh.shift_sec/60.0 - n.toi_minutes) > 1.0 THEN 'd_shift_toi_mismatch_gt_1min'
       WHEN COALESCE(ov.ov_sec,0) > 0 THEN 'e_positive_overlap_not_filled'
       ELSE 'f_verified_zero_candidate' END AS class,
  n.season, count(*) AS rows, count(DISTINCT n.game_id) AS games
FROM nulls n LEFT JOIN seg USING (game_id) LEFT JOIN sh USING (game_id, player_id) LEFT JOIN ov USING (game_id, player_id)
GROUP BY 1,2 ORDER BY 2,1;
