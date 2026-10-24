-- READ-ONLY preview of the P2.31 PP-TOI backfill for regular-season games
-- from 2026-01-01 to yesterday. Lists every NULL pp_toi_minutes row the two
-- fixed fill functions would change, with the value each would write:
--   POSITIVE  nhl.fill_pp_toi_minutes_from_shift_overlap (existing function)
--   ZERO      nhl.fill_pp_toi_zero_from_shift_overlap (20261024 migration)
-- Same predicates as the functions. Run as the research login.
WITH games AS (
  SELECT game_id, game_date FROM nhl.games
   WHERE substring(game_id::text,5,2) = '02'
     AND game_date >= DATE '2026-01-01' AND game_date < CURRENT_DATE
), seg_games AS (
  SELECT DISTINCT game_id FROM nhl.game_manpower_segments
   WHERE game_id IN (SELECT game_id FROM games)
     AND start_sec IS NOT NULL AND end_sec IS NOT NULL AND end_sec > start_sec AND pp_team_id IS NOT NULL
), pp AS (
  SELECT DISTINCT game_id, period, start_sec, end_sec, pp_team_id AS team_id
    FROM nhl.game_manpower_segments
   WHERE game_id IN (SELECT game_id FROM games)
     AND start_sec IS NOT NULL AND end_sec IS NOT NULL AND end_sec > start_sec AND pp_team_id IS NOT NULL
), shifts AS (
  SELECT DISTINCT game_id, player_id, team_id, period, start_sec, end_sec
    FROM nhl.shiftcharts_raw
   WHERE game_id IN (SELECT game_id FROM games)
     AND player_id IS NOT NULL AND team_id IS NOT NULL AND period IS NOT NULL
     AND start_sec IS NOT NULL AND end_sec IS NOT NULL AND end_sec > start_sec
), coverage AS (
  SELECT game_id, player_id::bigint AS player_id, SUM(end_sec - start_sec) AS shift_seconds,
         COUNT(DISTINCT team_id) AS team_count
    FROM shifts GROUP BY 1, 2
), overlap AS (
  SELECT s.game_id, s.player_id::bigint AS player_id,
         SUM(GREATEST(0, LEAST(s.end_sec, p.end_sec) - GREATEST(s.start_sec, p.start_sec))) AS pp_seconds
    FROM shifts s JOIN pp p ON p.game_id = s.game_id AND p.period = s.period AND p.team_id = s.team_id
   GROUP BY 1, 2
)
SELECT g.game_date, raw.game_id, raw.player_id, raw.team_id, raw.toi_minutes,
       CASE WHEN COALESCE(o.pp_seconds, 0) > 0 THEN 'POSITIVE' ELSE 'ZERO' END AS fill_kind,
       CASE WHEN COALESCE(o.pp_seconds, 0) > 0 THEN round((o.pp_seconds / 60.0)::numeric, 2) ELSE 0.00 END AS proposed_pp_toi_minutes
  FROM nhl.skater_game_logs_raw raw
  JOIN games g USING (game_id)
  LEFT JOIN overlap o ON o.game_id = raw.game_id AND o.player_id = raw.player_id
  LEFT JOIN coverage c ON c.game_id = raw.game_id AND c.player_id = raw.player_id
 WHERE raw.pp_toi_minutes IS NULL
   AND (
     (COALESCE(o.pp_seconds, 0) > 0 AND (raw.toi_minutes IS NULL OR o.pp_seconds / 60.0 <= raw.toi_minutes))
     OR (COALESCE(o.pp_seconds, 0) = 0 AND raw.game_id IN (SELECT game_id FROM seg_games)
         AND raw.toi_minutes > 0 AND COALESCE(raw.pp_sog, 0) = 0
         AND c.team_count = 1 AND abs(c.shift_seconds - raw.toi_minutes * 60) <= 60)
   )
 ORDER BY g.game_date, raw.game_id, raw.player_id;
