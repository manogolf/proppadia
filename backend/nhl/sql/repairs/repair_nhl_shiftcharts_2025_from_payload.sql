-- PREPARED ONLY. Run as the ingestion login in one transaction; commit only if
-- the row count equals :expected_rows (the preview count). P2.31.
-- Season-2025 regular-season shift charts: recompute start_sec, end_sec,
-- duration_sec and team_id from each row's own official payload (raw_json:
-- period-relative startTime/endTime/duration, teamId). Only these patterns
-- change: seconds stored as whole-game seconds (exactly 1200/2400/3600/4800
-- too high), missing seconds, missing duration, missing team ID. A row whose
-- stored value conflicts with its payload in any other way is not touched,
-- and the guard refuses everything if the count differs.
WITH p AS (
  SELECT shift_id, start_sec, end_sec, duration_sec, team_id,
         CASE WHEN raw_json->>'startTime' ~ '^\d+:\d{2}$'
              THEN split_part(raw_json->>'startTime', ':', 1)::int * 60 + split_part(raw_json->>'startTime', ':', 2)::int END AS ps,
         CASE WHEN raw_json->>'endTime' ~ '^\d+:\d{2}$'
              THEN split_part(raw_json->>'endTime', ':', 1)::int * 60 + split_part(raw_json->>'endTime', ':', 2)::int END AS pe,
         CASE WHEN raw_json->>'duration' ~ '^\d+:\d{2}$'
              THEN split_part(raw_json->>'duration', ':', 1)::int * 60 + split_part(raw_json->>'duration', ':', 2)::int END AS pd,
         (raw_json->>'teamId')::bigint AS pt
    FROM nhl.shiftcharts_raw
   WHERE substring(game_id::text, 1, 4) = '2025' AND substring(game_id::text, 5, 2) = '02'
     AND (raw_json->>'gameId')::bigint = game_id
), t AS (
  SELECT shift_id, ps, pe,
         CASE WHEN duration_sec IS NULL THEN pd ELSE duration_sec END AS new_duration,
         COALESCE(team_id, pt) AS new_team
    FROM p
   WHERE ps IS NOT NULL AND pe IS NOT NULL AND ps <= pe
     AND (team_id IS NULL OR pt IS NULL OR team_id = pt)
     AND (start_sec IS NULL OR start_sec - ps IN (0, 1200, 2400, 3600, 4800))
     AND (end_sec IS NULL OR end_sec - pe IN (0, 1200, 2400, 3600, 4800))
     AND (start_sec IS DISTINCT FROM ps OR end_sec IS DISTINCT FROM pe
          OR (duration_sec IS NULL AND pd IS NOT NULL) OR (team_id IS NULL AND pt IS NOT NULL))
)
UPDATE nhl.shiftcharts_raw AS raw
   SET start_sec = t.ps, end_sec = t.pe, duration_sec = t.new_duration, team_id = t.new_team
  FROM t
 WHERE raw.shift_id = t.shift_id
   AND (SELECT CASE WHEN count(*) = :expected_rows THEN TRUE
                ELSE ('SHIFTCHARTS_2025_GUARD_FAILED:matching_rows=' || count(*))::boolean END FROM t);
