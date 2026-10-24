-- Read-only check for the season-2025 shift-chart payload correction (P2.31).
-- Run with the research login before and after. Before (2026-10-24 20:57 UTC):
-- still_fixable=208283. After a committed correction: still_fixable=0 and the
-- whole_game_seconds / null_seconds / null_team counts drop to 0 for rows whose
-- payload carries the value.
BEGIN READ ONLY;
WITH p AS (
  SELECT start_sec, end_sec, duration_sec, team_id, period, (raw_json->>'typeCode')::int AS tc,
         CASE WHEN raw_json->>'startTime' ~ '^\d+:\d{2}$'
              THEN split_part(raw_json->>'startTime', ':', 1)::int * 60 + split_part(raw_json->>'startTime', ':', 2)::int END AS ps,
         CASE WHEN raw_json->>'endTime' ~ '^\d+:\d{2}$'
              THEN split_part(raw_json->>'endTime', ':', 1)::int * 60 + split_part(raw_json->>'endTime', ':', 2)::int END AS pe,
         CASE WHEN raw_json->>'duration' ~ '^\d+:\d{2}$'
              THEN split_part(raw_json->>'duration', ':', 1)::int * 60 + split_part(raw_json->>'duration', ':', 2)::int END AS pd,
         (raw_json->>'teamId')::bigint AS pt
    FROM nhl.shiftcharts_raw
   WHERE substring(game_id::text, 1, 4) = '2025' AND substring(game_id::text, 5, 2) = '02'
)
SELECT now() AS checked_at_utc, current_user AS checked_as,
       count(*) FILTER (WHERE ps IS NOT NULL AND pe IS NOT NULL AND ps <= pe
                          AND (team_id IS NULL OR pt IS NULL OR team_id = pt)
                          AND (start_sec IS NULL OR start_sec - ps IN (0, 1200, 2400, 3600, 4800))
                          AND (end_sec IS NULL OR end_sec - pe IN (0, 1200, 2400, 3600, 4800))
                          AND (start_sec IS DISTINCT FROM ps OR end_sec IS DISTINCT FROM pe
                               OR (duration_sec IS NULL AND pd IS NOT NULL) OR (team_id IS NULL AND pt IS NOT NULL))) AS still_fixable,
       count(*) FILTER (WHERE start_sec IS NOT NULL AND ps IS NOT NULL AND start_sec - ps IN (1200, 2400, 3600, 4800)) AS whole_game_seconds,
       count(*) FILTER (WHERE start_sec IS NULL AND ps IS NOT NULL) AS null_seconds,
       count(*) FILTER (WHERE team_id IS NULL AND pt IS NOT NULL) AS null_team,
       count(*) FILTER (WHERE start_sec IS NOT NULL AND ps IS NOT NULL AND start_sec <> ps
                          AND start_sec - ps NOT IN (1200, 2400, 3600, 4800)) AS other_conflicts
FROM p;
COMMIT;
