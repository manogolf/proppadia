BEGIN;

CREATE OR REPLACE VIEW mlb_cleanroom_v1.lineup_temporal_observation_audit AS
SELECT
  l.*,
  g.scheduled_start_utc,
  CASE
    WHEN l.snapshot_timestamp_utc IS NULL THEN 'LINEUP_TIME_MISSING'
    WHEN g.scheduled_start_utc IS NULL THEN 'LINEUP_SCHEDULE_TIME_MISSING'
    WHEN l.snapshot_timestamp_utc >= g.scheduled_start_utc THEN 'LINEUP_POST_FIRST_PITCH'
    WHEN l.player_mlb_id IS NULL THEN 'LINEUP_IDENTITY_UNRESOLVED'
    ELSE 'LINEUP_VALID_PREGAME'
  END AS temporal_classification
FROM mlb_cleanroom_v1.lineup_snapshots l
LEFT JOIN mlb_cleanroom_v1.current_games g USING (game_pk);

COMMENT ON VIEW mlb_cleanroom_v1.lineup_temporal_observation_audit IS
  'All authentic lineup observations with exact-game first-pitch temporal classification; no source rows excluded.';

CREATE OR REPLACE VIEW mlb_cleanroom_v1.valid_pregame_lineup_observations AS
SELECT *
FROM mlb_cleanroom_v1.lineup_temporal_observation_audit
WHERE temporal_classification = 'LINEUP_VALID_PREGAME'
  AND lineup_status = 'CONFIRMED'
  AND batting_order_position BETWEEN 1 AND 9;

COMMENT ON VIEW mlb_cleanroom_v1.valid_pregame_lineup_observations IS
  'Confirmed exact-ID lineup observations strictly before official first pitch. Governing-capture queries must additionally enforce market-time and run-visibility boundaries.';

GRANT SELECT ON mlb_cleanroom_v1.lineup_temporal_observation_audit
  TO mlb_cleanroom_research;
GRANT SELECT ON mlb_cleanroom_v1.valid_pregame_lineup_observations
  TO mlb_cleanroom_research;

COMMIT;
