-- PREPARED ONLY. Owner authorization required. Paste the whole block into the
-- Supabase SQL editor for the production project and run it once.
-- It does what backend/nhl/scripts/execute_nhl_source_correction.py does, without
-- a stored credential: one transaction that temporarily grants the correction
-- role, switches to it, verifies the effective role, runs the reviewed guarded
-- UPDATE (identical to repair_nhl_pp_toi_32_duplicate_shift_intervals.sql),
-- switches back, revokes the role, and verifies the revoke. Any failure aborts
-- the transaction and nothing is committed, including the grant.
-- Before running: run verify_nhl_pp_toi_32_duplicate_shift_intervals.sql
-- (expect original=32, corrected=0). The final SELECT below is the receipt
-- (expect original=0, corrected=32); save its output with the approval.

BEGIN;

DO $$
BEGIN
  IF session_user <> 'postgres' THEN
    RAISE EXCEPTION 'RUN_AS_POSTGRES_IN_SQL_EDITOR:session_user=%', session_user;
  END IF;
  IF pg_has_role(session_user, 'proppadia_nhl_source_correction', 'MEMBER') THEN
    RAISE EXCEPTION 'CORRECTION_ROLE_ALREADY_GRANTED_REFUSING_TO_PROCEED';
  END IF;
END $$;

GRANT proppadia_nhl_source_correction TO postgres;
SET LOCAL ROLE proppadia_nhl_source_correction;

DO $$
BEGIN
  IF current_user <> 'proppadia_nhl_source_correction' THEN
    RAISE EXCEPTION 'CORRECTION_SET_ROLE_VERIFICATION_FAILED:current_user=%', current_user;
  END IF;
END $$;

UPDATE nhl.skater_game_logs_raw
SET pp_toi_minutes = c.to_value
FROM (VALUES
    (2023020012::bigint, 8482684::bigint, 17::bigint, 4.55::numeric, 4.38::numeric),  -- NJD official 1
    (2023020204, 8480839, 4, 5.98, 5.38),  -- BUF official 7
    (2023020268, 8483460, 6, 0.83, 0.50),  -- CBJ official 29
    (2023020329, 8475714, 28, 4.65, 4.20),  -- TOR official 10
    (2023020352, 8477979, 32, 1.77, 0.88),  -- WSH official 15
    (2023020428, 8473449, 4, 2.42, 1.53),  -- BUF official 7
    (2023020650, 8480003, 3, 1.33, 0.67),  -- BOS official 6
    (2023020692, 8478028, 9, 0.47, 0.23),  -- COL official 21
    (2023020693, 8476875, 16, 5.30, 4.42),  -- MTL official 8
    (2023020748, 8478115, 19, 2.83, 2.43),  -- NYI official 2
    (2023020835, 8480113, 31, 0.57, 0.28),  -- WPG official 52
    (2023020890, 8477952, 11, 3.58, 2.70),  -- DET official 17
    (2023020927, 8475958, 32, 1.07, 0.85),  -- WSH official 15
    (2023020961, 8478466, 11, 4.85, 3.95),  -- DET official 17
    (2023020961, 8479314, 13, 6.62, 6.33),  -- FLA official 13
    (2023020979, 8483466, 8, 3.28, 3.18),  -- CHI official 16
    (2023020998, 8474590, 32, 3.40, 3.28),  -- WSH official 15
    (2023021058, 8479400, 14, 2.08, 1.58),  -- LAK official 26
    (2023021058, 8481606, 14, 1.50, 1.00),  -- LAK official 26
    (2023021063, 8477839, 27, 0.92, 0.63),  -- TBL official 14
    (2023021081, 8479977, 24, 4.72, 3.93),  -- SEA official 55
    (2023021116, 8480748, 18, 0.23, 0.12),  -- NSH official 18
    (2023021188, 8482133, 25, 2.78, 2.52),  -- SJS official 28
    (2023021277, 8477503, 28, 1.38, 1.23),  -- TOR official 10
    (2024020123, 8477903, 22, 0.10, 0.05),  -- PHI official 4
    (2024020141, 8474600, 18, 4.98, 4.87),  -- NSH official 18
    (2024020331, 8480043, 25, 6.08, 5.65),  -- SJS official 28
    (2024020557, 8480806, 1, 1.17, 0.82),  -- ANA official 24
    (2024020557, 8482745, 1, 4.53, 3.60),  -- ANA official 24
    (2024020644, 8476461, 22, 1.35, 0.80),  -- PHI official 4
    (2024020843, 8475172, 7, 6.03, 5.75),  -- CGY official 20
    (2024021138, 8476468, 20, 1.27, 1.22)  -- NYR official 3
) AS c(game_id, player_id, raw_team_id, from_value, to_value)
WHERE skater_game_logs_raw.game_id = c.game_id
  AND skater_game_logs_raw.player_id = c.player_id
  AND skater_game_logs_raw.team_id = c.raw_team_id
  AND skater_game_logs_raw.pp_toi_minutes = c.from_value
  -- All-or-nothing: every one of the 32 keys must currently hold its exact
  -- original value under its pinned team, or the cast raises and the
  -- executor rolls the whole transaction back.
  AND (SELECT CASE WHEN count(*) = 32 THEN TRUE
               ELSE ('PP_TOI_32_ORIGINAL_VALUE_GUARD_FAILED:matching_rows=' || count(*))::boolean END
       FROM nhl.skater_game_logs_raw g
       JOIN (VALUES
    (2023020012::bigint, 8482684::bigint, 17::bigint, 4.55::numeric, 4.38::numeric),  -- NJD official 1
    (2023020204, 8480839, 4, 5.98, 5.38),  -- BUF official 7
    (2023020268, 8483460, 6, 0.83, 0.50),  -- CBJ official 29
    (2023020329, 8475714, 28, 4.65, 4.20),  -- TOR official 10
    (2023020352, 8477979, 32, 1.77, 0.88),  -- WSH official 15
    (2023020428, 8473449, 4, 2.42, 1.53),  -- BUF official 7
    (2023020650, 8480003, 3, 1.33, 0.67),  -- BOS official 6
    (2023020692, 8478028, 9, 0.47, 0.23),  -- COL official 21
    (2023020693, 8476875, 16, 5.30, 4.42),  -- MTL official 8
    (2023020748, 8478115, 19, 2.83, 2.43),  -- NYI official 2
    (2023020835, 8480113, 31, 0.57, 0.28),  -- WPG official 52
    (2023020890, 8477952, 11, 3.58, 2.70),  -- DET official 17
    (2023020927, 8475958, 32, 1.07, 0.85),  -- WSH official 15
    (2023020961, 8478466, 11, 4.85, 3.95),  -- DET official 17
    (2023020961, 8479314, 13, 6.62, 6.33),  -- FLA official 13
    (2023020979, 8483466, 8, 3.28, 3.18),  -- CHI official 16
    (2023020998, 8474590, 32, 3.40, 3.28),  -- WSH official 15
    (2023021058, 8479400, 14, 2.08, 1.58),  -- LAK official 26
    (2023021058, 8481606, 14, 1.50, 1.00),  -- LAK official 26
    (2023021063, 8477839, 27, 0.92, 0.63),  -- TBL official 14
    (2023021081, 8479977, 24, 4.72, 3.93),  -- SEA official 55
    (2023021116, 8480748, 18, 0.23, 0.12),  -- NSH official 18
    (2023021188, 8482133, 25, 2.78, 2.52),  -- SJS official 28
    (2023021277, 8477503, 28, 1.38, 1.23),  -- TOR official 10
    (2024020123, 8477903, 22, 0.10, 0.05),  -- PHI official 4
    (2024020141, 8474600, 18, 4.98, 4.87),  -- NSH official 18
    (2024020331, 8480043, 25, 6.08, 5.65),  -- SJS official 28
    (2024020557, 8480806, 1, 1.17, 0.82),  -- ANA official 24
    (2024020557, 8482745, 1, 4.53, 3.60),  -- ANA official 24
    (2024020644, 8476461, 22, 1.35, 0.80),  -- PHI official 4
    (2024020843, 8475172, 7, 6.03, 5.75),  -- CGY official 20
    (2024021138, 8476468, 20, 1.27, 1.22)  -- NYR official 3
       ) AS k(game_id, player_id, raw_team_id, from_value, to_value)
         ON g.game_id = k.game_id AND g.player_id = k.player_id
        AND g.team_id = k.raw_team_id AND g.pp_toi_minutes = k.from_value);

RESET ROLE;
REVOKE proppadia_nhl_source_correction FROM postgres;

DO $$
BEGIN
  IF current_user <> 'postgres'
     OR pg_has_role('postgres', 'proppadia_nhl_source_correction', 'MEMBER') THEN
    RAISE EXCEPTION 'CORRECTION_ROLE_RESET_OR_REVOKE_FAILED';
  END IF;
END $$;

COMMIT;

-- Receipt (read-only).
WITH k(game_id, player_id, original_value, corrected_value) AS (VALUES
    (2023020012::bigint, 8482684::bigint, 4.55::numeric, 4.38::numeric),
    (2023020204, 8480839, 5.98, 5.38),
    (2023020268, 8483460, 0.83, 0.50),
    (2023020329, 8475714, 4.65, 4.20),
    (2023020352, 8477979, 1.77, 0.88),
    (2023020428, 8473449, 2.42, 1.53),
    (2023020650, 8480003, 1.33, 0.67),
    (2023020692, 8478028, 0.47, 0.23),
    (2023020693, 8476875, 5.30, 4.42),
    (2023020748, 8478115, 2.83, 2.43),
    (2023020835, 8480113, 0.57, 0.28),
    (2023020890, 8477952, 3.58, 2.70),
    (2023020927, 8475958, 1.07, 0.85),
    (2023020961, 8478466, 4.85, 3.95),
    (2023020961, 8479314, 6.62, 6.33),
    (2023020979, 8483466, 3.28, 3.18),
    (2023020998, 8474590, 3.40, 3.28),
    (2023021058, 8479400, 2.08, 1.58),
    (2023021058, 8481606, 1.50, 1.00),
    (2023021063, 8477839, 0.92, 0.63),
    (2023021081, 8479977, 4.72, 3.93),
    (2023021116, 8480748, 0.23, 0.12),
    (2023021188, 8482133, 2.78, 2.52),
    (2023021277, 8477503, 1.38, 1.23),
    (2024020123, 8477903, 0.10, 0.05),
    (2024020141, 8474600, 4.98, 4.87),
    (2024020331, 8480043, 6.08, 5.65),
    (2024020557, 8480806, 1.17, 0.82),
    (2024020557, 8482745, 4.53, 3.60),
    (2024020644, 8476461, 1.35, 0.80),
    (2024020843, 8475172, 6.03, 5.75),
    (2024021138, 8476468, 1.27, 1.22)
)
SELECT now() AS checked_at_utc, current_user AS checked_as,
       count(*) FILTER (WHERE r.pp_toi_minutes = k.original_value) AS original,
       count(*) FILTER (WHERE r.pp_toi_minutes = k.corrected_value) AS corrected,
       count(*) FILTER (WHERE r.player_id IS NULL) AS missing,
       count(*) FILTER (WHERE r.player_id IS NOT NULL AND r.pp_toi_minutes IS DISTINCT FROM k.original_value
                          AND r.pp_toi_minutes IS DISTINCT FROM k.corrected_value) AS other,
       json_agg(json_build_object('game_id', k.game_id, 'player_id', k.player_id,
                                  'pp_toi_minutes', r.pp_toi_minutes) ORDER BY k.game_id, k.player_id) AS rows
FROM k LEFT JOIN nhl.skater_game_logs_raw r USING (game_id, player_id);
