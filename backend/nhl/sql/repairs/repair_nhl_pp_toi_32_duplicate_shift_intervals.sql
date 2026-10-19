-- PREPARED ONLY. Separate owner authorization is required before execution.
-- Run only through the governed executor, which verifies the admin session,
-- grants and SET ROLEs proppadia_nhl_source_correction, checks current_user and
-- exact privileges, runs this single UPDATE, then resets and revokes the role:
--   ./bin/python backend/nhl/scripts/execute_nhl_source_correction.py \
--     --sql-file <this file> --sql-sha256 <shasum -a 256 of this file> \
--     --authorization-reference <owner approval> --execute-authorized-correction
-- Proposal: artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/pp_toi_recovery_compatibility_differences_20261016.csv
-- Proposal SHA-256: 4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631
-- Scope: 32 nhl.skater_game_logs_raw.pp_toi_minutes values in 29 games, reason
--   DUPLICATE_SHIFT_INTERVAL (period-bounded, deduplicated shift reconstruction).
-- Source hashes: each row's PBP, boxscore, and shiftchart SHA-256 are bound in
--   artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/game_receipts/.
-- team_id is the application's legacy namespace (LEGACY_TEAM_ID_TO_ABBR in
--   backend/nhl/scripts/recover_nhl_historical_pp_toi.py). Each pinned value
--   was read 2026-10-19 and names the official team shown in the row comment.
-- On 2026-10-19 all 32 keys held their original values (read-only check).
-- Direction: original -> corrected.
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
