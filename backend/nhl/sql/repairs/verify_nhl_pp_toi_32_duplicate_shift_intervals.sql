-- Read-only receipt for the 32-row PP-TOI correction. Run with the restricted
-- research login before and after execution; it changes nothing.
-- Expected before: original=32. Expected after a committed correction: corrected=32.
-- Proposal SHA-256: 4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631
BEGIN READ ONLY;
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
ROLLBACK;
