-- Read-only receipt for round 3 of the PP-TOI team-swap correction. Run with the
-- restricted research login before and after execution; it changes nothing.
-- Expected before: original=36, corrected=0. After a committed correction: corrected=36, original=0.
-- Proposal SHA-256: 1abb26a5ae4e20bd835a9e74d0391824e1d5da078e8f8f06a03f723b44ff5a6e
BEGIN READ ONLY;
WITH k(game_id, player_id, original_value, corrected_value) AS (VALUES
    (2025020386::bigint, 8471214::bigint, 0.26::numeric, 3.75::numeric),
    (2025020386, 8474590, 0.26, 2.08),
    (2025020386, 8475166, 0.08, 0.00),
    (2025020386, 8475171, 0.08, 0.00),
    (2025020386, 8475714, 0.08, 0.00),
    (2025020386, 8476853, 0.08, 0.00),
    (2025020386, 8476872, 0.08, 0.00),
    (2025020386, 8476880, 0.26, 1.95),
    (2025020386, 8476931, 0.08, 0.00),
    (2025020386, 8477541, 0.08, 0.00),
    (2025020386, 8477845, 0.26, 0.00),
    (2025020386, 8477947, 0.26, 0.93),
    (2025020386, 8478057, 0.08, 0.00),
    (2025020386, 8478440, 0.26, 2.20),
    (2025020386, 8478462, 0.08, 0.00),
    (2025020386, 8478463, 0.26, 1.80),
    (2025020386, 8478904, 0.08, 0.00),
    (2025020386, 8478911, 0.26, 0.18),
    (2025020386, 8479318, 0.08, 0.00),
    (2025020386, 8479345, 0.26, 3.20),
    (2025020386, 8479442, 0.08, 0.00),
    (2025020386, 8479520, 0.26, 0.17),
    (2025020386, 8480796, 0.26, 0.17),
    (2025020386, 8480873, 0.26, 0.00),
    (2025020386, 8481122, 0.08, 0.00),
    (2025020386, 8481580, 0.26, 1.55),
    (2025020386, 8481582, 0.08, 0.00),
    (2025020386, 8481656, 0.26, 0.77),
    (2025020386, 8481711, 0.08, 0.00),
    (2025020386, 8482088, 0.26, 0.75),
    (2025020386, 8482148, 0.26, 0.17),
    (2025020386, 8482259, 0.08, 0.00),
    (2025020386, 8482720, 0.08, 0.00),
    (2025020386, 8483573, 0.26, 1.57),
    (2025020386, 8484158, 0.08, 0.00),
    (2025020386, 8484186, 0.26, 2.18)
)
SELECT now() AS checked_at_utc, current_user AS checked_as,
       count(*) FILTER (WHERE r.pp_toi_minutes = k.original_value AND k.original_value <> k.corrected_value) AS original,
       count(*) FILTER (WHERE r.pp_toi_minutes = k.corrected_value) AS corrected,
       count(*) FILTER (WHERE r.player_id IS NULL) AS missing,
       count(*) FILTER (WHERE r.player_id IS NOT NULL AND r.pp_toi_minutes IS DISTINCT FROM k.original_value
                          AND r.pp_toi_minutes IS DISTINCT FROM k.corrected_value) AS other
FROM k LEFT JOIN nhl.skater_game_logs_raw r USING (game_id, player_id);
COMMIT;
