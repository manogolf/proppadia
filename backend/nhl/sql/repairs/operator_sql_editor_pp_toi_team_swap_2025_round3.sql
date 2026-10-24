-- PREPARED ONLY. Owner authorization required. Paste the whole block into the
-- Supabase SQL editor for the production project and run it once.
-- One transaction: temporarily grants the correction role to postgres, switches
-- to it, verifies the effective role, runs the reviewed guarded UPDATE
-- (identical to repair_nhl_pp_toi_team_swap_2025_round3.sql), switches back, revokes
-- the role and verifies the revoke. Any failure aborts the transaction and
-- nothing is committed, including the grant.
-- Before running: run verify_nhl_pp_toi_team_swap_2025_round3.sql (expect original=36,
-- corrected=0). The final SELECT is the receipt (expect corrected=36, original=0).

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
    (2025020386::bigint, 8471214::bigint, 15::bigint, 0.26::numeric, 3.75::numeric),
    (2025020386, 8474590, 15, 0.26, 2.08),
    (2025020386, 8475166, 10, 0.08, 0.00),
    (2025020386, 8475171, 10, 0.08, 0.00),
    (2025020386, 8475714, 10, 0.08, 0.00),
    (2025020386, 8476853, 10, 0.08, 0.00),
    (2025020386, 8476872, 10, 0.08, 0.00),
    (2025020386, 8476880, 15, 0.26, 1.95),
    (2025020386, 8476931, 10, 0.08, 0.00),
    (2025020386, 8477541, 10, 0.08, 0.00),
    (2025020386, 8477845, 15, 0.26, 0.00),
    (2025020386, 8477947, 15, 0.26, 0.93),
    (2025020386, 8478057, 10, 0.08, 0.00),
    (2025020386, 8478440, 15, 0.26, 2.20),
    (2025020386, 8478462, 10, 0.08, 0.00),
    (2025020386, 8478463, 15, 0.26, 1.80),
    (2025020386, 8478904, 10, 0.08, 0.00),
    (2025020386, 8478911, 15, 0.26, 0.18),
    (2025020386, 8479318, 10, 0.08, 0.00),
    (2025020386, 8479345, 15, 0.26, 3.20),
    (2025020386, 8479442, 10, 0.08, 0.00),
    (2025020386, 8479520, 15, 0.26, 0.17),
    (2025020386, 8480796, 15, 0.26, 0.17),
    (2025020386, 8480873, 15, 0.26, 0.00),
    (2025020386, 8481122, 10, 0.08, 0.00),
    (2025020386, 8481580, 15, 0.26, 1.55),
    (2025020386, 8481582, 10, 0.08, 0.00),
    (2025020386, 8481656, 15, 0.26, 0.77),
    (2025020386, 8481711, 10, 0.08, 0.00),
    (2025020386, 8482088, 15, 0.26, 0.75),
    (2025020386, 8482148, 15, 0.26, 0.17),
    (2025020386, 8482259, 10, 0.08, 0.00),
    (2025020386, 8482720, 10, 0.08, 0.00),
    (2025020386, 8483573, 15, 0.26, 1.57),
    (2025020386, 8484158, 10, 0.08, 0.00),
    (2025020386, 8484186, 15, 0.26, 2.18)
) AS c(game_id, player_id, raw_team_id, from_value, to_value)
WHERE skater_game_logs_raw.game_id = c.game_id
  AND skater_game_logs_raw.player_id = c.player_id
  AND skater_game_logs_raw.team_id = c.raw_team_id
  AND skater_game_logs_raw.pp_toi_minutes = c.from_value
  -- All-or-nothing: every one of the 36 keys must currently hold its exact
  -- expected value under its pinned team, or the cast raises and the whole
  -- transaction rolls back.
  AND (SELECT CASE WHEN count(*) = 36 THEN TRUE
               ELSE ('PP_TOI_TEAM_SWAP_R3_GUARD_FAILED:matching_rows=' || count(*))::boolean END
       FROM nhl.skater_game_logs_raw g
       JOIN (VALUES
    (2025020386::bigint, 8471214::bigint, 15::bigint, 0.26::numeric, 3.75::numeric),
    (2025020386, 8474590, 15, 0.26, 2.08),
    (2025020386, 8475166, 10, 0.08, 0.00),
    (2025020386, 8475171, 10, 0.08, 0.00),
    (2025020386, 8475714, 10, 0.08, 0.00),
    (2025020386, 8476853, 10, 0.08, 0.00),
    (2025020386, 8476872, 10, 0.08, 0.00),
    (2025020386, 8476880, 15, 0.26, 1.95),
    (2025020386, 8476931, 10, 0.08, 0.00),
    (2025020386, 8477541, 10, 0.08, 0.00),
    (2025020386, 8477845, 15, 0.26, 0.00),
    (2025020386, 8477947, 15, 0.26, 0.93),
    (2025020386, 8478057, 10, 0.08, 0.00),
    (2025020386, 8478440, 15, 0.26, 2.20),
    (2025020386, 8478462, 10, 0.08, 0.00),
    (2025020386, 8478463, 15, 0.26, 1.80),
    (2025020386, 8478904, 10, 0.08, 0.00),
    (2025020386, 8478911, 15, 0.26, 0.18),
    (2025020386, 8479318, 10, 0.08, 0.00),
    (2025020386, 8479345, 15, 0.26, 3.20),
    (2025020386, 8479442, 10, 0.08, 0.00),
    (2025020386, 8479520, 15, 0.26, 0.17),
    (2025020386, 8480796, 15, 0.26, 0.17),
    (2025020386, 8480873, 15, 0.26, 0.00),
    (2025020386, 8481122, 10, 0.08, 0.00),
    (2025020386, 8481580, 15, 0.26, 1.55),
    (2025020386, 8481582, 10, 0.08, 0.00),
    (2025020386, 8481656, 15, 0.26, 0.77),
    (2025020386, 8481711, 10, 0.08, 0.00),
    (2025020386, 8482088, 15, 0.26, 0.75),
    (2025020386, 8482148, 15, 0.26, 0.17),
    (2025020386, 8482259, 10, 0.08, 0.00),
    (2025020386, 8482720, 10, 0.08, 0.00),
    (2025020386, 8483573, 15, 0.26, 1.57),
    (2025020386, 8484158, 10, 0.08, 0.00),
    (2025020386, 8484186, 15, 0.26, 2.18)
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
