-- ROLLBACK ONLY after coordinated application rollback and separate approval.
-- Restores the former direct ingest UPDATE grants and removes safe-fill entry
-- points. This restores the pre-migration exposure and is not a protection fix.
BEGIN;

REVOKE EXECUTE ON FUNCTION nhl.promote_skater_game_log_protected_fields(date)
  FROM proppadia_nhl_ingest;
REVOKE EXECUTE ON FUNCTION nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)
  FROM proppadia_nhl_ingest;
DROP FUNCTION nhl.promote_skater_game_log_protected_fields(date);
DROP FUNCTION nhl.fill_pp_toi_minutes_from_shift_overlap(bigint);

GRANT UPDATE (team_id, opponent_id, is_home, game_date, shots_on_goal,
              shot_attempts, toi_minutes, pp_toi_minutes, blocks, hits,
              fenwick_for, missed_shots, blocked_shots_taken, rebounds_for,
              takeaways, giveaways, penalties_drawn, penalties_taken,
              ev_shot_attempts, pp_shot_attempts, sh_shot_attempts,
              ev_sog, pp_sog, sh_sog, goals, assists, points)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;

COMMIT;
