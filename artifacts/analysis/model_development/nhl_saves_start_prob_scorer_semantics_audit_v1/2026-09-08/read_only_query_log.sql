-- Read-only evidence queries executed 2026-09-08; no writes.
SELECT start_prob::text, COUNT(*) FROM nhl.training_features_goalie_saves_v2 GROUP BY start_prob ORDER BY start_prob NULLS FIRST;

SELECT COUNT(*) goalie_participation_rows, COUNT(*) FILTER (WHERE start_flag IS TRUE) starter_rows, COUNT(*) FILTER (WHERE start_flag IS FALSE) nonstarter_rows, COUNT(*) FILTER (WHERE start_flag IS NULL) null_flag_rows, COUNT(*) FILTER (WHERE start_prob IS NULL) null_start_prob_rows, MIN(start_prob), MAX(start_prob), MIN(created_at), MAX(created_at) FROM nhl.goalie_game_logs_raw WHERE game_date BETWEEN DATE '2023-10-03' AND DATE '2025-06-17' AND toi_minutes > 0;

-- Training CSV was joined locally to goalie_game_logs_raw on game_date/player/team/opponent; max TOI per game-team is diagnostic only, never starter authority.
