-- Remove only the role-specific policies and column SELECT grants added by
-- 20261017_mlb_research_source_read.sql. Apply before making later ACL changes.
BEGIN;

DROP POLICY IF EXISTS proppadia_mlb_research_select ON mlb.player_stats;
DROP POLICY IF EXISTS proppadia_mlb_research_select ON mlb.player_derived_stats;
DROP POLICY IF EXISTS proppadia_mlb_research_select ON mlb.player_ids;
DROP POLICY IF EXISTS proppadia_mlb_research_select ON public.mlb_team_map;

REVOKE SELECT (
    player_id, game_id, game_date, team, opponent, position, hits, hits_allowed,
    outs_recorded, is_starter, earned_runs, strikeouts_pitching, at_bats, plate_appearances
) ON mlb.player_stats FROM proppadia_mlb_research_ro;

REVOKE SELECT (
    player_id, game_id, game_date, team,
    d7_hits, d7_runs_scored, d7_rbis, d7_home_runs, d7_singles, d7_doubles,
    d7_triples, d7_walks, d7_strikeouts_batting, d7_stolen_bases, d7_total_bases,
    d7_hits_runs_rbis, d7_runs_rbis, d7_outs_recorded, d7_strikeouts_pitching,
    d7_walks_allowed, d7_earned_runs, d7_hits_allowed, d7_at_bats, d7_plate_appearances,
    d15_hits, d15_runs_scored, d15_rbis, d15_home_runs, d15_singles, d15_doubles,
    d15_triples, d15_walks, d15_strikeouts_batting, d15_stolen_bases, d15_total_bases,
    d15_hits_runs_rbis, d15_runs_rbis, d15_outs_recorded, d15_strikeouts_pitching,
    d15_walks_allowed, d15_earned_runs, d15_hits_allowed, d15_at_bats, d15_plate_appearances,
    d30_hits, d30_runs_scored, d30_rbis, d30_home_runs, d30_singles, d30_doubles,
    d30_triples, d30_walks, d30_strikeouts_batting, d30_stolen_bases, d30_total_bases,
    d30_hits_runs_rbis, d30_runs_rbis, d30_outs_recorded, d30_strikeouts_pitching,
    d30_walks_allowed, d30_earned_runs, d30_hits_allowed, d30_at_bats, d30_plate_appearances
) ON mlb.player_derived_stats FROM proppadia_mlb_research_ro;

REVOKE SELECT (player_id, player_name, team, team_id)
    ON mlb.player_ids FROM proppadia_mlb_research_ro;
REVOKE SELECT (player_id, game_id, team_id)
    ON mlb.player_team_by_game FROM proppadia_mlb_research_ro;
REVOKE SELECT (team_id, abbr)
    ON public.mlb_team_map FROM proppadia_mlb_research_ro;
REVOKE SELECT (game_date, game_id, player_id, prop_type, rolling_result_avg_7)
    ON mlb.model_training_props FROM proppadia_mlb_research_ro;

COMMIT;
