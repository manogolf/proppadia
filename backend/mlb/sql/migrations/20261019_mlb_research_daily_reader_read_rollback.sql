-- Remove only the column SELECT grant added by 20261019_mlb_research_daily_reader_read.sql.
-- First remove MLB_RESEARCH_DATABASE_URL from the mlb-slate-output and
-- mlb-book-upload recipes and the starter skill/workload LaunchAgent if it
-- was added after deployment.
BEGIN;

REVOKE SELECT (game_id, game_date, home_team_abbr, away_team_abbr)
    ON mlb.game_info FROM proppadia_mlb_research_ro;
REVOKE SELECT (walks_allowed)
    ON mlb.player_stats FROM proppadia_mlb_research_ro;

COMMIT;
