-- Remove only the column SELECT grant added by 20261019_mlb_research_game_info_read.sql.
-- First remove MLB_RESEARCH_DATABASE_URL from the mlb-slate-output and
-- mlb-book-upload recipes if it was added after deployment.
BEGIN;

REVOKE SELECT (game_id, game_date, home_team_abbr, away_team_abbr)
    ON mlb.game_info FROM proppadia_mlb_research_ro;

COMMIT;
