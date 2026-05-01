-- Proposed foundation table for Retrosheet-backed historical pitcher game logs.
-- Not applied by automation yet.
--
-- Source roles:
-- - Retrosheet: historical backbone for pitcher game logs.
-- - Chadwick Register: ID bridge from Retrosheet player IDs to MLBAM IDs.
-- - MLB Stats API: live/current-season source, not the only historical backfill source.

CREATE TABLE IF NOT EXISTS mlb.pitcher_game_logs_historical (
    game_date date NOT NULL,
    game_id_retrosheet text NOT NULL,
    pitcher_retrosheet_id text NOT NULL,
    pitcher_mlbam_id bigint,
    player_name text,
    team text,
    opponent text,
    is_starter boolean NOT NULL DEFAULT false,
    innings_pitched numeric(5,2),
    outs_recorded integer,
    strikeouts integer,
    walks integer,
    hits_allowed integer,
    earned_runs integer,
    runs_allowed integer,
    home_runs_allowed integer,
    batters_faced integer,
    game_finished boolean,
    source text NOT NULL DEFAULT 'retrosheet',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pitcher_game_logs_historical_pk
        PRIMARY KEY (game_id_retrosheet, pitcher_retrosheet_id),
    CONSTRAINT pitcher_game_logs_historical_source_chk
        CHECK (source IN ('retrosheet', 'mlb_api', 'manual')),
    CONSTRAINT pitcher_game_logs_historical_outs_nonnegative_chk
        CHECK (outs_recorded IS NULL OR outs_recorded >= 0)
);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_hist_date
ON mlb.pitcher_game_logs_historical (game_date DESC);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_hist_mlbam_date
ON mlb.pitcher_game_logs_historical (pitcher_mlbam_id, game_date DESC)
WHERE pitcher_mlbam_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_hist_retro_date
ON mlb.pitcher_game_logs_historical (pitcher_retrosheet_id, game_date DESC);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_hist_team_date
ON mlb.pitcher_game_logs_historical (team, game_date DESC);
