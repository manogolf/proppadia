-- Proposed starter batters-faced landing table.
-- PROPOSED ONLY / NOT APPLIED.
--
-- Purpose:
-- - Store official, source-provenanced pitcher game-log BF facts.
-- - Initial write scope is starter-only MLB StatsAPI boxscore BF after dry-run gate acceptance.
-- - This table must not be used by production formulas, tiers, selectors, uploads, models, or grading
--   until a separate downstream adoption review is approved.
--
-- Do not apply this migration automatically.

CREATE TABLE IF NOT EXISTS mlb.pitcher_game_logs_bf (
    game_id bigint NOT NULL,
    game_date date NOT NULL,
    pitcher_mlbam_id bigint NOT NULL,
    pitcher_name text,
    team text,
    opponent text,
    is_home boolean,
    is_starter boolean NOT NULL DEFAULT true,

    outs_recorded integer,
    hits_allowed integer,
    walks_allowed integer,
    strikeouts_pitching integer,
    earned_runs integer,
    batters_faced integer NOT NULL,

    bf_source text NOT NULL,
    bf_source_priority integer NOT NULL,
    source_url text,
    source_payload_hash text,
    source_run_at timestamptz,

    validation_status text NOT NULL,
    warning_code text,
    reject_reason text,
    conflict_reason text,
    skip_reason text,
    validation_notes text,
    backfill_run_id text NOT NULL,

    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pitcher_game_logs_bf_pk
        PRIMARY KEY (game_id, pitcher_mlbam_id, bf_source),
    CONSTRAINT pitcher_game_logs_bf_source_chk
        CHECK (bf_source IN ('statsapi_boxscore', 'retrosheet_chadwick', 'manual_review')),
    CONSTRAINT pitcher_game_logs_bf_validation_status_chk
        CHECK (validation_status IN ('accepted', 'warning_accepted')),
    CONSTRAINT pitcher_game_logs_bf_starter_scope_chk
        CHECK (is_starter = true),
    CONSTRAINT pitcher_game_logs_bf_priority_positive_chk
        CHECK (bf_source_priority > 0),
    CONSTRAINT pitcher_game_logs_bf_bf_nonnegative_chk
        CHECK (batters_faced >= 0),
    CONSTRAINT pitcher_game_logs_bf_outs_nonnegative_chk
        CHECK (outs_recorded IS NULL OR outs_recorded >= 0),
    CONSTRAINT pitcher_game_logs_bf_hits_nonnegative_chk
        CHECK (hits_allowed IS NULL OR hits_allowed >= 0),
    CONSTRAINT pitcher_game_logs_bf_walks_nonnegative_chk
        CHECK (walks_allowed IS NULL OR walks_allowed >= 0),
    CONSTRAINT pitcher_game_logs_bf_strikeouts_nonnegative_chk
        CHECK (strikeouts_pitching IS NULL OR strikeouts_pitching >= 0),
    CONSTRAINT pitcher_game_logs_bf_earned_runs_nonnegative_chk
        CHECK (earned_runs IS NULL OR earned_runs >= 0),
    CONSTRAINT pitcher_game_logs_bf_warning_required_chk
        CHECK (validation_status <> 'warning_accepted' OR warning_code IS NOT NULL),
    CONSTRAINT pitcher_game_logs_bf_reject_not_written_chk
        CHECK (reject_reason IS NULL AND conflict_reason IS NULL AND skip_reason IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_bf_date
ON mlb.pitcher_game_logs_bf (game_date DESC);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_bf_pitcher_date
ON mlb.pitcher_game_logs_bf (pitcher_mlbam_id, game_date DESC);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_bf_team_date
ON mlb.pitcher_game_logs_bf (team, game_date DESC);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_bf_run_id
ON mlb.pitcher_game_logs_bf (backfill_run_id);

CREATE INDEX IF NOT EXISTS idx_pitcher_game_logs_bf_source_priority
ON mlb.pitcher_game_logs_bf (bf_source_priority, bf_source);
