BEGIN;

CREATE TABLE IF NOT EXISTS nhl.user_props (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    prediction_timestamp timestamptz NOT NULL DEFAULT NOW(),
    game_id bigint NOT NULL,
    game_date date,
    player_id bigint NOT NULL REFERENCES nhl.players(player_id),
    player_name text,
    team text,
    team_id bigint REFERENCES nhl.teams(team_id),
    opponent_id bigint REFERENCES nhl.teams(team_id),
    prop_type text NOT NULL,
    prop_value numeric(6,2) NOT NULL,
    over_under text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    outcome text,
    prop_source text NOT NULL DEFAULT 'nhl_user_added',
    predicted_outcome text,
    confidence_score double precision,
    user_id text,
    CONSTRAINT nhl_user_props_over_under_check
        CHECK (over_under = ANY (ARRAY['over'::text, 'under'::text])),
    CONSTRAINT nhl_user_props_prop_type_check
        CHECK (prop_type = ANY (ARRAY['shots_on_goal'::text, 'goalie_saves'::text, 'points'::text]))
);

CREATE INDEX IF NOT EXISTS nhl_user_props_lookup_idx
    ON nhl.user_props (game_id, player_id, prop_type);

CREATE UNIQUE INDEX IF NOT EXISTS nhl_user_props_unique_prop_idx
    ON nhl.user_props (game_id, player_id, prop_type, over_under, prop_value, prop_source);

COMMIT;
