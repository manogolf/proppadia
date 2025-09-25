\echo 'Starting concurrent matview refreshes...'
SET statement_timeout = '0';

REFRESH MATERIALIZED VIEW CONCURRENTLY public.player_pitching_games;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.player_pitching_laststarts_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.pitcher_last3_agg;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.player_rolling_batting_agg;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.pitcher_rolling_per9;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.player_rolling_pitching_rates;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.player_rolling_agg_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.bvp_rollup_prior;
REFRESH MATERIALIZED VIEW CONCURRENTLY public.bvp_pairs;

\echo 'All concurrent refreshes completed.'
