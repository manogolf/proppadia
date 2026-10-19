-- Remove only the column SELECT grants added by 20261019_mlb_research_daily_check_read.sql.
-- First unset MLB_RESEARCH_DATABASE_URL on the nine reader recipes if it was added after deployment.
BEGIN;

REVOKE SELECT (id, prop_source, prop_value, line) ON mlb.model_training_props FROM proppadia_mlb_research_ro;
REVOKE SELECT (
    is_home, singles, doubles, triples, home_runs, total_bases, runs_scored, rbis,
    walks, strikeouts_batting, stolen_bases, pa_source, pa_backfilled_at,
    hit_by_pitch, sacrifice_flies, sacrifice_hits, catcher_interference
) ON mlb.player_stats FROM proppadia_mlb_research_ro;
REVOKE SELECT (game_time, home_team_id, away_team_id) ON mlb.game_info FROM proppadia_mlb_research_ro;
REVOKE SELECT (is_placeholder) ON mlb.player_ids FROM proppadia_mlb_research_ro;
REVOKE SELECT (lineup_slot) ON mlb.prop_features_precomputed FROM proppadia_mlb_research_ro;
REVOKE SELECT (slate_date, snapshot_ts) ON mlb.today_odds_book_rows FROM proppadia_mlb_research_ro;
REVOKE SELECT (game_date, last_snapshot_ts) ON mlb.today_market_snapshot FROM proppadia_mlb_research_ro;
REVOKE SELECT (
    game_date, game_id, player_id, player_name, team, opponent, is_home, prop_type, line, side,
    best_price, best_price_book, market_median, market_range, value_vs_market, open_price,
    latest_price, minutes_since_open, num_snapshots, price_change_from_open, book_count,
    price_dispersion, coverage_quality_label, coverage_quality_reason, timing_signal,
    timing_reason, streak_context_label, consistency_score, last_5_avg, last_10_avg,
    season_avg, hit_rate_last_5, hit_rate_last_10, hit_rate_season, streak_type,
    streak_count, baseline_delta, decision_label, decision_reason
) ON mlb.today_workspace_mlb FROM proppadia_mlb_research_ro;

COMMIT;
