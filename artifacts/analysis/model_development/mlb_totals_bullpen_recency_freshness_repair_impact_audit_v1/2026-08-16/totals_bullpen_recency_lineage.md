# Totals bullpen recency lineage

Official final MLB live feeds are retained by the completed-slate/player-stat recovery stage under `artifacts/analysis/mlb/player_stats_completeness/<date>/game_<game_pk>/sources/`. The daily wrapper runs that recovery and final-integrity check before the totals daily hook. `live_context_bridge_v1._historical_context()` previously built team relief history only from the frozen feature-spine boxscores, whose last date is 2026-08-05. `_bullpen()` then selected strict-prior appearances and constructed the feature row consumed by the frozen scorer.

The repaired bridge keeps the frozen spine as its base and deterministically supplements it with one content-consistent retained official final feed per later game. Duplicate retained sources must normalize to the same relief record or the load fails. New prediction contexts retain cutoff, last team-game date, source hash, acquisition timestamp manifest, and `BULLPEN_RECENCY_FRESHNESS_INVARIANT_V1`.

## Exact semantics

- Burden = sum of official reliever outs from games with `official_date < target_date` and within the prior three calendar days, divided by 3 outs/inning.
- Starters (`gamesStarted > 0`) are excluded. Extra-inning reliever outs are included.
- Only official `Final` games qualify. Postponed games contribute nothing until completed; a suspended/resumed game is governed by the official date in its final MLB feed.
- Team identity is the official numeric MLB team ID. The frozen date-level cutoff excludes all same-date games, so neither doubleheader game can leak into the other.
- Numerical zero is valid only when current strict-prior source coverage is established and the governed team has zero relief outs in the window. Missing or old coverage is not zero.

`STALE_HISTORY_MUST_NOT_BE_INTERPRETED_AS_ZERO_BURDEN`
