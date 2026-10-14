# Production exposure decision tree

Source: `backend/nhl/scripts/score_sog_poisson_baseline.py` (`score_predictions`, SHA in `lineage_manifest.json`). For each row, convert to numeric, turn non-finite values into missing, then take the first nonmissing value:

1. `d10_toi_min_avg`
2. `d20_toi_min_avg`
3. `d5_toi_min_avg`
4. `szn_toi_per_game_5on5 + szn_toi_per_game_pp` (both must yield a finite sum)
5. `season_5on5_icetime_per_game / 60 + season_5on4_icetime_per_game / 60` (source fields are seconds/game)
6. no exposure: row is unscored; there is no default, roster-derived substitute, cap, or floor in this scorer.

Exposure must be >= 0. Rate independently coalesces D10 → D20 → D5; lambda = rate × selected minutes / 60. No position-specific adjustment. PP contributes only within the season 5v5+PP fallback; PK is not included. The 5v5+5v4 fallback uses seconds/game and does not include 4v5. The scorer writes `poisson_source=d10` only if D10 rate and D10 TOI are present, otherwise `fallback`; it does not record the exact fallback source.
