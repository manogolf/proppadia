# park_history_depth lineage

- Source: official MLB schedule games in the governed totals feature spine, keyed by `venue_id`.
- Historical construction: for each target date, `park_history_depth = len(prior completed official games at the same venue with an earlier game date)`. All games on a target date are frozen before that date's outcomes enter history.
- Live construction: the bridge rebuilds the same venue history through the frozen historical spine's last completed game and exposes the resulting count to each prospective context. The Aug 6–15 prospective rows therefore use the retained through-Aug-5 state rather than outcomes from the prospective stream.
- Formula consuming depth for park shrinkage: `w = n / (n + 50)` and `strict_prior_total_run_factor = w * mean(prior adjusted total ratios) + (1 - w) * 1.0`.
- Grain/unit: one integer count per game/venue state; unit is prior official games at that venue.
- As-of rule: strict prior; current-game outcomes are excluded. The retained historical construction is date-strict.
- Season behavior: the governed population begins March 30, 2023 and carries venue history across 2024, 2025, and 2026 without an annual reset.
- Fallback: `LEAGUE_REGRESSED_SPARSE_PARK` below 20 games; absent venue state becomes depth 0 and `LEAGUE_PARK_FALLBACK` in the live bridge.
- Missingness: no missing depth in the 9,012 historical rows or 126 prospective rows examined.
- Preprocessing: direct numeric input, standardized by frozen development mean 80.043836180284003 and scale 46.750716401230889; no cap, log, saturation, season reset, or uncertainty-only gating.
- Frozen downstream equation: `log(mu) += -0.026813037900692 * ((park_history_depth - 80.043836180284003) / 46.750716401230889)`.
- Source code: `tmp/analysis/build_mlb_totals_feature_spine_v1.py` (historical construction), `backend/mlb/totals_predictions/live_context_bridge_v1.py` (live context), and `backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json` (frozen preprocessing/coefficient).

The count is intrinsically a sample-support/data-confidence quantity for the regressed park factor. Its separate admission to expected-run location turns increasing data volume into a directional run forecast.
