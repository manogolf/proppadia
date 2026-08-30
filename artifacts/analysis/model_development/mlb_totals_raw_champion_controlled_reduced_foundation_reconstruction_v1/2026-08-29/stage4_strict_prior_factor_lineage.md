# Strict-prior total-run-factor lineage

`STRICT_PRIOR_FACTOR_EQUIVALENCE = MACHINE_TOLERANCE`

The direct model has one `strict_prior_total_run_factor` term, but the factor is composite and contains five defensible primitive information concepts (the team-scoring concept is instantiated separately for the home and away teams).

```text
official game + venue identity
  + official final home/away/total runs from dates strictly before the target date
  -> expanding league total mean (8.6 before any history)
  -> expanding home-team and away-team runs-scored means (league_mean / 2 when absent)
  -> adjusted venue ratio = prior_game_final_total / max(expected_home + expected_away, 0.5)
  -> direct venue ratio = mean(all prior adjusted ratios at venue)
  + venue history depth n
  -> shrinkage weight = n / (n + 50)
  -> strict_prior_total_run_factor = weight * direct_venue_ratio + (1 - weight) * 1.0
```

- Cutoff: historical construction freezes every game on a date before admitting any outcome from that date. It never reads the target game's outcome.
- Window: expanding from the governed spine start (2023-03-30), not a trailing window.
- Normalization: no inner z-score; `StandardScaler` is applied later by the location model. Denominator floor is 0.5.
- Clipping: none. Shrinkage target is 1.0 and constant 50 is fixed.
- Fallback: no venue history gives direct ratio 1, depth 0, weight 0, factor 1; team history falls back to half the strict-prior league mean; initial league mean is 8.6.
- Inputs: no sportsbook, market, opponent-prevention, prediction, or evaluation-outcome information.
- Support role: `park_history_depth` enters factor construction even if omitted as a separate direct model term.
- Version: historical `MLB_TOTALS_FEATURE_SPINE_V1` advances date-by-date. Prospective `live_context_bridge_v1` uses the identical equation on a foundation frozen through 2026-08-05. The latter does not advance the park foundation during Aug. 17–28.
