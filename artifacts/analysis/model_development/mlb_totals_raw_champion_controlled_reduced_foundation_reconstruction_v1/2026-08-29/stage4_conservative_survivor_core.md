# Conservative survivor core

`CONSERVATIVE_CORE_RESULT = NEAR_CHAMPION`

Direct features (4): league_total, home_starter_prior_starts, strict_prior_total_run_factor, park_history_depth. The survivor contains six unique primitive information concepts: the factor's five plus home-starter history support. Direct `league_total` and `park_history_depth` reuse concepts already embedded in the factor, so counting term instances would give eight but would double-count two concepts. Aggregate MAE/CRPS are 3.579921/2.527619, versus RAW 3.587180/2.533314. Preservation `NEAR_COMPLETE`; temporal `STABLE`. No promotion is authorized.
