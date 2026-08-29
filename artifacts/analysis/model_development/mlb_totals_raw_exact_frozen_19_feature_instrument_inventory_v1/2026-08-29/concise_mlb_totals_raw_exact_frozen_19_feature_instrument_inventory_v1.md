# MLB Totals RAW exact frozen feature-instrument inventory v1

## Contract reconciliation

The requested prior count of 19 is not the exact frozen RAW count. The authoritative artifact with hash `fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac` contains **22 direct features**. The 19-feature list is Totals C after removing `home_starter_prior_starts`, `away_starter_prior_starts`, and `park_history_depth` from direct location. This package keeps the requested path/filename for traceability and inventories all 22 RAW terms; it does not silently omit three terms.

## Identity

- Operational designation: `DIRECT_NEGATIVE_BINOMIAL_RAW_V1`; artifact identity/version: `DIRECT_NEGATIVE_BINOMIAL` / `DIRECT_NEGATIVE_BINOMIAL`.
- Canonical model hash: `fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac`.
- Artifact: `backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json`; byte SHA-256 `c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe` (distinct from canonical JSON hash).
- Training: 4,859 governed development games, 2023-03-30 through 2024-09-30; package `artifacts/analysis/model_development/mlb_totals_prediction_representative_rerun_v1/2026-08-06`; frozen 2026-08-06.
- Form: `REGULARIZED_POISSON_LOCATION_WITH_NEGATIVE_BINOMIAL_DISTRIBUTION`; target `OFFICIAL_FINAL_TOTAL_RUNS`; intercept `2.1965397963878517`; dispersion alpha `0.1294447997701300`.
- Preprocessing: every raw numeric feature is transformed by its embedded development-only StandardScaler; no feature-level log transform.

## Exact ordered RAW feature list

1. `league_total` -0.012652504836166
2. `home_offense` +0.014383004870291
3. `home_prevention` -0.013582351836678
4. `away_offense` +0.013564475744246
5. `away_prevention` -0.013586090275207
6. `home_starter_ra9` +0.013753975761016
7. `away_starter_ra9` +0.006799765020473
8. `home_starter_prior_starts` +0.012677233712015
9. `away_starter_prior_starts` +0.000826835652700
10. `home_expected_outs` -0.011065970207803
11. `away_expected_outs` +0.007419496809108
12. `home_workload_uncertainty_outs` +0.010362819278096
13. `away_workload_uncertainty_outs` +0.001432380112666
14. `home_bullpen_ra9` +0.015070549615556
15. `away_bullpen_ra9` +0.010854280711090
16. `home_bullpen_likely_available_reliever_count` -0.010844449128953
17. `away_bullpen_likely_available_reliever_count` +0.003558827664028
18. `home_bullpen_recent_innings_burden` -0.000834537374009
19. `away_bullpen_recent_innings_burden` +0.007154814495772
20. `strict_prior_total_run_factor` +0.043471584925325
21. `park_history_depth` -0.026813037900692
22. `game_number` +0.003839049993725

Because all terms are standardized, each coefficient is exactly the log-`mu` effect of a +1 training-standard-deviation move and is comparable as a standardized location effect—not as causal feature importance. Strongest positive: `strict_prior_total_run_factor` +0.043471584925325. Strongest negative: `park_history_depth` -0.026813037900692. Weakest absolute term: `away_starter_prior_starts` +0.000826835652700.

## Exact location and distribution equation

```text
log(mu) = 2.196539796387852
  -0.012652504836166 × ((league_total - 9.127133482909020) / 0.116223891978068)
  +0.014383004870291 × ((home_offense - 4.570371380309777) / 0.633010478360472)
  -0.013582351836678 × ((home_prevention - 4.558559917513907) / 0.642375959934065)
  +0.013564475744246 × ((away_offense - 4.559919885143778) / 0.613825371232372)
  -0.013586090275207 × ((away_prevention - 4.567582879203796) / 0.672491566445702)
  +0.013753975761016 × ((home_starter_ra9 - 4.621325646023378) / 2.315275758811759)
  +0.006799765020473 × ((away_starter_ra9 - 4.567044285917361) / 2.173952161191014)
  +0.012677233712015 × ((home_starter_prior_starts - 18.736777114632641) / 15.546598179262267)
  +0.000826835652700 × ((away_starter_prior_starts - 18.666803869108872) / 15.472110456847977)
  -0.011065970207803 × ((home_expected_outs - 15.678414157977553) / 3.019296568590848)
  +0.007419496809108 × ((away_expected_outs - 15.801231001162217) / 3.011490034385665)
  +0.010362819278096 × ((home_workload_uncertainty_outs - 2.566806965726491) / 1.566326169655698)
  +0.001432380112666 × ((away_workload_uncertainty_outs - 2.547419224747018) / 1.565439814937851)
  +0.015070549615556 × ((home_bullpen_ra9 - 4.543327985645738) / 0.747910191815763)
  +0.010854280711090 × ((away_bullpen_ra9 - 4.553736276372421) / 0.841100653792650)
  -0.010844449128953 × ((home_bullpen_likely_available_reliever_count - 9.422309117102284) / 3.097623440635831)
  +0.003558827664028 × ((away_bullpen_likely_available_reliever_count - 9.578925704877546) / 3.117064346527840)
  -0.000834537374009 × ((home_bullpen_recent_innings_burden - 9.605542978665019) / 3.328334099753410)
  +0.007154814495772 × ((away_bullpen_recent_innings_burden - 9.224806201550388) / 3.227513329346698)
  +0.043471584925325 × ((strict_prior_total_run_factor - 1.008024391556188) / 0.058277054946044)
  -0.026813037900692 × ((park_history_depth - 80.043836180284003) / 46.750716401230889)
  +0.003839049993725 × ((game_number - 1.013171434451533) / 0.114008542513367)

mu = exp(log(mu))
size = 1 / 0.129444799770130
prob = size / (size + mu)
```

`mu` is the unrounded stored RAW expected total and negative-binomial mean/location. The PMF uses integer support 0..30 with the 30+ tail folded into 30. For a total line, mass strictly above/below supplies over/under probability; equality at an integer line is push probability. RAW's point forecast is `mu`, not a median or mode.

## Count/confidence findings

Exactly three RAW direct terms qualify as history/sample-support confidence quantities: both starter prior-start counts and park history depth. All are type C: used upstream for fallback/shrinkage **and** directly in the RAW location. Totals C keeps their upstream governance but removes all three direct terms. Availability counts, workload quantities, uncertainty estimates, and `game_number` were explicitly inspected and are not sample-support confidence fields.

## Frozen structural evidence

- `park_history_depth`: training mean/median/max 80.044/80/161 versus prospective 291.270/299/302; 121/126 (96.032%) above training max. Its negative direct term remains frozen while the same depth also controls park shrinkage.
- `home_starter_prior_starts`: training mean/max 18.737/65 versus prospective 51.698/120; 33.333% above training max. `away_starter_prior_starts`: 18.667/66 versus 55.333/118; 38.095% above max. Both remain frozen direct terms and fallback gates; controlled within-pitcher evidence did not support an unbounded linear causal effect.
- Bullpen recency: the Aug 7-16 stale-artifact defect was operationally repaired for future scoring with retained official-final read-through and a fail-closed freshness invariant. Original frozen RAW rows remain immutable and partly contaminated; all RAW coefficients remain unchanged.
- Other existing checks: league-total center shifted downward; the negative RAW coefficient adds location in that state. Retained reliever-availability counts and game number stayed within documented training maxima, with no other drifting-count absorption detected.

## RAW versus Totals C

C hash `21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd` has 19 direct features. It adds no new direct fields. It removes only `home_starter_prior_starts`, `away_starter_prior_starts`, and `park_history_depth`; the remaining 19 constructions and scaler states are unchanged, but all retained location coefficients and intercept/dispersion were refit on the same 4,859-row development matrix. C continues to use the removed quantities upstream for starter fallback gating and park-factor shrinkage.

## `V1_INTERCEPT = +0.493550`

The value is the 2,120-row development actual-minus-RAW mean residual (`0.49354953700527243`) from the frozen 2025 development split in the 2026-08-12 standalone calibration repair. The additive intercept-only calibration was selected on a separate 563-game validation split. It is an **external run-space diagnostic**: `corrected_mu = raw_mu + 0.493550`, followed by rebuilding the distribution at unchanged alpha. It is not RAW artifact intercept `2.1965397963878517`, does not modify RAW's log equation, and current RAW scoring does not apply it. It is never applied to C because C's refit already relocates the distribution and existing frozen evidence says the RAW-only shift would likely overcorrect.

## Terminology

RAW is a trained operational private-shadow champion/control and point-forecast foundation. It is not a simple baseline: formal reviews compare it separately with leakage-safe prior-league-mean and team-shrunk baselines. The artifact is `RESEARCH_ONLY_NOT_AUTHORIZED`, so “production” is accurate only for the internal operational control path—not for a public-authorized Totals product.

## Offseason questions (not tested)

- `HYPOTHESIS_ONLY`: starter prior-start counts may proxy durability, experience, role stability, or data quality missing from the 19 retained direct inputs.
- `HYPOTHESIS_ONLY`: park history depth may proxy calendar/season progression or venue-era state that the regressed park factor does not capture.
- `HYPOTHESIS_ONLY`: multicollinearity may redistribute genuine starter/park signal when the three count terms are removed and all 19 retained coefficients are refit.
- `HYPOTHESIS_ONLY`: count terms may compensate for a missing latent feature or mismatch between historical shrinkage and live feature persistence.
- `HYPOTHESIS_ONLY`: RAW's retained structural bias may happen to offset other location misses over the current forward window, creating an unstable net advantage over C.

No model was fit, refit, mutated, promoted, or scored in this inventory; no EV/ROI or new performance experiment was run.
