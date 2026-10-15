# NHL Points HGB confidence root-cause study

## Gate and model contract

The retained frozen artifact reproduced across 22 immutable 2026 captures (6,698 rows); maximum numeric difference over expected count and three Poisson probabilities was 2.22e-16, below the 1e-12 parity tolerance. Attribution proceeded only after this passed.

The frozen artifact hash is `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`. It is a `HistGradientBoostingRegressor` trained with Poisson loss on untransformed official goals + assists, with a `StandardScaler` and the ten ordered CORE features. Missing/nonfinite features are coerced and zero-filled before scaling. `predict()` yields expected points/count; it is floored at 1e-8, then converted by Poisson survival (`P(Over 0.5)=1-exp(-mu)`, Under is its complement). No additional count transformation or calibration is applied.

## 2026 diagnosis

On the 714 settled 0.5 rows: 416–298; selected expectation 70.1% vs 58.3% realized; expected-count residual (realized minus predicted) +0.232 points/player-game. Negative Edge: n=129, residual +0.016; positive Edge: n=585, residual +0.280; Edge >=10% residual +0.341; Edge >=20% residual +0.573.

**The most concrete anomaly is the attempt-rate feature:** `attempts_d10_per60` is exactly zero for all 711 nonmissing settled rows, compared with a training median of 8.33; all 714 operational rows fall outside the training 1st–99th percentile range. This is a reproducible operational feature-support problem and a plausible contributor to low counts, not yet proof of causal responsibility. Team D10 shots-for is also shifted (median 15.3 versus 29.3 training); mean PP TOI is shifted high (median 2.31 versus 0.314; 84.6% outside training 1st–99th percentiles). These distributions should be source-audited before model-form changes.

For comparison, 2025 strict-prior 0–5-game opening rows had only +0.020 mean count residual (n=5,228), with Over frequency 30.3% versus 29.5% predicted. That does not support an early-season cumulative-stat artifact as the sole explanation for the 2026 +0.232 residual. Season-prior totals do vary with games played in fixed-P/G counterfactuals, but those synthetic changes are not causal evidence.

The fixed cohort tables compare high-confidence correct and wrong rows within confidence bands, plus market opposition and same-side/high-confidence cohorts. Count residual tables lead the probability analysis. The 2026 sample is short; the anomaly should be treated as provisional pending source/history investigation.

## Historical cross-check

The retained strict-prior 2025 holdout has 47,239 rows and uses 2023–24 training (90,486 rows). Frozen-model reproduction max error is 2.22e-16. See `2025_early_mid_late.csv`, `2025_confidence_map.csv`, and `training_distribution_shift.csv`.

## Root-cause interpretation

Current classification: **MULTIFACTOR, provisional**. Highest-priority diagnosis is operational feature support/representation shift, especially shot attempts and team-shot context; season-opening cumulative quantities may contribute but do not explain the result alone. The frozen model has no market input. Feature replacements, matched cases, and tree summaries are diagnostic only; no causal claim is made. Tree node summaries do not assert sample-level path attribution. The retained 2025 source rows provide only approximate historical assist context; a full strict-prior role-trend history was not retained.

Up to three bounded candidate families and out-of-time tests are in `bounded_repair_candidates.csv`. **Calibration is not the proposed root-cause remedy; market probability remains diagnostic only and was not fed to HGB.** No retraining, promotion, filter, Funnel V1, or production change occurred.

## Oct. 15 pending

The quote-bound Oct. 15 map covers 547 immutable HGB identities; 188 have retained 0.5 market quotes. Counts by Under confidence, Edge, market opposition, current-season games prior, and suspected feature regime are in `oct15_pending_diagnostic.csv`; outcomes were not joined.

## Package contents

Contract, reproducibility evidence, outcome-blind cohort registry and outcome splits, expected-count residuals/bins, feature distributions/maps, accumulation counterfactuals, 2025 holdout maps, distribution shift, sensitivity, matched cases, tree/output summaries, root-cause scorecard, repair candidates, Oct. 15 pending exposure, lineage, and backlog are included.

Provider calls: 0; paid credits: 0; production DB mutations: 0.
