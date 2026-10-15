# NHL Points cross-season conditional feature-response audit

## Scope and frozen cohorts

The primary comparison is 47,239 2025 regular-season strict-prior holdout player-games against 1,320 officially final, participating 2026 player-games from Oct. 9–14. The 2026 captures contain 1,800 total identities, including 480 unresolved outcomes. The 714-row exact two-sided market intersection is secondary only.

2025 predicted/realized/residual: 0.459/0.460/+0.001. 2026: 0.317/0.466/+0.149. 2026 game-cluster bootstrap 95% CI for mean residual: [0.09194650734944135, 0.20685704735980573].

## Parity and feature construction

Frozen artifact SHA256 `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9` and exact ten-feature order match both retained paths; 2025 replay max absolute error 2.22e-16. The holdout is out of time: fitted seasons are 2023–24 and holdout is 2025. Official Points are goals + assists for final participants only. Reconciliation outcomes remain separate from pregame features.

Feature construction classifications are in `feature_construction_comparison.csv`. The established attempts history absence is the only confirmed missingness drift among the ten features. Existing one-feature zeroing evidence is retained in `attempts_zeroing_existing_evidence.csv`; it showed only a small average change, so it cannot account for the aggregate gap. Team D10 shots and player team SOG share use matching team/game bindings and strict-prior windows in the training and operational feature paths.

## Early-season and common-support controls

See `early_season_historical_control.csv` for fixed current-season games-prior bands. Residual gaps for bands with both seasons represented: {"0": 0.07, "1-2": 0.019, "3-5": 0.087, "6-10": 0.141}. Later 2025 is retained as a distinct maturity reference.

Outcome-blind one-to-one matching paired 30 rows per season using the eight pregame covariates and declared calipers. Matched residuals: 2025 -0.0949236401003815, 2026 0.32581774282607123; difference 0.42074138292645274. Balance before/after is in `common_support_balance.csv`.

## Conditional findings

Fixed 2025-derived feature bins, shared expected-count bands, seven bounded interactions, and team/opportunity contexts are delivered as CSVs. Separately computed residual partitions in `global_error_concentration.csv` sum within each dimension to the full 2026 residual and are not additive across dimensions. Market quote status appears only as a secondary descriptive split.

Goals/assists and zero/one/2+ scoring rates are in `scoring_outcome_decomposition.csv`. Supported within-player, comparable-state observations are in `within_player_comparison.csv` (585 players represented in both seasons; 431 caliper-supported pairs). Feature perturbations in `frozen_model_feature_sensitivity.csv` are sensitivity only, not causal attribution.

The existing 2025 market-like reweighting was outcome-blind, effective sample size 43386.23721394749, weighted residual -0.0018; it is secondary evidence and does not establish conditional stability.

## Root-cause decision

`NO_SINGLE_REPAIR_JUSTIFIED`. Attempts missingness is confirmed but has small estimated frozen-model effect. Early-season feature support, conditional response, team context, and player composition remain bounded descriptive hypotheses requiring prospective replication. No individual feature mechanism currently meets the requested measurable, conditional, isolated-repair and healthy-row protection criteria. Next study: preregister season-volume × recent-scoring × TOI strata over additional full-population slates, preserving an untouched low-residual reference group.

## Oct. 15 pending exposure

`PENDING_OUTCOME`; 547 immutable feature rows were used with no outcomes joined. Summary and feature rows are retained separately.

## Integrity

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. HGB retraining/calibration/authority changes: none. Funnel V1: unchanged.

## Cross-season conditional and source caveats

At the same expected-count band .30–.40, the 2026 residual exceeds 2025 by +0.188; the pattern appears across all shared expected-count bands, but the 1.00+ band has zero 2026 rows. This supports a changed conditional relationship in the observed six-slate sample, subject to game-cluster uncertainty and prospective replication.

Official goals, assists, and zero/one/2+ rates are close in the two full cohorts. The aggregate target mix therefore does not explain the +0.148 residual difference.

The 2026 team D10 shots mean is 14.91, versus 27.43 in 2025. Both code paths use the same units and strict-prior team-game aggregation. However, the immutable operational captures do not retain underlying per-team prior-game shot rows, so source-level reconciliation to official team shots is unresolved. `team_context_audit.json` records this limitation.

Attempts missingness is confirmed. The retained 2025 zeroing experiment moved mean prediction by -0.002, while the separately retained 2026 training-median replacement sensitivity is in `frozen_model_feature_sensitivity.csv` and is not a causal estimate. The mismatch is not attributed as the full 2026 gap.

The strict outcome-blind, raw-unit common-support procedure retained 30 pairs. Post-match balance status is `INADEQUATE_MULTIVARIATE_OVERLAP` (maximum absolute SMD 1.05; threshold 0.10), driven by limited team-context support. Treat its residual comparison as descriptive, not as a balanced estimate. Within-player pairs are one-to-one within player across seasons and enforce recent form, PPG, TOI, PP TOI, and expected-count calipers.

2026 capture deterministic replay status is `PASS` with maximum absolute prediction error 1.1102230246251565e-16. The shared-upstream review is bounded to the feature builders and immutable captures. Shot rate, attempts, TOI, and team shots depend on skater-log source coverage; attempts missingness is confirmed, while team context source completeness remains unresolved. No acquisition or database inspection was performed.
