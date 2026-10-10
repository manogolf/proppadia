# NHL d10 streak responsiveness, persistence, and snapback

## Scope and sources

Primary window: 2026-10-03 through 2026-10-08. Oct. 9 had no qualifying FINAL target slate in the retained source set at execution time. The analysis uses the same 1582 unique settled production player-games, authoritative production run bindings, and FINAL outcome packages as the verified d10 study. Production model: poisson_baseline / baseline_v1. The row-level prediction run IDs and hashes are retained in `player_rate_timeseries.csv`.

## Reconstruction and temporal design

Independent strict-prior database reconstruction matched d5, d10 and d20 for all 1582 target rows; max absolute d10 difference is 3.55e-15. All contributing-game rows precede their target date. Consecutive observations mean consecutive target opportunities for the same player inside the six-day study window, not necessarily every NHL game played by that player. This creates 962 within-window transitions. State and streak thresholds were calculated on Oct. 3–5 only and held fixed on Oct. 6–8. Hot/cold event labels use realized outcomes retrospectively and are diagnostics, never pregame classifications.

Nearness tolerances (discovery 60th percentiles): |d5−d10| ≤ 1.112; |d10−d20| ≤ 0.851. High/low d10 cutoffs (discovery 80th/20th percentiles): 7.501/3.306. No threshold was tuned on validation.

## Continuation and snapback

High d10 state count: 202; low d10 state count: 177. The next target observation remains above the high cutoff in 82.7% of high states and falls below it in 17.3%. Mean high-state target-game rate residual is -1.278; low-state residual is 1.103. Rising-confirmed n=54, residual=-1.543; falling-confirmed n=36, residual=0.220. A d5-confirmed high state has n=112, residual=-1.386; a cooling high state has n=90, residual=-1.145. Their 1.5 Over accuracies are 68.8% and 67.8%. d5 confirmation does not lower the observed regression in this sample; see discovery/validation splits and clustered intervals.

The d20 anchor persistence fraction has median 0.497 over 384 meaningful-spread observations. Values below 0 indicate realized target rate below d20; values between 0 and 1 indicate partial persistence; values above 1 indicate acceleration beyond d10. Read robust distributions and d5 splits in `persistence_fraction.csv`. Of rows with d10 materially above d20, d10 declines at the next target in 44.3%; when materially below d20, it rises in 36.3%. Hot and cold residuals are -1.278 and 1.103; their sum is -0.175 (cluster interval in `cluster_uncertainty.csv`), consistent with roughly symmetric regression/rebound in this window.

## Streak length and window mechanics

Retrospective hot sequences: 527 (including separate d10-rising runs); cold outcome sequences: 525. A one-game hot spike had mean d10 error 5.20; two-game and 3–4-game hot bursts had errors 4.71 and 5.14, so the short sample gives no clean persistence-length cutoff. Across transitions, realized rate was already falling while d10 still rose in 127 cases. Sequence trajectories, including T0 through later states, are retained in the hot/cold CSVs. Recovered exact d10 contributors allow 962 adjacent-observation window comparisons. The symmetric numerator/denominator decomposition reconciles exactly to each d10 change. Among the 101 largest discovery-defined d10 declines, 36.6% had a top-5%-rate contributor exit. This indicates extreme exits explain a minority of sharp snapbacks; window entry/exit and denominator contributions are detailed in the CSVs.

## Discovery and validation

Validation uses Oct. 6–8 with the Oct. 3–5 thresholds held fixed. High d10 validation n=52, mean residual=-1.694; low d10 validation residual is 0.633. The high d10 and d5-confirm/cooling residual patterns repeat in validation, with small state samples. Player-cluster bootstrap intervals for continuation, snapback magnitude, d5 state residuals, d20 persistence and hot/cold asymmetry are in `cluster_uncertainty.csv`.

## Interpretation

The strongest supported interpretation is **D10_RESPONSIVE_WITH_D20_ANCHOR**. d10 follows observed movement, but high states regress and the d5 split does not reliably distinguish persistence from snapback; d20 is a useful reference level, and extreme games leaving the window explain only part of sharp declines. The recommended next research direction is **STUDY_FIXED_D10_D20_BLEND**, as a research-only comparison. No coefficient or rule is proposed for production.

The market matched 36 d5-confirming and 29 cooling high-state rows. Market Brier score beat production in both groups, but neither market nor production probabilities ranked the groups in the direction of the observed Over rates; see `market_state_analysis.csv`. Exact paired C/G shadow predictions were unavailable. This short chronological sample and target-opportunity sampling limit claims about streak duration and lead/lag timing.

Production predictions generated: NO. Shrinkage implemented: NO. Model changed: NO. Calibration fitted: NO. Distribution changed: NO. Provider calls: 0. Paid credits: 0. Database mutations: 0 (source data was queried read-only for the prior reconstruction).
