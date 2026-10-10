# NHL SOG d10 rate regression diagnostic

Window 2026-10-03 through 2026-10-08. Production poisson_baseline/baseline_v1. Population: 1582 unique settled player-games, matching the prior lambda study. Production prediction and official outcome hashes verified for each slate; all retained target d10 values match the supplied feature artifacts.

## Formula and contributor gate

Production SQL (`backend/nhl/sql/export_sog_denali_pregame.sql`) selects each player’s rows with game date strictly before slate date, orders by game date descending then game ID descending, and takes the latest 20. d10 is sum(SOG) for rn≤10 divided by sum(TOI) for rn≤10, multiplied by 60. It is a ratio of aggregate counts/exposure, not the mean of per-game rates. Null/zero TOI is converted to NULL for the exposure sum; SOG is independently coalesced from blank/null to zero. There is no season filter, so windows can cross season IDs. The rate query includes available player-game records regardless of game type; no partial-game exclusion is coded. No minimum of ten observations is imposed: rows with fewer prior games use the available set. Identity is player_id; target game is excluded by the strict date predicate.

Read-only DB reconstruction compared 1582 targets: exact matches 1582, mismatches 0, maximum absolute difference 3.55e-15. Reconstruction class: all `D10_CONTRIBUTORS_EXACTLY_RECONSTRUCTED`. Feature-file d10 equals production scoring rate; feature inputs’ historical receipts were not byte-certified at runtime, a provenance limit.

## High-tail result

Top decile d10 mean 10.503; next-game realized rate 8.775; residual -1.728. d10 residual worsens from low to high deciles, but is not strictly monotonic. In high d10 rows, d20 MAE is 4.690 vs d10 MAE 4.938, and d5 MAE 5.189. The spread slope of next-game rate residual on d10−d20 is -0.673, player-cluster 95% CI [-0.925, -0.402].

Stable-high rows (d10 top quintile with d5 and d20 within the data-derived tolerance) n=84, residual -1.482; disagreeing high-d10 rows n=233, residual -1.381. Top-2 concentration and one-game removal diagnostics are in their CSVs. Recency weighting, older five games, and d20 are descriptive only; no estimator is promoted.

Validation applies discovery thresholds unchanged (d10 cut 7.501; d10−d20 cut 0.813). Validation high-d10 residual: -1.241; high-spread residual: -0.940. See validation table for all strata.

The market, d5/d10/d20 metrics, within-player state, TOI stability, low-TOI sensitivity, count summaries, positions, home/away, team/opponent, team environment, and bounded exemplar data are in companion CSVs. Actual TOI and realized rate are postgame diagnostics. C/G exact paired rows were not available; no retrospective substitution was made.

Primary interpretation: **high d10 regression is real, with evidence that rate elevation versus d20 and within-player state contributes; concentration and exposure mechanisms are not established strongly enough to prescribe a production estimator.** Next direction: `STUDY_D10_TO_D20_SHRINKAGE` only as a research comparison, not an implemented model.

Model changed: NO. Calibration fitted: NO. Distribution changed: NO. Provider calls / paid credits / database mutations: 0 / 0 / 0.
