# Hits 0.5 sportsbook-independent full-board prospective shadow v1

Frozen on 2026-08-23 before the first eligible slate. Prospective evidence begins with 2026-08-24. Earlier process-only rows never count as prospective evidence.

## Authority and invariants

- Experiment: `MLB_HITS05_SPORTSBOOK_INDEPENDENT_FULL_BOARD_SHADOW_V1`.
- Exact model: `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb`.
- Exact artifact SHA-256: `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`.
- Target semantics: `P(official hits >= 1)` at the fixed proposition definition `Hits 0.5`; this is not a sportsbook line input.
- Frozen scoring: artifact LR/RF probabilities blended using embedded `max(AUC-0.5, 0)` weights, followed by the existing deterministic line-sensitivity transform with alpha `0.90`. No refit, recalibration, feature, threshold, or procedure change is authorized.
- Market availability, line, price, probability, selection, and outcome are forbidden from population admission, feature construction, and scoring. Market observations attach afterward in a separate immutable table.
- Shadow only. No public export, upload, Quick Card, candidate selection, wagering surface, promotion, or replacement path reads this ledger.

The implementation reuses the official governed lineup/starter identity capture created by the current nonmarket parent producer, then separately scores `models_out/latest/hits.joblib`. It never consumes the parent producer's challenger score or its 54-feature model. This is the earliest current replayable identity point with official hitter, game, lineup, scheduled-start, and opposing-starter lineage and no sportsbook denominator.

## Prospective evidence contract

Predictions and their full input vectors are append-only and are frozen strictly before scheduled start. Later official lineup releases may add previously missing player-game identities, but never replace or rescore an existing identity. A run after start excludes that game. Missing or failed slates are recorded and may not be reconstructed later using knowledge unavailable at the missed capture time.

Outcomes are accessed only by the independent grader after official exact-completeness validation. Market history and outcome history are independently append-only. The SQLite schema enforces prediction, feature, outcome, market, eligibility, rank, and run immutability with update/delete rejection triggers.

## Predeclared evaluation

Primary score is Brier. Secondary scores are log loss and 10-bin ECE. Reports include frozen daily quintiles and deciles, top-minus-bottom observed-rate separation, upper-decile support, game-date clustered bootstrap intervals (10,000 draws, seed 20260823), first/second temporal halves, leave-one-date-out, leave-one-decile-out, and calibration slope/intercept when at least 200 resolved rows and both classes exist.

Evaluated populations are: all technically eligible appearance-resolved rows; market-observed resolved rows; market-unobserved resolved rows; BetOnline nearest/available within 30 minutes resolved rows; and no-appearance rows reported separately. Sportsbook comparisons occur only within matched market-observed cohorts.

The fixed population baseline is the formal-review hit rate `0.575713564031321`. The hitter-shrunk baseline uses only strict-prior appearance-resolved player games: `(prior games with >=1 hit + 8 * population baseline) / (prior resolved games + 8)`.

No decision is permitted before all of: 20 completed game-date clusters, 5,000 appearance-resolved full-board predictions, and 500 appearance-resolved upper-decile predictions. The experiment continues beyond 20 clusters until all support requirements hold. Interim favorable results cannot certify or stop it.

After the horizon:

- `FULL_BOARD_INCREMENTAL_INFORMATION_REPRODUCED`: model Brier and log loss are both lower than both frozen baselines, and the date-cluster bootstrap 95% interval for hitter-baseline minus model Brier is strictly positive.
- `FULL_BOARD_ORDERING_ONLY`: the incremental rule fails, but the frozen top-decile observed rate exceeds the bottom-decile observed rate.
- `FULL_BOARD_NO_INCREMENTAL_INFORMATION`: neither rule holds.
- `FULL_BOARD_EVIDENCE_INSUFFICIENT`: the horizon or required estimation support is incomplete.

These categories are evidence descriptions only. Certification and public readiness remain separately governed.

## Preserved findings

This experiment does not reinterpret the completed review. It preserves `HITS05_20_CLUSTER_FORWARD_EVIDENCE_WEAK`, `HITS05_CERTIFICATION_STILL_DEFERRED`, `HITS05_PUBLIC_PREDICTION_NOT_READY`, and `INCREMENTAL_INFORMATION_NOT_REPRODUCED`.
