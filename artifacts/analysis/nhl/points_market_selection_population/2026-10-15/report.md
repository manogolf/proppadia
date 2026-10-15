# NHL Points market-selection population audit

## Finding

Classification: **POSSIBLE_CONTRIBUTOR**. The frozen HGB has a positive realized-minus-predicted residual in the full settled population, and that raw residual is larger among player-games with retained 0.5 quotes. However, the outcome-blind matched comparison is small (0.029 points), so the evidence does not support quote availability as the primary cause. The analysis is descriptive and does not establish causality.

## Population and quote denominators

- Full immutable 2026 capture population, Oct. 9–14: 1,800 player-games.
- Official participated/final outcomes: 1,320; unresolved: 480; final nonparticipants: 0.
- Across these dates, quoted means exact two-sided or one-sided retained quote evidence: 847 predicted, including partials separately; nonquoted/unmatched: 953.
- Direct reconciliation to the prior market-intersection study: Oct. 10–14 exact two-sided 0.5 quotes yield 714 settled identities. Existing map: 714 settled identities; identity overlap 714.
- Matched exact-two-sided market intersection: predicted 0.370, realized 0.602, residual 0.232. This reproduces the stated ~0.370 → ~0.602 gap. The full settled population is predicted 0.317, realized 0.466, residual 0.149.

| Settled group | N | Predicted | Realized | Residual | MAE | RMSE | Brier | Log loss |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| All | 1320 | 0.317 | 0.466 | 0.149 | 0.528 | 0.723 | 0.221 | 0.635 |
| Quoted (two-sided or partial) | 831 | 0.366 | 0.578 | 0.211 | 0.606 | 0.812 | 0.246 | 0.688 |
| Not quoted | 489 | 0.232 | 0.276 | 0.044 | 0.395 | 0.538 | 0.179 | 0.544 |

Over calibration gap is predicted P(Over 0.5) minus observed point-game rate: all -0.086; quoted -0.119; not quoted -0.029. Quote statuses are assigned from retained pregame prices/source quote counts, never from player identity. Exact two-sided: 821; one-sided partial: 26; attachment row with no valid quote: 953; no attachment identity: 0.

## Selection and residual diagnostics

- Matched outcome-blind exact coarsened strata: 86 strata / 111 weighted pairs; quoted-minus-nonquoted predicted 0.006, realized 0.035, residual 0.029. This is not an individual causal match.
- Propensity-style quote model: cross-validated AUC 0.900; this diagnostic uses HGB pregame features only and is not deployed.
- Raw residual difference (quoted minus nonquoted): 0.167. Exact date-level results are in `date_level_quote_selection.csv`; Oct. 13 is shown separately.
- 2025 quoted-like profile: 2,659 holdout rows under a frozen, outcome-blind median gate from 2026 quoted features; predicted 0.848, realized 0.973, residual 0.124. Outcome-blind pregame-feature reweighting of all 2025 holdout gives predicted 0.457, realized 0.455, residual -0.002; ESS 43386/47239.
- Under calibration gap (predicted P(Under) minus observed Under rate): quoted 0.119; not quoted 0.029. High-confidence bands are in `high_confidence_under_by_quote_status.csv`.
- Market same-side/opposition comparisons use exact two-sided quotes and are in `market_agreement_comparison.csv`. Market availability and odds were not HGB inputs.
- Same-player both-state sample: 123. Position was not inferred; team IDs are reported only where official outcomes bind, with team-level sample sizes in `team_quote_selection_diagnostics.csv`.
- Same-player similar-state caliper pairs (|expected|≤0.05, TOI≤2, last10 Points≤0.25): 104; quoted-minus-nonquoted realized difference 0.048. This is descriptive and does not support causal claims.

## Oct. 15 pending population

547 HGB player-games; 188 quoted (34.4%), 359 nonquoted. Mean expected points: quoted 0.419, nonquoted 0.231. High-confidence Under (P≥0.80): 19 quoted / 181 nonquoted. Status: **PENDING_OUTCOME**; no outcome join performed.

## Interpretation and constraints

The model was trained/evaluated on a broad historical player-game population but the cited 2026 0.5 market-intersection study evaluates the narrower exact-two-sided market-eligible subset. Its profile is stronger on several retained pregame usage/scoring fields. Market selection is a **possible contributor**, not established as a primary driver: the residual difference shrinks materially under outcome-blind strata, 2025 quoted-like rows also show positive residual, and 2025 feature reweighting remains centered. Future work may evaluate market-eligible performance separately and test omitted role/quality information; no such training, feature, filter, or calibration change is made here.

No market input entered HGB; no calibration/retraining/Funnel/production changes; provider calls 0; paid credits 0; production DB mutations 0. All captures, quote attachments, outcome files, and output hashes are pinned in `lineage_manifest.json`.

## Package

The CSV/JSON outputs include the complete all-player population, quote splits, feature comparisons, strength/expected-count bins, outcome and residual maps, matching, propensity, date/game coverage, same-player and market-agreement comparisons, 2025 profile/reweighting, denominator reconciliation, and pending Oct. 15 inventory. Backlog context is in `backlog_context.md`.
