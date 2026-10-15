# NHL Points funnel — first prospective validation

Frozen contract: `NHL_POINTS_CONSIDERATION_FUNNEL_V1`; supplied fingerprint `eb0c7d140103edeefcb7680cce7bb5fb3ba1bd88d990cff1e53bf1dd689fe3a9`.
Prospective date: **2026-10-14**. Frozen rows: **93**; settled **91**; unresolved **2**.
Outcome binding: `OUTCOME_JOIN_BY_CANONICAL_KEY` on `(game_id, player_id, line)`. All 93 rows joined once with no duplicate or cross-key joins. Prediction identity hashes differ on all 93 rows; frozen prediction and market bindings were retained.

## Frozen 0.5 funnel reproduction

58 → 49 → 23 → 20 (Stage 0 → after Stage 1 → after Stage 2 → after Stage 3). Two of the 58 remained participation unresolved, so Stage 0 grading uses 56 settled rows.

## 0.5 cohorts

| Cohort | N | W-L | Expected losses | Observed losses | Excess losses | Brier | Log loss | Mean selected p | Realized win rate | Calibration gap |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Stage 0 raw | 56 | 36-20 | 15.887 | 20 | 4.113 | 0.2400 | 0.6776 | 0.7163 | 0.6429 | 0.0735 |
| Stage 1 excluded | 9 | 8-1 | 2.687 | 1 | -1.687 | 0.1247 | 0.4120 | 0.7014 | 0.8889 | -0.1875 |
| After Stage 1 | 47 | 28-19 | 13.199 | 19 | 5.801 | 0.2621 | 0.7284 | 0.7192 | 0.5957 | 0.1234 |
| Stage 2 excluded | 24 | 14-10 | 7.028 | 10 | 2.972 | 0.2805 | 0.7785 | 0.7072 | 0.5833 | 0.1238 |
| After Stage 2 | 23 | 14-9 | 6.171 | 9 | 2.829 | 0.2429 | 0.6761 | 0.7317 | 0.6087 | 0.1230 |
| Stage 3 excluded | 3 | 2-1 | 1.323 | 1 | -0.323 | 0.2636 | 0.7211 | 0.5589 | 0.6667 | -0.1078 |
| Final survivors | 20 | 12-8 | 4.848 | 8 | 3.152 | 0.2398 | 0.6694 | 0.7576 | 0.6000 | 0.1576 |

## Losses removed and excess-loss change

| Cohort | Rows | Wins removed | Losses removed | Expected losses | Observed losses | Excess losses |
|---|---:|---:|---:|---:|---:|---:|
| Stage 1 incremental | 9 | 8 | 1 | 2.687 | 1 | -1.687 |
| Stage 2 incremental | 24 | 14 | 10 | 7.028 | 10 | 2.972 |
| Stage 3 incremental | 3 | 2 | 1 | 1.323 | 1 | -0.323 |

| Stage | Survivor excess before | Survivor excess after | Excess removed |
|---|---:|---:|---:|
| Stage 1 incremental | 4.113 | 5.801 | -1.687 |
| Stage 2 incremental | 5.801 | 2.829 | 2.972 |
| Stage 3 incremental | 2.829 | 3.152 | -0.323 |

## Interpretation

Stage 1 excluded 9 settled rows (8-1) with -1.687 excess losses. The excluded rows performed better than their probabilities expected; survivor excess losses increased from 4.113 to 5.801. Stage 1 reduced candidate count while removing a better-than-expected cohort on this first date.
Stage 2 removed 24 settled rows (14 wins, 10 losses), with 2.972 excess losses. It removed 52.6% of the Stage 1 survivor losses versus 50.0% of their wins. This first date modestly supports the frozen screen's excess-loss reduction; one date is insufficient evidence for validation.
Stage 3 removed 3 rows (2 wins, 1 losses), excess -0.323; it mainly discards wins / does not reduce excess losses on this date.
Stage 2 diagnostics: HGB UNDER 24 rows / 14-10; HGB OVER 0 / 0-0; same-side 12; opposite-side 12.
Raw versus final survivor HGB calibration gap: 0.0735 vs 0.1576; Brier 0.2400 vs 0.2398; log loss 0.6776 vs 0.6694.
Market comparisons use exact common rows within each cohort:
- Stage 0 (N=56): HGB Brier/log loss/calibration gap 0.2400/0.6776/0.0735; market 0.2344/0.6598/0.0401.
- Final survivors (N=20): HGB 0.2398/0.6694/0.1576; market 0.2257/0.6433/0.0594.
No ROI claim. No screen is validated, disproven, permanent, or profitable. Thresholds unchanged; no inclusion rule or Stage 4 added.

## Other lines and all-line status

1.5: 31 settled rows, 25-6 (reported separately). 2.5: 4 settled rows, 4-0 (descriptive only).
Across all 93 rows: Stage 1 removals 12; Stage 2 35; Stage 3 3; survivors 43. Settled survivor record 29-14; expected losses 6.063, observed losses 14, excess losses 7.937.

## Oct. 15 pending, outcome-free

| Line | Market-usable | Stage 1 removed | Stage 2 removed | Stage 3 removed | Survivors |
|---|---:|---:|---:|---:|---:|
| 0.5 | 185 | 23 | 85 | 15 | 62 |
| 1.5 | 23 | 1 | 16 | 0 | 6 |
| 2.5 | 2 | 0 | 1 | 0 | 1 |

Prospective finalized dates: **1**. Repeatability gate: **not met; minimum 6 independent finalized HGB dates**. Discovery results remain separate.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0.
