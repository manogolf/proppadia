# NHL Points Probability Ladder Coherence Audit V1

## Decision

Retained scorer disposition: `SAFE_ONLY_WITH_CANDIDATE_EXCLUSION`.  
`POINTS_IMMUTABLE_SHADOW_PATH = PROCEED_WITH_EXPLICIT_FAIL_CLOSED_GATE`.

The crossings are real full-precision model outputs, not display rounding, side inversion, row mismatch, or a join defect. They arise because the scorer runs three independent line-specific balanced logistic classifiers, each with its own fitted scaler and coefficients, without a shared count distribution or cross-line constraint. A reconciliation would change the meaning of at least one frozen classifier and is not authorized or automatically recommended.

## Exact definition

For integer player points `X`, the fields mean `p_over_0_5=P(X>=1)`, `p_over_1_5=P(X>=2)`, and `p_over_2_5=P(X>=3)`. A coherent ladder requires:

`p_over_0_5 >= p_over_1_5 >= p_over_2_5`.

A raw adjacent violation is `max(0,p_over_1_5-p_over_0_5)` or `max(0,p_over_2_5-p_over_1_5)`. The retained output contains 222 non-monotonic player-games out of 310 (71.613%). Material is explicitly defined as at least 0.01 probability (1 percentage point): 213/310 (68.710%). Large is at least 5 percentage points and severe at least 10 percentage points.

The 0.5→1.5 pair crosses in 78 ladders, with median positive crossing 0.019122 and maximum 0.037143. The 1.5→2.5 pair crosses in 222 ladders, with median 0.103978 and maximum 0.270405. Full quantiles and threshold counts are in the violation summary.

## Identity and construction findings

- The diagnostic replay used the same `(player_id,game_id)`, one 310-row input CSV, the same 15 feature values per player-game, one scorer invocation, Over orientation, and line interpretation. It reproduced the retained file byte-for-byte. The original retained file carries no run ID or parent-input hash, so its original run binding is not independently provable. The three probabilities do **not** share one fitted model: each line loads a different joblib.
- Historical database rows share player-game identity and generic `phoenix/phoenix_v2` labels. A common load timestamp is not a run ID; exact historical input snapshots and feature values were not retained and are therefore unprovable.
- All three artifact feature-name lists are identical. Differences are the fitted scaler state, coefficients, intercept, and binary target threshold. `classes_=[0,1]` for each; the scorer emits class-1 as `prob_over`.
- No rounding is applied before persistence. All 222 retained raw crossings remain visible at the UI's 0.1-percentage-point precision. Across stored history, 41,946 ladders cross in raw probabilities and 41,894 still cross after display rounding; rounding hides 52 crossings and creates none.

## Historical severity

The recoverable database population contains 54,918 complete player-game ladders from 2025-10-07 through 2026-04-16. Any crossing affects 41,946/54,918 (76.379%); material crossings affect 40,745/54,918 (74.192%). Affected date counts and all magnitude quantiles are retained in the summary CSV. Historical exact run/input identity is not recoverable, so these figures characterize stored outputs rather than certify original run packages.

## Decision impact

Contradictory favored-side ladders occur when a lower line favors Under while the next higher line favors Over: 27/310 in the retained output and 4,163/54,918 in stored history. The most severe retained ordering is player 8479977 in game 2025021309: `0.536566 >= 0.441354 < 0.711759`, a 27.0405-point 1.5→2.5 crossing. The historical maximum is player 8480876 in game 2025021242 on 2026-04-07: `0.491215 >= 0.359175 < 0.758643`, a 39.9467-point crossing. Even when both lines favor the same side, a higher-line Over probability exceeding a lower-line Over probability can inflate a line-local gap or EV and change rank/cap selection.

No governed Points Wilson, EV, gap, price, ranking, cap, candidate, or upload ledger exists. The current research surface uses line-local model-minus-market edge and price bounds. From 48 overwrite-capable archived market files, 5,633 player-games had a reconstructable playable max-edge choice; 5,354 came from non-monotonic ladders, 5,290 from material ladders, and 1,705 selected a crossing higher line. Those 1,705 choices are logically dominated under the model-event ordering because Over at the lower threshold is an event superset, although different prices mean they are not automatically economically dominated bets. Zero meet the stricter recoverable bet test: a lower Over line was easier and had the same or better archived decimal payout. These are reconstructions, not historical candidate or upload records.

Wilson/confidence gates and price caps do not restore order. EV/gap rules can pass an inflated crossing; ranking can prefer it; per-player/game/slate caps can then suppress a coherent alternative. The issue therefore can change actionable selection and is not presentation-only.

## Historical proper scoring by ladder state

Only rows with official participant outcomes are scored; missing/nonparticipant rows are excluded. The database supplies zero outcome overlaps for the season-2025 prediction population. Repository evidence supports only 8 retained official GameCenter boxscores on 2025-10-26, yielding 180 player-game outcomes. This is enough to calculate descriptive proper scores but not enough for a broad or temporal model-quality conclusion. Under is the exact probability/outcome complement of Over, so Brier and log loss are mathematically identical for the two orientations; both sides are retained in the detailed CSV.

| Ladder state | Line | Participant rows | Brier | Log loss | ECE-10 |
|---|---:|---:|---:|---:|---:|
| COHERENT | 0.5 | 6 | 0.168875 | 0.525075 | 0.245637 |
| COHERENT | 1.5 | 6 | 0.102818 | 0.380390 | 0.312574 |
| COHERENT | 2.5 | 6 | 0.082778 | 0.328982 | 0.275221 |
| NON_MONOTONIC_ANY | 0.5 | 174 | 0.320000 | 0.883939 | 0.326036 |
| NON_MONOTONIC_ANY | 1.5 | 174 | 0.524673 | 1.533705 | 0.665072 |
| NON_MONOTONIC_ANY | 2.5 | 174 | 0.710290 | 2.517844 | 0.815706 |
| NON_MONOTONIC_MATERIAL_GE_1PP | 0.5 | 173 | 0.321064 | 0.886391 | 0.325790 |
| NON_MONOTONIC_MATERIAL_GE_1PP | 1.5 | 173 | 0.527245 | 1.540651 | 0.667284 |
| NON_MONOTONIC_MATERIAL_GE_1PP | 2.5 | 173 | 0.713919 | 2.530442 | 0.818761 |

These are descriptive scoring comparisons, not betting-edge evidence. State membership is itself model-output-defined and should not be interpreted causally.

## Safe fail-closed disposition

A simple preseason gate can preserve coherent predictions without altering the scorer: require one unique row at each of 0.5/1.5/2.5, calculate crossings from raw unrounded `p_over`, and exclude the **entire player-game ladder** from candidate, upload, and execution eligibility when the maximum adjacent crossing is at least 0.01. Retain the untouched probabilities and reason code for diagnostic observation. Missing/duplicate ladders also fail closed. Sub-1-point crossings remain observation-only and explicitly flagged.

This is exclusion, not monotonic repair. It does not sort, clip, recalibrate, or reinterpret the frozen models. The 1-point threshold is an explicit operational tolerance, not an optimized performance threshold.

## Next task

Proceed with exactly `NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1_WITH_EXPLICIT_LADDER_COHERENCE_GATE`. The runner must preserve raw probabilities, implement the fail-closed rule above before any candidate/upload output, and keep all outputs observation-only. Goalie Saves remains `NOT_READY_FOR_PRESEASON_BURN_IN`; no goalie-source work is unlocked.
