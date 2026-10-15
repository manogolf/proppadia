# NHL Points consideration funnel V1 freeze

## Frozen contract

`NHL_POINTS_CONSIDERATION_FUNNEL_V1` is `FROZEN_FOR_PROSPECTIVE_VALIDATION` (fingerprint `15763cff55314952489b7221eb20237cb2ed749cbe0b95891377c2c1338e99c4`). Discovery is finalized HGB Points × retained market evidence from 2026-10-10 through 2026-10-13. Prospective validation starts 2026-10-14. No Oct. 10–13 outcomes were used to tune or alter the frozen rules.

- **Stage 1 — `RESEARCH_EXCLUDE_NEGATIVE_EDGE`:** exclude iff selected-side production edge < 0. The edge is selected-side model probability minus raw American implied probability from that side’s separately aggregated median attachment price; this includes vig. Missing edge is `EDGE_UNAVAILABLE` and is retained.
- **Stage 2 — `RESEARCH_EXCLUDE_EXTREME_MODEL_MARKET_GAP`:** among Stage 1 survivors, exclude iff signed selected-side model probability minus selected-side no-vig market probability > 0.15. This is strict and positive-only.
- **Stage 3 — `PROVISIONAL_STAGE_3_OPPOSITE_SIDE_DISAGREEMENT`:** among Stage 2 survivors, exclude iff model side and no-vig market favorite differ. It remains provisional and evaluated separately.

Order is Stage 0 → Stage 1 → Stage 2 → Stage 3. Each row has only one first-exclusion stage; overlapping later flags are diagnostics and never double-count removals. These are `RESEARCH_CONSIDERATION_EXCLUSIONS`, with no effect on prediction generation, immutable predictions, scoring, exports, production authority, or grading. No inclusion criteria, candidate target, or Stage 4 was added.

## Frozen reference and first unseen date

Discovery 0.5 reference counts reproduce as 658 → 538 → 263 → 227. Oct. 14’s pre-outcome dry view remains bound to its prior artifact: 93 market-usable rows across lines, 43 survivors; at 0.5, 58 → 49 → 23 → 20. Its classification timestamp is 2026-10-14T19:29:25.340306Z.

Existing official reconciliation has no Oct. 14 package; the latest available final reconciliation is Oct. 13. Therefore `OCT14_PROSPECTIVE_VALIDATION = AWAITING_FINAL_OUTCOMES`. No outcome has been joined, and this is not a graded validation date.

The append-only ledger is initialized with all 93 pre-outcome classifications and blank outcomes. Discovery and prospective populations are separate; cumulative prospective grades begin at zero. At least six independent finalized HGB dates are required before a repeatability review, and six dates alone do not establish proof.

## Validation design

For each future finalized date, the schemas reserve counts removed/surviving at each stage, cohort wins/losses, expected/observed/excess losses, model and market Brier/log loss, calibration gap, Stage 2 loss/win removal and Stage 3 incremental separation. Stage 2 subgroup reporting (UNDER/OVER, same-side confidence disagreement, opposite-side) is diagnostic only. Stage 1 rationale remains `NONCOMPETITIVE_FOR_CONSIDERATION`, not a validated failure region. Any later possible regime is recorded only as `FUTURE_RESEARCH_CANDIDATE`; no same-ledger Stage 4 test.

Any rule, order, threshold, edge, signed/absolute gap, Stage 3, or missing-value change requires a new version such as V2 after an explicit research decision. There is no automatic production promotion.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Manual NHL.com review: not part of this funnel.
