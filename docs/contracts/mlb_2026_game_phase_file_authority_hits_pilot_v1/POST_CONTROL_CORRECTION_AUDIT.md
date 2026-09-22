# Post-Control Correction Audit

## Finding

For only `backend/mlb/scripts/evaluate_hits_model_candidates.py` over the frozen 2026-05-08 through 2026-08-02 retained cohort, the provisional classification is:

`RESULT_SAME_BUT_CONTROL_VIOLATED`

Commit `0ae9d87f4e7cd7a758e87862dcca6d579f1bcd13` is a post-control correction. It cannot be used as evidence that the implementation before that commit complied with the fail-closed source-type requirement.

## Preserved source history

| Event | Commit | Timestamp | Evidence |
|---|---|---|---|
| Fallback first entered repository | `d5a5a4f466123c658aa18b1b7cf3b43283c1f5be` | 2026-03-29T07:58:02-07:00 | Legacy recompute added `--require-regular-season` and the missing-to-`R` SQL predicate. |
| Evaluator introduced with fallback | `5b24d71143fe120c41f9d1b40bc60bbb5cfbf8de` | 2026-04-24T15:34:28-07:00 | New 448-line evaluator already contained the predicate. |
| Fail-closed phase contract introduced | `28ec66a2334cfa423a4939a1ae0b0cbe4a84ab24` | 2026-09-21T10:34:04-07:00 | Missing, unknown and conflicting authoritative types became explicit contract failures. |
| Canonical certification introduced | `01c610c4ed88c2a7fa814cce3dae6d3e25a5aa46` | 2026-09-21T15:43:03-07:00 | Deterministic phase tests and activation evidence added. |
| Defect identified in cutover design | `3aec4975adb2aa0753e51bd1714245243956376b` | 2026-09-21T20:53:06-07:00 | Evaluator classified as an observed contamination defect and first cutover candidate. |
| Post-control correction | `0ae9d87f4e7cd7a758e87862dcca6d579f1bcd13` | 2026-09-21T21:23:55-07:00 | Fallback removed from this evaluator; exact-gamePk file authority added. |

The evaluator did not exist before `5b24d711…`. Its introducing blob is `de3ede5896750e6b6b070e1179785b5d92b9039f`; the last pre-correction blob is `17e82e9aeda06df0251ba12a9a4dae1086639fb4`; and the corrected blob committed by `0ae9d87f…` is `1dc75e1b912c4840667e62877bc1594895551252`.

No commit was amended, reverted, squashed, or otherwise rewritten by this audit.

## Why the fallback entered

The first repository introduction is directly documented by the March 29 diff and comments:

- purpose: optional Spring Training exclusion for regular-season-only recomputes;
- compatibility constraint: use `to_jsonb(m)->>'game_type'` where the physical column might not exist;
- chosen behavior: `COALESCE(..., 'R')`, which treated absent or empty type as regular season.

The evaluator copied that behavior when created on April 24. Its generic commit message, `Commit all pending changes`, provides no evaluator-specific scientific rationale. The narrowest supported conclusion is that an earlier schema-compatibility shortcut propagated into the evaluator; intent beyond that is not evidenced.

## Why certification did not reject it

The September 21 certification was not repository-wide consumer enforcement:

1. `test_zero_date_based_phase_reconstruction` scanned only the phase contract, canonical phase helper, and offline proposal builder.
2. The matching dependency-free validator used the same three-file source scope.
3. Positional-insert and producer checks examined selected canonical/clean-room writers, not downstream evaluation consumers.
4. The activation README's blocker inventory named Full-board Hits and external normalized-game fallbacks but omitted this evaluator.
5. No dedicated evaluator phase-control test existed before `0ae9d87f…`, and repository tracing found no call site that would have caused another certification path to execute it.
6. The source-completion gate re-executed the canonical assertions and validated proposal population integrity; it did not add a full-repository missing-default scan.

Therefore the certification passed its declared checks while leaving this consumer outside the checked surface. That is a certification-coverage defect, not evidence that the consumer complied.

## Frozen cohort evidence

Input:

- path: `artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/frozen_hits_review_population.csv`;
- SHA-256: `7c94ead53af9669c1164e41fcd9714edd0656dc02d6550f0c773aa1a3c1147fd`;
- 7,564 rows;
- 651 distinct exact gamePks;
- dates 2026-05-08 through 2026-08-02;
- all 651 gamePks authoritatively classified `REGULAR_SEASON`.

Before and after cohort, probability/pick, outcome, and evaluation-metric hashes are identical. No gamePk is excluded or newly blocked. That supports `RESULT_SAME` for this cohort.

The control nevertheless admitted missing type as `R` before correction. Outcome equality in an all-regular cohort does not validate that behavior against preseason, postseason, special, missing, unknown, conflicting, absent, or stale identities. That supports `CONTROL_VIOLATED`.

## Non-extrapolation boundary

This audit makes no result claim about:

- other Hits consumers;
- prediction construction;
- Full-board Hits;
- external modeling splits;
- Moneyline;
- Totals;
- grading;
- agreement reporting;
- earlier seasons;
- untested date ranges.

Those remain separately auditable surfaces. The corrected interface and its synthetic edge tests are reusable controls, but they are not empirical outcome evidence for those surfaces.
