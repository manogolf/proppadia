# MLB Regular-Season-Only Requirement Compliance and Impact Audit V1

Status: **READ-ONLY AUDIT COMPLETE; REALIZED BOUNDED COHORTS UNAFFECTED; REPOSITORY-WIDE IMPACT PARTIALLY UNPROVABLE**

## Overall finding

The earliest proven repository control is commit `7f22c7623ff5b94462749f51e29288c26e473e03` (2026-02-15). It added the Opening Day instruction to enable the regular-season-only stat-derived lock, implemented positive raw StatsAPI `gameType == "R"` selection, excluded missing type, and tested rejection of Spring Training and postseason types.

That requirement was not preserved uniformly. Of the 24 entries in the committed sidecar cutover map, this audit classifies 3 as compliant, 19 as violating, 0 as statically unprovable, and 2 as not applicable. These counts describe pre-correction control behavior, not realized contamination.

Every bounded 2026 row population for which complete exact-gamePk retained evidence was available joined to authoritative `R` / `REGULAR_SEASON` membership with 100% coverage. No proven bounded cohort contains a preseason, postseason, special, unknown, or authority-missing identity. Therefore no measured probability, pick, grading, market, ROI, or quality denominator changes in those bounded cohorts.

The repository-wide conclusion remains partial rather than globally clean. Complete exact-gamePk input manifests are absent for the operational stat-derived/model-training population, broad legacy research and qualification outputs, UBO5 downstream research surfaces, aggregate Ops Brief/daily-index inputs, BvP feature consumption, and feature-lineage consumption. Those surfaces are `UNPROVABLE_FROM_RETAINED_EVIDENCE`; they are not assumed regular from dates, model participation, market availability, or defaults.

## Preserved bounded finding

`evaluate_hits_model_candidates.py`, 2026-05-08 through 2026-08-02:

- 7,564 rows;
- 651 exact gamePks;
- all 651 authoritatively `R` / `REGULAR_SEASON`;
- before and after identity, probability, and outcome hashes identical;
- accuracy 57.218403%, AUC 0.5404937507, Brier 0.244818025934, and log loss 0.683024373973 unchanged;
- classification: `RESULT_SAME_BUT_CONTROL_VIOLATED`.

Commits `0ae9d87f4e7cd7a758e87862dcca6d579f1bcd13` and `0ef19ac4d2cf5d141853605abaa246a77397f652` are post-control correction and bounded audit evidence. They do not prove prior compliance and are not extrapolated to another Hits consumer, prediction construction, Full-board Hits, external splits, Moneyline, Totals, grading, agreement reporting, earlier seasons, or untested dates.

## Realized evidence summary

The machine-readable lane reconciliation records the exact row and game counts. Highlights are:

- Hits candidate frozen evaluation: 7,564 rows / 651 games, all regular.
- Full-board Hits: 18,592 eligibility observations / 394 games; 5,877 predictions / 358 games; 5,868 outcomes / 357 games, all regular.
- Read-only Moneyline snapshot: 633 predictions / 633 games and 630 outcomes / 630 games, all regular.
- RAW Totals: 608 predictions / 608 games and 597 outcomes / 597 games, all regular.
- Totals C: 467 predictions / 467 games and 452 outcomes / 452 games, all regular.
- Agreement study: 158 predictions / 158 games, 155 risk rows / 155 games, 35 outcomes / 35 games, and 1,550 bookmaker-price rows / 155 games, all regular.
- Retained main-market inventories: 95,630 rows across four tables; each table has 100% authoritative regular-season identity coverage.
- External normalized games: 1,528 rows / 1,528 games, all regular; all 1,527 retained StatsAPI feed rows have a present raw `R` type and match normalized type.
- Frozen external Tier A 2026 certification manifests: 43,696 player-game rows / 1,518 games, all regular.

The affected-game ledger is intentionally empty: no non-regular exact gamePk was proven in a fully reconciled governed cohort. Empty does not mean global proof; unresolved surfaces are listed in `audit_coverage_and_missing_evidence_register.csv`.

## Requirement trace

1. `7f22c762…` established the raw-`R` positive-membership lock for stat-derived generation before Opening Day.
2. `4d83b49b…` carried that instruction into the February 16 must-have backlog and separately required preseason/regular quality segmentation.
3. `d5a5a4f…` introduced a legacy missing-type-to-`R` compatibility fallback on March 29.
4. `5b24d711…` introduced Hits candidate evaluation with the same fallback on April 24.
5. July and August consumers introduced additional missing-to-`R` defaults and phase-free ledgers.
6. `f57afa6e…` introduced a calendar-derived October/November postseason label on September 9.
7. `28ec66a2…` established the general fail-closed authoritative phase contract on September 21.
8. `01c610c4…` certified the bounded canonical contract but also widened the stat-derived `require_regular_season` selector to accept postseason. Its tests did not enumerate downstream consumers.
9. `266dc486…` and `9cbe7064…` proved canonical source/test coverage, not repository-wide consumer compliance.
10. `3aec4975…` documented the consumer defects and sidecar cutover map.
11. `0ae9d87f…` corrected only the Hits candidate evaluator; `0ef19ac4…` bounded the historical compliance finding.

## Scientific interpretation

The retained evidence supports continued use of the specifically reconciled regular-season cohorts. It does not support a blanket certification of all historical MLB results. Prediction probabilities and picks in the proven cohorts remain unchanged because phase filtering removes zero rows. The control defects remain real: an all-regular realized cohort can prove outcome parity while still exposing a fail-open design that would admit future postseason, missing, or stale identities.

No existing metric is proven to require numerical restatement. Results on unmanifested surfaces require phase reconciliation before they can be certified or reused as a regular-season-only baseline.

## Minimum correction order

1. Freeze exact-gamePk manifests for the unprovable stat-derived/model-training, legacy research/ROI, Ops reporting, BvP, and feature-lineage populations before changing logic.
2. Join those frozen identities to the existing source-hashed authority proposal and publish missing/conflict ledgers; do not infer from dates.
3. Restate only a result whose frozen membership actually changes.
4. Remove the remaining missing-to-`R`, calendar-derived, and postseason-admitting controls one consumer at a time, preserving probability/pick hashes for admitted rows.
5. Add repository-wide enforcement that enumerates the 24 mapped consumers and rejects defaults, calendar reconstruction, and absent positive membership.
6. Require prospective exact-gamePk phase admission before postseason reporting or grading; keep regular-season close blocked until its complete inventory passes.

No correction, database operation, pipeline, schedule change, model change, prediction change, publication, wager, staging, commit, or push was performed by this audit.

Overall classification: `REGULAR_SEASON_EVIDENCE_PARTIALLY_UNPROVABLE`
