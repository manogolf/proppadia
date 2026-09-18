# Historical BvP downstream-consumption and materiality audit

Contract: `BVP_HISTORICAL_DOWNSTREAM_MATERIALITY_V1`.
Identity repair: `1c36620785837e057f89b263d3ab88f28c723469`.
Database evidence cutoff: September 18, 2026 **15:16:37.094857 PT / 22:16:37.094857 UTC**. Filesystem discovery is bounded to retained repository research/operational roots and installed user wrappers/LaunchAgents. Later generated audit files are not new acquisition evidence.

## Decision

`HISTORICAL_BVP_DEFECT_BOUNDED_REBUILD_REQUIRED` **and** `HISTORICAL_BVP_DEFECT_LINEAGE_INCOMPLETE`.

Confirmed defect context was admitted to three copies of a Total Bases research population. Their BvP context coverage/certification claims require bounded, superseding corrections. This is not evidence that their stored predictions, trained coefficients, production decisions or wagering results changed. Historical model impact cannot be certified where original training/source-selection receipts are missing. No currently qualified production MLB model exists under the canonical model authority. Independent Moneyline, RAW Totals, Totals C and current Hits nonmarket-parent source paths do not consume this defective feature family.

No historical database row, prediction, outcome, model, public output or operational setting was changed. No API request, acquisition/prediction rerun, model fit or historical rebuild was performed. This package contains only audit utilities and new evidence copies.

## Exact row disposition

The 270,582 retained BvP rows include 8,021 off-date rows across 43 games and 18 requested slate dates, April 7 through September 18. The off-date rows have 17 acquisition-calendar dates; this is not a count of original immutable acquisition runs. The table's mutable key omits the slate date, and upserts can replace date/computed-at/features. That limits historical replay.

| Class | Rows | Games | Slate dates | Acquisition dates | Distinct batters |
|---|---:|---:|---:|---:|---:|
| CONFIRMED_UTC_LOCAL_DATE_IDENTITY_DEFECT | 741 | 4 | 1 | 1 | 57 |
| VERIFIED_LEGITIMATE_RESCHEDULE_OR_SUSPENSION | 1,937 | 13 | 11 | 11 | 122 |
| OTHER_CONFIRMED_IDENTITY_DEFECT | 0 | 0 | 0 | 0 | 0 |
| UNRESOLVED_IDENTITY_PROVENANCE | 5,252 | 25 | 5 | 4 | 221 |
| SEPTEMBER_18_HASH_PINNED_QUARANTINE | 91 | 1 | 1 | 1 | 7 |

Original opposing pitcher identities/counts are **unrecoverable**, not zero, for every nonempty class: the legacy 19-field payload did not retain that identity or a genuine source-observation timestamp. Batter counts are class-specific and must not be added to obtain a population distinct count.

- April 15's 741 confirmed rows reproduce four previous-local-night game substitutions during the retained 17:05:40–17:23:07 UTC invocation. Its log reports exactly four local substitutions and 2,613 written rows; affected row computed-at values fall inside that invocation. This adds acquisition-linked evidence to the date fingerprint.
- The 91 September 18 rows are classified only through the existing complete-row SHA-pinned quarantine, not a broad game/date exclusion.
- The 1,937 legitimate rows have retained official requested-day entries, explicit reschedule/resumption metadata and pre-start acquisition. This establishes legitimate game/date association only—not full pitcher/as-of feature certification.
- Of the original 5,954 UTC/local fingerprints, 741 are confirmed, 91 are quarantined and **5,122 remain fingerprint-only unresolved**. The remaining 2,067 off-date rows divide into 1,937 legitimate and 130 unresolved; the latter game's retained acquisition was after its original start despite postponement metadata.
- No blanket exclusion of 8,021 rows is authorized or implemented. All authoritative schedule occurrences, including original and makeup entries, are retained in the evidence ledger.

The compressed row ledger includes the original key and row/features hashes, stored and official dates, UTC/PT scheduled start, acquisition time/date, classification/reason, schedule records and matching retained wrapper segments. The immutable database snapshot preserves the complete original off-date and September 18 rows.

## Consumers and proof standard

Inventory: 167 source/wrapper/documentation consumers (excluding this audit's new utility/tests), 34,896 CSV headers, 1,881 selected full feature-row scans, 793 retained BvP-referencing report/metadata files and 87 serialized model files. Source, fully scanned feature artifacts, report references and models are SHA-pinned; header-only CSV discovery records are not whole-file byte attestations. Entries distinguish source reachability from row admission. There were no CSV scan errors. This is a bounded inventory, not a claim that discarded database revisions, external notebooks or absent in-memory values were recovered.

Source reachability, matching game/player keys, feature names and equal zero-valued BvP fields alone are **not actual-consumption proof**. Across all disposition classes, 5,799 artifact-row admissions carry explicit source linkage; only the confirmed-defect admissions described below establish the bounded material impact. Three copies of one population do not create three independent outcome cohorts.

| Consumer family | Evidence and disposition |
|---|---|
| `prop_workflow.py` / prediction-wide / slate output | Current canonical identity gate enforced; historical fallback loses source identity. Past prediction use remains lineage-incomplete without row receipts. |
| `model_trainer.py` / LR-RF historical models / challengers | Current gate enforced. BvP column schemas show possible exposure, not specific training-row admission. |
| `v2_write_training_from_pfp.py` | Current gate enforced; existing official-final-game filter is an additional guard. Retained training key matches alone do not prove BvP ingestion. |
| Total Bases compact/context hydration utility | Explicit source game/date/tag and PA/AB/Hits/TB values prove substantive context admission to three CSVs; original source execution timestamp remains unproven. |
| Historical Hits and contextual challengers | Source fields/artifact hashes inventoried; absent per-training-source receipts prevent defect-only coefficient/performance replay. Annotate, do not claim no impact. |
| Feature health / source diagnostics / archived raw readers | Date-only and latest-prior joins remain reachable in noncertified manual/research utilities. Their prior evidence cannot be retroactively certified. |
| Selectors, ranking, Quick Card, upload exports | Transitive/export consumers are inventoried. No hash-linked confirmed-defect chain to a published or executed pick is proven. Missing lineage is not proof of zero risk. |
| Canonical Moneyline / RAW Totals / Totals C / current Hits parent | Unaffected by construction for this BvP-row defect; source hashes retained. This does not certify unrelated shared `game_info` defects. |
| September 18 manual BvP invocation | Log proves downstream predictive slate and impact stages skipped; this invocation produced no predictive/model consumption. It does not prove that no independent later reader accessed its data. |

Read-only affected-ID database queries retained 7,996 training rows, 1,897 player-derived-stat rows, 1,417 slate-copy rows and 1,375 wide-copy rows; no `bvp_stats` or `player_props` rows were returned for the 43 affected IDs. Training records expose no original BvP feature/source receipt. Player-derived stats and canonical Moneyline use independent sources. Key overlaps do not prove actual feature use. No BvP-dependent SQL view was discovered in the reviewed `mlb`/`public` view definitions.

Of 87 retained models, 83 metadata objects were readable and 37 declared direct BvP input columns. Four metadata failures remain documented: two research-only custom calibrator/candidate class imports, one invalid-probability test fixture and one empty dummy fixture. No exact training-row receipt was recovered. Retired `latest` LR/RF artifacts are nonauthoritative; other research models require lineage limitation annotations. A stale artifact-local enabled flag does not override `NO_QUALIFIED_MLB_MODEL`.

Retained historical BvP on/off tests are supplementary, not defect-only counterfactuals: April 15 reports observed zero prediction delta, while other reports show nonzero BvP deltas. Neither result establishes what the confirmed defect rows did to historical model quality.

## Proven materiality and counterfactuals

All three affected CSVs contain 24,780 rows. Each includes the **same 56 affected target rows**, linked to **46 confirmed source keys**, 46 players, nine target games and three target dates: April 15 (44), April 16 (3), April 17 (9).

| Retained surface | Exact required correction |
|---|---|
| `total_bases_canonical_post_bvp_blocker_audit/total_bases_post_bvp_blocker_classified_rows.csv` | Mask 56 confirmed source contexts; 55 newly replay-needed flags absent alternative source. |
| `total_bases_canonical_post_rolling_blocker_audit/total_bases_post_bvp_blocker_classified_rows.csv` | Same 56 contexts; 56 newly replay-needed flags. |
| `total_bases_canonical_spine_rolling_hydrated/2026-04-01_2026-06-14/total_bases_canonical_spine_dry_run_dataset.csv` | Same 56 contexts; 56 newly replay-needed flags. |

These paths are under `artifacts/analysis/mlb/model_quality/`; the impact/remediation ledgers pin their complete paths and SHA-256 values. The six associated Markdown/JSON reports require superseding context-coverage annotations/reports. No existing artifact is overwritten by this audit.

Confirmed-source masking changes BvP coverage **23,743 → 23,687** per copy. The hydration package merges context into preexisting predictions; it does not rescore them. Its stored probability and outcome values therefore remain unchanged under context-only correction. Candidate/promotion/wager impact and an alternative eligible-source fallback cannot be replayed exactly from a mutable present-day PFP snapshot.

For transparency, the package also computes fixed-population diagnostic sensitivities:

| Metric | Original stored population | Removing the 56 affected context rows |
|---|---:|---:|
| Scored rows | 23,000 | 22,945 |
| P_OVER ≥ 0.5 classification accuracy | 62.9826% | 63.0203% |
| Brier | 0.217514065 | 0.217406817 |
| Log loss | 0.702887402 | 0.702848862 |
| ECE, ten fixed equal-width bins | 0.053982655 | 0.054099663 |

This is **population-removal sensitivity, not improved predictions, original wager-rule accuracy or a certified rebuild**. No ROI is available under an exact executable-price contract. Stored baseball outcomes are unchanged. The broad all-off-date-source bound removes 1,933 context-linked target rows per copy, including legitimate/unresolved rows; it is explicitly uncertified and never defines the confirmed population. All exact full-precision metrics and contracts are in `counterfactual_metric_comparison.json`.

## September 18 preservation and active boundary

The original 1,937 rows and full hash stream remain unchanged; original SHA-256 is `87de42893507c219325e239b03d0f43ab9b2bf43c3f5444b480bfc1fab5f2775`. The protected reader population retains 1,846 game-identity-eligible rows, 142 batters and 13 games; the exact 91 rows cannot enter certified admission. Raw date equality alone no longer suffices. Regression tests execute all three actual query functions with mocked readers and reject wrong-slate rows while preserving identity metadata.

No other active certified bypass was found in the loaded Proppadia GUI jobs and reviewed wrapper graph. Direct SQL/manual research readers remain noncertified. Even the 1,846 eligible rows are **not fully feature-certified**: original opposing-pitcher and genuine source-time provenance is absent.

Historical/prospective statuses remain separate:

1. Forward acquisition after the repair uses official IDs plus independent date/PT/team/start checks and private per-request journals; natural-run receipts must still be validated before certification.
2. September 18 is partial game-identity certification only, preserving the 91-row quarantine.
3. The 832 confirmed/quarantined historical rows are not admitted to new certified use without exact identity protection.
4. The 1,937 legitimate reschedule rows are not fully feature-certified merely because their game/date association passes.
5. The 5,252 unresolved rows remain unresolved; neither automatic exclusion nor retroactive certification is justified.
6. Pre-fix research/model/report artifacts remain legacy evidence with the stated limitations, even where aggregate metrics appear unchanged.

## Smallest ordered remediation plan

1. Keep the current canonical gates and hash-pinned quarantine. No database repair is required or authorized here.
2. Separately authorize a **bounded deterministic context-certification rebuild** of the three matrices and six associated reports. Use retained 24,780-row copies, mask only confirmed linked source contexts, mark unavailable alternative-source replay, and publish superseding evidence. No API calls or model fitting are needed for this correction.
3. Annotate lineage limitations for historical BvP-input models, raw date-based diagnostic artifacts and downstream consumers lacking source receipts. Preserve retired/obsolete artifacts for research rather than promoting or refitting them.
4. Require original matrix/source-pick/as-of receipts before any model-specific defect counterfactual or proposed model rebuild. The original historical observation/pitcher timestamp cannot be recollected today. Do not mass-refit on the basis of 8,021 off-date rows.
5. No action for the independently sourced active Moneyline/Totals/Hits-parent lanes for this defect. No published or executed wager correction is justified without actual linkage evidence.

The machine-readable remediation matrix records authority, exposure, materiality, action, retained-input feasibility, cost and superseding—not replacement—policy for each identified item. Required rebuilds are recommendations only; none were performed.

## Tomorrow's natural-run readiness

`CODE_READY_WITH_POWER_DISPATCH_CAVEAT`: September 19, 2026 **03:30 PT / 10:30 UTC** remains the installed schedule. The loaded wrapper routes through the existing Makefile to the corrected collector. `BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1` function source is unchanged from the repair commit. Both governed locks, identity gate, per-request empty-response journal, starter-exclusion journal, acquisition-failure exits and normal no-qualified-model downstream/impact skips are preserved.

Installed wrapper SHA-256: `23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb`.
Collector SHA-256: `5e6a9bf7a7204ee650b56c4e08b6b1918b06c6833788256e5ef5016c2f58de36`.
Current repeating wake remains 05:27 daily. `StartCalendarInterval` does not wake a sleeping Mac; manual system sleep can defer 03:30 dispatch. Readiness is not a guarantee of timely dispatch or healthy networking. No power setting was changed.

After the next natural invocation inspect the exact current-run segment of `artifacts/ops/mlb_bvp_prewarm_daily.out.log` and `.err.log`; `START`, run identity, initial-schedule attempts/retry result, `identity_summary`, `DATABASE_WRITE_COMMITTED`, `DONE`, `BVP_PREWARM_RUN_END wrapper_rc=0`, and both lock releases. Inspect the private `artifacts/ops/bvp_identity_v1/2026-09-19/*.jsonl` receipts for intended/verified/rejected/prepared/written counts, batter/pitcher/game/team identities, response hashes and true observation timestamps, explicit empty successes and exact starter-exclusion reasons. Compare durable payload hashes with admitted rows and pre-start timing. Keep acquisition success separate from downstream model/impact skips. Do not invoke the run manually for this audit.

## Validation, package and rollback

89 focused identity, wake-retry, wrapper semantics, actual-reader/query and Hits shadow tests pass. Compilation, installed-wrapper shell syntax and `git diff --check` pass. The repeatable offline validator verifies SHA-256 entries, all 8,021 unique original row keys/hashes, deterministic dispositions, original September 18 preservation, exact source masks and original/filtered metric reproduction. The explicit read-only database recheck confirms **all 270,582 original BvP rows unchanged**, not merely the 91-row subset. Validation requires no new acquisition or model execution.

Replay verification from the repository root:

```sh
.venv/bin/python -m backend.mlb.scripts.audit_mlb_bvp_downstream_materiality_v1 validate
```

Principal files: `summary.json`, `row_disposition_ledger.jsonl.gz`, `consumer_inventory.jsonl`, `downstream_impact_ledger.jsonl`, `acquisition_date_downstream_trace.jsonl`, `counterfactual_metric_comparison.json`, `remediation_matrix.jsonl`, `september18_preservation_validation.json`, `tomorrow_natural_run_readiness.json`, `validation_results.json`, and `sha256_manifest.json`. Large audit ledgers are losslessly compressed; original source artifacts are not removed or changed. The package is approximately 6 MB, not a duplicate of the research/model estate.

Changed code: new audit utility `backend/mlb/scripts/audit_mlb_bvp_downstream_materiality_v1.py` and focused tests `backend/mlb/tests/test_bvp_downstream_materiality_audit.py`. Operational collector/readers/wrappers are unchanged. Existing agreement-study worktree changes and its unrelated publication lock are preserved outside this commit. Rollback, if later authorized: revert only the audit commit (utility/tests/new evidence); no operational or database rollback is necessary. Local commit only; **push: no**.
