# MLB 2026 Stat-Derived Training Phase Correction Design V1

Status: **DESIGN READY; NO IMPLEMENTATION OR RESTATEMENT PERFORMED**

## Decision

Preserve `mlb.model_training_props` as observation history. Add one shared, backend-neutral regular-season eligibility gate at dataset assembly boundaries. The gate admits a row only when its exact gamePk has positive `REGULAR_SEASON` membership in a fully verified authoritative phase instance. It changes membership only; retained rows, features, targets, probabilities, predictions, outcomes, and prices are byte-for-byte inputs to the same downstream logic.

The current backend is the source-hashed file authority exposed by `CanonicalGamePhaseAuthority`. A future sidecar may replace that backend only after full-key and admitted-membership parity. Consumers must not contain their own phase mapping, date rule, or missing-type default.

## Verified input

The complete input package at `docs/contracts/mlb_2026_stat_derived_model_training_phase_population_freeze_v1/` was verified before use:

- validator: 61 passed, 0 failed;
- total size: 244,578,450 bytes;
- files: 19;
- large ledger: 153,352,113 bytes and 618,151 data rows;
- authority: proposal SHA-256 `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`, 2,919 exact gamePks;
- source manifest SHA-256 `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`.

No file from the freeze package was edited. The large ledger remains untracked and must not be staged or committed until repository storage impact is reviewed.

## Contamination finding

The complete 2026 operational population contains 600,766 rows over 2,812 exact gamePks:

- 459,604 rows / 2,341 gamePks are authoritative regular season;
- 141,162 rows / 471 gamePks are authoritative preseason;
- postseason, special/unknown, absent authority, missing gamePk, duplicate identity, and source-conflict counts are zero.

All three frozen operational scopes—complete `model_training_props`, stat-derived, and base-model eligible—are identical. This is an observed dataset-membership defect. It is not evidence that every retained model binary trained on the entire current relation.

Two retained evaluation artifacts contain non-regular membership:

- `tmp/mlb_base_vs_market_rows_anybook_full.csv`: 69,652 regular rows / 443 games; 869 preseason rows / 25 games (734 `S` rows / 20 games and 135 `E` rows / 5 games).
- `tmp/mlb_base_vs_market_rows_anybook_one_sided_rows.csv`: 55,252 regular rows / 1,381 games; 16,516 preseason rows / 471 games (15,517 `S` rows / 440 games and 999 `E` rows / 31 games).

Counts are source-specific and overlap; they must not be summed as unique observations across artifacts.

## Historical result decision

Both affected retained evaluation artifacts can be deterministically restated by filtering their retained 2026 rows through exact gamePk authority. They retain exact gamePk and their original prediction, outcome, and price fields; no missing prediction or price may be manufactured. Existing missing values remain missing under the existing metric contract. The freeze's read-only counterfactual found unchanged recorded graded metrics for these two artifacts because their excluded preseason rows were outside the metric denominators, although artifact membership changes.

The operational relation counterfactual changes membership and diagnostic metrics. It does not identify a published or certified result to restate because historical run manifests do not bind those results to an exact input population.

The frozen 7,564-row Hits pilot remains `PROVEN_REGULAR_INPUT` for only its tested 651-game cohort and retains `RESULT_SAME_BUT_CONTROL_VIOLATED`. It is not generalized to another result.

## Model lineage conclusion

No legacy `model_trainer.py` binary has a retained exact-gamePk input manifest bound to its model hash. MODEL_INDEX and semantic-registration records retain useful model metadata and artifact identity, but not the required training membership. Those binaries are `INPUT_MEMBERSHIP_UNPROVABLE`, regardless of creation date.

The Hits residual ranker is `RECONSTRUCTABLE_BUT_UNBOUND`: its declared 229,610-row input CSV is retained with exact game IDs and matches the diagnostic row count, but its input was not bound by an immutable training-run manifest and the present authority covers only 2026.

The Hits 0.5 full-spine model is `NOT_APPLICABLE` to this correction because its recorded lineage is the separate strict-prior `player_stats/game_info` spine, not `model_training_props` or the retained reconcile artifacts.

No existing model binary is classified `PROVEN_REGULAR_INPUT` from retained training lineage.

The inventory individually covers 538 local MLB model binaries: 428 under `models_out`, 39 in the legacy bundle, the Hits residual ranker, and 70 MLB-tagged research binaries. With result inputs and semantic registrations, the lineage ledger contains 544 records. Research artifacts are not upgraded merely because nearby evidence exists; without the complete exact-gamePk/authority/model binding required here, they remain `INPUT_MEMBERSHIP_UNPROVABLE`.

## Package contents

- `package_storage_report.csv`: every freeze-package file size and storage disposition.
- `contamination_by_source.csv`: exact phase counts by frozen source population.
- `retained_evaluation_restatement.json`: deterministic restatement feasibility and boundaries.
- `affected_consumer_map.csv`: every identified direct reader classified by use.
- `model_lineage_inventory.csv`: in-scope model/result artifacts and lineage classification.
- `shared_eligibility_contract.md`: the single eligibility interface and backend transition rule.
- `future_training_manifest_contract.json`: mandatory immutable run binding.
- `correction_sequence.md`: staged validation and rollback plan.
- `retraining_evidence_threshold.md`: evidence required before retraining.
- `minimum_justified_implementation_task.md`: the smallest next implementation.
- `source_evidence_manifest.csv`, `sha256_manifest.txt`, `validate_design.py`, and `validation_report.json`: provenance and deterministic validation.

## Boundary confirmation

This package is design-only. It did not change source code, selectors, databases, observations, models, predictions, certifications, schedules, or pipelines. It did not retrain, rescore, restate, stage, commit, push, install software, or make a network/provider request.

The tracked worktree was clean at task start at `08c661a9c9108795514d3a882a5fdc2a80508cee`. During validation, unrelated NHL source/test/documentation edits appeared and were independently committed as `398af3669c92ea953fffa0bcca9e1a11072a301c`, advancing HEAD while retaining the starting commit in ancestry. They were not created, inspected, staged, or modified by this task. Final validation proves zero tracked or staged changes and leaves both MLB review packages untracked.

Classification: `TRAINING_PHASE_CORRECTION_DESIGN_READY`
