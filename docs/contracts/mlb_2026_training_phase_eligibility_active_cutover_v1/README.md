# MLB 2026 Training Phase Eligibility Active Cutover V1

Status: **READY; ACTIVE FOR FUTURE ORDINARY TRAINING; NO MODEL TRAINED**

> **Evidence correction:** The original 444-artifact safety observation below is preserved historical evidence but used an overbroad/underinclusive population. `evidence_correction_v1/` supersedes its scope and interpretation: the original monitor covered 428 MLB plus 16 NHL paths and omitted 110 MLB binaries. The corrected metadata-only population is 538 MLB paths / 536 inode identities. No exhaustive byte-identity claim is made.

## Outcome

The ordinary `backend/mlb/model_trainer.py` path now applies the committed backend-neutral `MLB_REGULAR_SEASON_TRAINING_ELIGIBILITY_V1` helper at each row-source boundary. Only exact-`gamePk` records with verified `REGULAR_SEASON` authority proceed. `PRESEASON` and `POSTSEASON` are deterministic exclusions; missing, special, unknown, conflicting, duplicate, stale, or hash-invalid authority stops the run. There is no date inference and no missing-type-to-`R` fallback.

The gate changes membership only. It does not delete, update, or relabel source observations. No training, scoring, serialization, database access, provider access, schedule action, model replacement, publication, or operational artifact write occurred in this task.

## Active location and bypass result

The gate runs in `_fetch_reconcile_and_merge`, `_fetch_base_and_merge`, and `_fetch_from_view`, before any subsequent in-process time-feature, derived-feature, total-bases-feature, split, fit, score, or serialization operation. Authority failures in view mode are deliberately outside its data-access fallback handler, so an authority failure cannot silently choose another source. `train_models_for_prop` independently requires a passing gate report and source snapshot before preprocessing and refuses directly injected or otherwise ungated frames.

The ordinary trainer entry points and callers were traced. The installed weekly LaunchAgent is disabled, its wrapper exits `78` before the preserved retraining chain, and the production cron-cycle retraining flag defaults to `0`. The enabled daily refresh does not invoke this trainer. No automatic training invocation was active during cutover.

## Mandatory future lineage

Before `build_pipeline` or any `.fit`, the trainer creates and re-reads an immutable, no-overwrite input manifest under the unique run identity. It binds the code commit, trainer configuration, exact admitted gamePk population, phase decisions and exclusions, source identities and hashes, canonical order, feature and target hashes/contracts, and train/validation split.

After a future successful fit, the trainer hashes the run-scoped model and evaluation summary, writes an immutable result manifest bound to the input manifest, and verifies both manifests and every bound artifact. Mutable `latest`, archive, and model-index registration occurs only after this completed binding. An incomplete, interrupted, missing, or tampered binding cannot register or publish a model through this path.

## Frozen reproduction

| Population | Rows | Distinct gamePks |
|---|---:|---:|
| Frozen input | 600,766 | 2,812 |
| Admitted regular season | 459,604 | 2,341 |
| Excluded preseason | 141,162 | 471 |
| Missing, unknown, conflicting, or duplicate | 0 | 0 |

The exclusions are the already-proven 440 `S` and 31 `E` gamePks. The committed before/after commitments remain identical:

- retained feature source-row commitment: `2e5431d3d62dfe57462e28886085926466d1f557faa7a064f51df8a2f35dbccd`;
- retained target projection: `c8f1d572a343a0f97dd43b4441ff80c2660ad6bbe780ba27f9859bf567aace97`;
- retained row order: `66b27c8788a9dd788bd0a86cd1383814c231fceda17517f4ea426f437e60ef8f`.

## Validation

The original dependency-free standard-library runner executed 40 of 40 intended scenarios: 40 passed, 0 failed, 0 skipped, and 0 unexecuted. This comprises 12 active-cutover tests, 15 committed dry-run tests, and 13 existing Hits authority tests. Thirteen aggregate checks passed. Its 444-path metadata population had the same pre/post state hash, and instrumentation observed zero fit calls, training commands, operational writes, database connections, network requests, or paid requests. The preserved 444-path result is superseded for artifact-population scope and must not be interpreted as byte identity; see `evidence_correction_v1/`.

## Limitations and next action

This cutover governs future ordinary `model_trainer.py` runs only. Existing models and historical result artifacts remain unbound and are not retroactively certified. Direct research code that imports lower-level pipeline builders is not a registered ordinary-training path and was not changed. The configured-view mode gates rows at the first boundary available to this process; this change does not certify how an externally defined view computed already-materialized upstream features. The current file authority must also be refreshed through a separately governed process before training on games outside its supported window.

The smallest next action is review of this commit. Before separately authorizing the first future controlled training run, verify that the authority window covers its exact input population, the weekly scheduler remains disabled unless deliberately enabled, and any configured external feature view has phase-safe strict-prior construction.

Classification: `TRAINING_PHASE_ACTIVE_CUTOVER_READY`
