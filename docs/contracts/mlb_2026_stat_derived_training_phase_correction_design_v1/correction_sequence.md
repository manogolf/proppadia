# Bounded Correction Sequence

No step below is authorized by this design package.

## Stage 1 — Eligibility interface implementation

Implement `MLB_REGULAR_SEASON_TRAINING_ELIGIBILITY_V1` once, using the existing `CanonicalGamePhaseAuthority` abstraction. Add dependency-free unit tests for exact admission, preseason/postseason exclusion, every fail-closed reason, stable ordering, no row mutation, repeated-run determinism, and file/sidecar backend parity fixtures.

Validation: existing authority tests plus new interface tests pass; static scan finds no date/type/default logic in the helper; proposal and retained sources verify before row processing.

Rollback: revert only the new unused helper and tests. No consumer behavior or data state has changed.

## Stage 2 — Selector cutover

Cut over the central dataset builders before leaf reports:

1. `backend/mlb/model_trainer.py` for `reconcile_csv`, `base_merge`, and view-backed modes;
2. `backend/mlb/scripts/build_mlb_reconcile_rows.py` before it emits evaluation/training artifacts;
3. direct training/calibration/validation entrypoints identified in `affected_consumer_map.csv`;
4. research and reporting consumers only where they claim regular-season membership.

Raw observation writers and raw-health reports remain unchanged. No observation is deleted, relabeled, or updated.

Validation: each consumer emits a gate report; exact row/gamePk deltas match authority; retained-row feature/target hashes are unchanged; zero blocked identities; tests prove all data-source modes invoke the shared gate.

Rollback: restore the prior consumer call site while leaving the shared interface dormant. Do not alter observations or authority evidence.

## Stage 3 — Frozen-population comparison

Against the frozen operational manifest, require exactly:

- input: 600,766 rows / 2,812 gamePks;
- admitted regular: 459,604 rows / 2,341 gamePks;
- excluded preseason: 141,162 rows / 471 gamePks;
- postseason/special/unknown/missing/conflict/duplicate: zero.

For every admitted row, feature and target hashes must equal the pre-cutover row. This stage compares membership and invariance only; it does not train or score.

Rollback: remove the cutover if counts or hashes differ. Any unexpected gamePk or count is an abort, not a tolerated drift.

## Stage 4 — Retained evaluation restatement

Under separate authorization, write new, versioned regular-only derivatives of the two retained affected artifacts. Never overwrite originals. Filter by exact gamePk, preserve all retained values, and recompute only metrics supported by existing non-null fields and the pre-existing metric contract.

Validation: input artifact hashes match; expected phase splits match `retained_evaluation_restatement.json`; excluded gamePk sets match; retained prediction/outcome/price hashes match; no new non-null value appears; recorded metric equality or change is reported exactly.

Rollback: discard the new derivative and supersession pointer. Original artifacts and results remain immutable.

## Stage 5 — Future manifest binding

Make the immutable run manifest mandatory before any new model/result certification. Emit it before training with input and authority sections fixed; finalize output hashes after training/result generation without changing the fixed input body. Certification verifies the entire chain.

Validation: hash-tamper, missing-field, wrong-parent, stale-authority, dirty-code, and population-drift tests fail closed; deterministic repeat produces the same input-population hash.

Rollback: block certification and keep the run experimental. Never certify an output using a partial legacy manifest.

## Stage 6 — Model-specific retraining decision

Apply `retraining_evidence_threshold.md` separately to each model. Do not infer input membership from model timestamp, the current relation, or a nearby artifact. Retraining is a later model-specific decision, not part of selector correction.

Validation and rollback are defined in a separately approved retraining protocol only after its threshold is met.
