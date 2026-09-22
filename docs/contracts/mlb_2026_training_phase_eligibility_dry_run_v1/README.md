# MLB 2026 Training Phase Eligibility Dry Run V1

Status: **READY; DRY RUN ONLY; ORDINARY TRAINING UNCHANGED**

## Result

The shared `MLB_REGULAR_SEASON_TRAINING_ELIGIBILITY_V1` helper admitted only positive exact-`gamePk` `REGULAR_SEASON` authority from the verified file backend.

| Population | Rows | Distinct gamePks |
|---|---:|---:|
| Frozen input | 600,766 | 2,812 |
| Admitted regular season | 459,604 | 2,341 |
| Excluded preseason | 141,162 | 471 |
| Postseason | 0 | 0 |
| Missing, unknown, special, conflict, or duplicate | 0 | 0 |

The 471 exclusions consist of 440 StatsAPI `S` gamePks and 31 `E` gamePks. `excluded_game_pks.csv` enumerates every exact gamePk and its row count.

## Membership-only proof

The gate preserves each admitted mapping object and its stable input order. Before/after hashes are identical for the retained row-identity order, retained target projection, and the frozen source-row commitment used for feature-value integrity. The compact freeze intentionally does not disclose raw operational feature vectors; its one-way `source_row_fingerprint` commits each complete frozen operational source row. No value is recomputed by the gate.

- feature source-row commitment SHA-256: `2e5431d3d62dfe57462e28886085926466d1f557faa7a064f51df8a2f35dbccd` before and after;
- target projection SHA-256: `c8f1d572a343a0f97dd43b4441ff80c2660ad6bbe780ba27f9859bf567aace97` before and after;
- retained row-order SHA-256: `66b27c8788a9dd788bd0a86cd1383814c231fceda17517f4ea426f437e60ef8f` before and after.

## Invocation

```bash
PYTHONDONTWRITEBYTECODE=1 /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  backend/mlb/model_trainer.py --phase-eligibility-dry-run
```

This separately invoked branch verifies the entire 244,578,450-byte freeze package and the source-hashed phase authority before reading population rows. It emits only `dry_run_report.json` and `excluded_game_pks.csv` in this governed package. It returns before model authority checks, data access, feature hydration, fitting, scoring, model-directory creation, serialization, or operational artifact writes.

Ordinary invocations retain their prior training path. No selector cutover is active.

## Validation

The dependency-free standard-library runner executed 15 of 15 intended scenarios: 15 passed, 0 failed, 0 skipped, 0 unexecuted. The validator passed 9 of 9 aggregate checks. The existing Hits authority pilot validator also remains passing.

Observed dry-run counters are zero for database connections, network and paid requests, fit/train/score calls, model/prediction/dataset/operational writes. The only permitted writes are the two bounded evidence files above.

## Recommendation and next action

The evidence justifies review of an active `model_trainer.py` selector cutover, but does not authorize it. The smallest next action is a separate reviewed cutover task that invokes this shared helper at each real trainer input boundary before feature aggregation, emits a mandatory gate report, and proves retained feature/target parity on the actual selected frame for `reconcile_csv`, `base_merge`, and configured-view modes.

Classification: `TRAINING_PHASE_ELIGIBILITY_DRY_RUN_READY`
