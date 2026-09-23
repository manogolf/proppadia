# Activation and rollback runbook (documentation only)

Nothing in this document is authorized by the current task. Do not execute
these steps until actual ordinary-retention postseason evidence exists and a
separate activation task is reviewed.

## Candidate preparation

1. Select immutable retained StatsAPI schedule files through an explicit
   source-set JSON. Pin each path, byte count, and SHA-256. Do not acquire or
   replay data for this step.
2. Pin the parent descriptor path and SHA-256. The first child must name the
   exact V1 descriptor.
3. Invoke the canonical interpreter and the offline builder with an absent,
   review-only output directory:

   ```text
   /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
     -m backend.mlb.scripts.build_mlb_versioned_file_phase_authority_v1 \
     --source-set <reviewed-source-set.json> \
     --output-dir <new-candidate-directory>
   ```

4. If the decision is `NO_NEW_AUTHORITY_EVIDENCE`, stop. No snapshot or
   selection change is justified.
5. For a candidate, verify every output hash, all source bytes, parent-prefix
   equality, 2,919 unchanged V1 rows, new exact-gamePk rows, phase counts, and
   supported date range.
6. Run all consumer dependency tests. A candidate remains inactive regardless
   of test success.

## Separately authorized activation

1. Require a reviewed candidate containing actual, non-synthetic retained
   postseason evidence.
2. Convert candidate status to a reviewed governed descriptor without changing
   its proposal or source-manifest bytes; pin the new descriptor hash.
3. Record the prior active-selection bytes and SHA-256.
4. Verify the selection still points to the expected V1 descriptor. Any
   mismatch aborts compare-and-swap activation.
5. Atomically replace only
   `backend/mlb/season_transition/authority_snapshots/active_selection.json`
   with a governed selection naming the reviewed descriptor and hash.
6. Start a new process boundary or clear only the documented in-process
   authority cache. Never allow one run to mix descriptor identities.
7. Validate each lane only through its next naturally eligible ordinary window.
   Do not manually rerun or duplicate acquisition.
8. Agreement V4 remains closed; it requires separate acquisition governance.

Abort on any missing file, hash mismatch, base-row difference, source/type/
season/round/relationship conflict, duplicate identity, unsupported schema,
parent-chain break, phase mixing, close-population change, or lane-specific
failure.

## Rollback

1. Preserve the failed snapshot, selection, validation, and lane evidence.
2. Verify the saved prior selection and exact V1 descriptor hash
   `543fda06d3c066bb6f0608ee8c05987829216fa441459a8fc08b4f87ee8c245a`.
3. Compare-and-swap the active selection only if it still names the failed
   descriptor expected by the rollback authorization.
4. Atomically restore the prior V1 selection bytes.
5. Start new consumer processes/clear documented caches. Consumers referencing
   removed child gamePks must fail closed; no automatic fallback is permitted
   inside an already-started run.
6. Re-run V1 descriptor, close, and consumer health checks. Rollback never
   deletes or rewrites predictions, outcomes, market evidence, BvP, feature
   lineage, training manifests, or either snapshot.
