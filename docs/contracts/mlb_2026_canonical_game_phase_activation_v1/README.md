# MLB 2026 canonical game phase activation V1

Status: **SOURCE PREPARED AND OFFLINE VALIDATED; NOT ACTIVATED**

Contract: `MLB_2026_CANONICAL_GAME_PHASE_ACTIVATION_V1`

Prepared: 2026-09-21

No migration, database write, live or paid request, daily pipeline, scheduler change, season close, prediction change, model promotion, publication, wager, or push was performed. The September 21 pre-acquisition failures remain documented missed snapshots; this package neither reruns nor compensates for them. Ordinary scheduled windows are the only authorized operational-validation path.

## Contract delivered

- `mlb.game_info` and `mlb_cleanroom_v1.games` can persist exact StatsAPI `gameType`, source season, normalized phase, postseason round, raw round description, and raw reschedule/resume relationships after the prepared migration is authorized.
- Phase is derived only by `backend/mlb/season_transition/contract_v1.py`. Missing, unknown, non-exact, or conflicting source types fail closed. No missing type becomes `R`; no calendar field determines phase.
- Clean-room game producers use named-column inserts and remain compatible with either the complete legacy schema or the complete activated schema. A partially applied schema is rejected.
- `mlb.canonical_game_phase_v1` is the downstream interface: join `downstream.game_pk = canonical_game_phase_v1.game_pk`. It intentionally has no date/team fallback and omits conflicting identities. Downstream tables receive no duplicate phase columns.
- The Python `CanonicalGamePhaseIndex.phase_for_game_pk()` provides the same exact-key, fail-closed interface for offline work.

## Offline backfill proposal

The committed proposal under `offline_backfill_proposal/` was built only from 62 retained `schedule.json` files below `backend/mlb/exports/cleanroom_v1/raw/MLB_STATS_API`:

- 882 retained schedule rows;
- 97 exact canonical `gamePk` values;
- 785 consistent duplicate observations;
- 97 `REGULAR_SEASON`, zero postseason/preseason/special rows in this retained subset;
- zero database writes and zero network requests.

Every source file is byte-hashed in `retained_source_manifest.jsonl`. Each proposal row carries its source paths and SHA-256 identities. This proposal is review material, not executable SQL, and was not applied.

Rebuild into a new directory only:

```bash
PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.build_mlb_canonical_game_phase_backfill_v1 \
  --input backend/mlb/exports/cleanroom_v1/raw/MLB_STATS_API \
  --output-dir /absolute/new/output/directory
```

## Deterministic validation

The dependency-free validator covers every postseason type, a regular game after the nominal closing date, postponed/rescheduled and suspended/resumed relationships, All-Star/special exclusion, missing/unknown/conflicting types, duplicate `gamePk` conflict, exact-key lookup, positional-insert regression, zero date reconstruction, both prepared schemas and the exact join view, proposal determinism/hashing, and the two-state propagation audit.

```bash
PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.validate_mlb_canonical_game_phase_activation_v1
```

The pytest counterpart is `backend/mlb/tests/test_mlb_canonical_game_phase_activation_v1.py`. The canonical interpreter currently has no `pytest` module, so validation uses the dependency-free runner; the test module was syntax-compiled with that same interpreter.

## Activation runbook (future authorization required)

1. Confirm the deployed commit equals the reviewed local commit, tracked worktree is clean, `.venv` and the DH publish lock remain the only expected untracked items, and ordinary scheduled ingestion has not exposed a contract error.
2. Rebuild the proposal from the complete retained schedule corpus into a new immutable directory. Require zero rejects/conflicts and verify both its inner manifest and this package manifest.
3. Take and verify a recoverable database backup for `mlb.game_info` and `mlb_cleanroom_v1.games`. Record row counts, primary keys, constraints, views, and dependent objects.
4. In a disposable database restored from that backup, apply `20260921_prepare_mlb_season_phase_contract_v1.sql`; verify constraints, producer compatibility, and rollback. This repository task did not do so.
5. Only with separate database-change authorization, apply the reviewed migration in a transaction during a controlled window.
6. Load only reviewed proposal rows through a separately reviewed, transactional activation loader keyed by `mlb.game_info.game_id` and `mlb_cleanroom_v1.games.(game_pk, source_payload_sha256)`. The loader must compare before update, reject conflicts, write no downstream phase columns, and roll back the entire transaction on any unmatched or conflicting identity. No loader or backfill write is authorized by this package.
7. Require every canonical target row to match its proposal hash/type/season/phase/round, require the exact-game view to omit no expected non-special game, and record an activation manifest.
8. Let the next ordinary scheduled windows exercise both producers. Do not create a compensating run for the September 21 missed snapshots.
9. Keep all downstream postseason gates closed until every blocker below is resolved and separately validated.

## Rollback runbook

1. Stop activation at the transaction boundary on any mismatch. If not committed, issue `ROLLBACK`; no further action is needed.
2. If committed but no dependent consumer has activated, preserve a phase export and activation log, then apply the prepared `20260921_rollback_mlb_season_phase_contract_v1.sql` only with separate destructive-change authorization. It drops the exact join view and phase columns, so the verified backup/export is mandatory.
3. Revert the activation deployment to the reviewed pre-activation commit. The source producers support the legacy schema and will use their named legacy column list.
4. Confirm original row counts/keys and ordinary scheduled ingestion health. Never delete or recreate the retained schedule files, `.venv` symlink, or DH publish lock.
5. If any downstream consumer has activated, first disable/revert that consumer. Do not drop the view or columns while a dependency remains.

## Exact remaining blockers for downstream lane gating

1. The prepared migration is unapplied and both live canonical tables require the reviewed backfill activation described above.
2. No transactional database backfill loader was authorized or delivered; it must be separately reviewed and tested against a restored database before activation.
3. Full-board Hits and external normalized-game code still contain missing-type-to-`R` behavior. That fallback must be removed before postseason admission.
4. Moneyline, RAW Totals, Totals C, Full-board Hits, and their graders still need exact `gamePk` joins to `mlb.canonical_game_phase_v1`, with missing/conflicting phase blocking the lane.
5. The agreement-separation study still reconstructs a postseason regime from October/November and must use the exact canonical join.
6. Daily reports, Ops Briefs, indexes, research exports, and grading summaries do not yet enforce distinct regular/postseason cohorts and round-separated postseason reporting.
7. The current retained clean-room proposal contains no postseason observation. All postseason mappings are deterministic fixture coverage only until ordinary scheduled windows retain real postseason source bytes.
8. Ordinary scheduled-window producer validation after activation is outstanding. The September 21 missed snapshots are not candidates for replay.

Until all eight are cleared, downstream postseason lane gating remains **BLOCKED**. This does not close the regular season and grants no prediction, promotion, publication, or wagering authority.

## Files

- Prepared activation migration: `backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql`
- Prepared rollback: `backend/mlb/sql/migrations/20260921_rollback_mlb_season_phase_contract_v1.sql`
- Canonical phase persistence/join code: `backend/mlb/season_transition/canonical_phase_v1.py`
- Offline builder: `backend/mlb/scripts/build_mlb_canonical_game_phase_backfill_v1.py`
- Offline validator: `backend/mlb/scripts/validate_mlb_canonical_game_phase_activation_v1.py`
- Deterministic pytest suite: `backend/mlb/tests/test_mlb_canonical_game_phase_activation_v1.py`
- Current/post-activation propagation audit: `docs/contracts/mlb_2026_regular_season_close_and_postseason_data_plan_v1/end_to_end_propagation_audit.csv`
- Package integrity: `sha256_manifest.txt`
