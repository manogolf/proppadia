# Canonical phase activation and rollback runbook

All commands below are **prepared only**. They require a new, explicit
activation or rollback authorization. They were not executed by this preflight.

## 0. Clear the blocking rehearsal gate

On a host with an isolated disposable PostgreSQL server already available:

```bash
cd /Users/jerrystrain/Projects/proppadia
/Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.rehearse_mlb_canonical_phase_migration_activation_v1 \
  --proposal docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl \
  --source-manifest docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/retained_source_manifest.jsonl \
  --migration backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql \
  --rollback backend/mlb/sql/migrations/20260921_rollback_mlb_season_phase_contract_v1.sql \
  --output artifacts/activation/mlb_2026_canonical_phase_v1/isolated_rehearsal_report.json
```

Require `status=PASS` for every scenario and zero production DDL/DML. Rebuild
and validate this preflight contract before continuing.

## 1. Pre-activation backup and evidence

Create a restricted, uncommitted backup directory. Load credentials without
printing them, then take a custom-format backup of only the governed tables:

```bash
cd /Users/jerrystrain/Projects/proppadia
umask 077
mkdir -p artifacts/activation/mlb_2026_canonical_phase_v1
set -a
source backend/.env
set +a
/opt/homebrew/opt/libpq/bin/pg_dump \
  --dbname="$SUPABASE_DB_URL" \
  --format=custom \
  --no-owner \
  --no-acl \
  --table=mlb.game_info \
  --table=mlb_cleanroom_v1.games \
  --file=artifacts/activation/mlb_2026_canonical_phase_v1/pre_activation_tables.dump
/Users/jerrystrain/Projects/proppadia/.venv/bin/python -c \
  "import hashlib,pathlib; p=pathlib.Path('artifacts/activation/mlb_2026_canonical_phase_v1/pre_activation_tables.dump'); print(p.stat().st_size, hashlib.sha256(p.read_bytes()).hexdigest())"
```

Abort if `pg_dump` fails, the file is empty, hashing fails, or a restore into the
disposable rehearsal server does not reproduce 10,337 `game_info` rows and 590
clean-room rows with identical identity hashes. Never commit the dump.

Take a new read-only snapshot immediately before the window:

```bash
/Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.collect_mlb_canonical_phase_migration_preflight_v1 \
  --env-file backend/.env \
  --output artifacts/activation/mlb_2026_canonical_phase_v1/pre_activation_snapshot.json
```

Require the exact target identity
`5fe30ccd585f3ccb9781999afafab6d42d793aee336998421e395cb5e9c5fb7d`,
no phase columns/view, 10,337/10,337 `game_info` rows/gamePks, 590/86
clean-room rows/gamePks, zero relevant relation/advisory locks, and the exact
proposal intersections 2,809 and 86.

## 2. Maintenance window and lock strategy

Separately pause every invocation path for these writers and record the
operator/time in uncommitted activation evidence:

- `insert_mlb_stat_derived.py`;
- `run_cleanroom_source_cycle.py`;
- `admit_exact_roster_bridge.py`;
- parent workflows such as `run_cleanroom_bol_tb15_capture.py`.

Confirm no process is running:

```bash
pgrep -alf 'insert_mlb_stat_derived|run_cleanroom_source_cycle|admit_exact_roster_bridge|run_cleanroom_bol_tb15_capture'
```

Expected output is empty. Do not change schedules under this runbook unless
that action is separately authorized. The loader uses a transaction-scoped
advisory lock and `ACCESS EXCLUSIVE ... NOWAIT`; it aborts instead of waiting
when any writer or lock conflicts.

## 3. Atomic schema migration and canonical backfill

Verify immutable inputs first:

- proposal SHA-256:
  `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`;
- source manifest SHA-256:
  `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`;
- migration SHA-256:
  `212233451cb8f7478e7ef92792c0026f834ae392ff6bd6baa58ad94ac2e8de39`;
- rollback SHA-256:
  `a382bd80f7104bdf144a5a51f52269851aad12362e007e62e37870942d68c488`.

With separate activation authorization, execute the guarded loader once:

```bash
/Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.activate_mlb_canonical_phase_v1 \
  --execute \
  --authorization-phrase EXECUTE_MLB_2026_CANONICAL_PHASE_ACTIVATION_V1 \
  --env-file backend/.env \
  --proposal docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl \
  --source-manifest docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/retained_source_manifest.jsonl \
  --migration backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql \
  --evidence-output artifacts/activation/mlb_2026_canonical_phase_v1/activation_evidence.json \
  --expected-proposal-sha256 b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879 \
  --expected-target-identity-sha256 5fe30ccd585f3ccb9781999afafab6d42d793aee336998421e395cb5e9c5fb7d \
  --expected-game-info-matches 2809 \
  --expected-cleanroom-rows 590 \
  --expected-game-info-changes 2809 \
  --expected-cleanroom-changes 590
```

The loader owns one atomic transaction for locks, schema migration, staging,
both table updates, trigger restoration, and validation. A failure writes local
durable evidence and rolls back or never starts. Do not separately apply the
SQL file.

## 4. Post-write validation and writer smoke checks

Require the activation evidence to report:

- status `COMMITTED`;
- exactly 2,809 and 590 changes;
- zero inserts/deletes and unchanged base identity hashes/row counts;
- 2,809 exact-gamePk view rows with no conflict omission;
- the clean-room append-only trigger enabled;
- all 464 source hashes verified and all 2,919 proposal identities unique.

Run a new read-only snapshot and the committed validators. Do not overwrite the
pre-activation snapshot:

```bash
/Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.collect_mlb_canonical_phase_migration_preflight_v1 \
  --env-file backend/.env \
  --output artifacts/activation/mlb_2026_canonical_phase_v1/post_activation_snapshot.json
/Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.run_mlb_canonical_phase_migration_preflight_tests_v1
```

Resume writers only through their separately governed scheduler mechanism.
Do not kick-start a pipeline. Let the next ordinary scheduled window exercise
the named-column writers, then repeat the read-only snapshot and require its new
game rows to carry exact source type/season/phase/hash without conflicts.

## 5. Abort and rollback decision

Abort before commit on **any** nonzero unexpected row/gamePk, source hash
mismatch, unverified source, missing/unknown/conflicting type, phase mismatch,
duplicate identity conflict, unexpected lock, waiting lock, writer
incompatibility, row creation/deletion, trigger-definition mismatch, disabled
trigger at commit, or deviation from 2,809/590 expected mutations.

After commit, roll back before resuming writers if any corresponding
post-validation threshold is nonzero, the view has fewer or more than 2,809
rows, the backup/activation evidence is incomplete, or an ordinary writer smoke
check fails.

## 6. Deterministic rollback

With separate rollback authorization and the activation evidence still intact:

```bash
/Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.rollback_mlb_canonical_phase_v1 \
  --execute \
  --authorization-phrase EXECUTE_MLB_2026_CANONICAL_PHASE_ROLLBACK_V1 \
  --env-file backend/.env \
  --rollback backend/mlb/sql/migrations/20260921_rollback_mlb_season_phase_contract_v1.sql \
  --activation-evidence artifacts/activation/mlb_2026_canonical_phase_v1/activation_evidence.json \
  --evidence-output artifacts/activation/mlb_2026_canonical_phase_v1/rollback_evidence.json \
  --expected-target-identity-sha256 5fe30ccd585f3ccb9781999afafab6d42d793aee336998421e395cb5e9c5fb7d
```

Require `ROLLED_BACK_COMMITTED`, unchanged base row counts and identity hashes,
zero V1 phase columns, and no canonical view. If base identities differ, the
rollback transaction aborts; restoring the backup is a separate destructive
operation requiring separate authorization.
