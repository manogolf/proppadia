# MLB 2026 canonical phase migration activation preflight V1

Status: **PREFLIGHT VALIDATED; ACTIVATION BLOCKED**

Contract: `MLB_2026_CANONICAL_PHASE_MIGRATION_ACTIVATION_PREFLIGHT_V1`

This package is a read-only operational-database preflight. No operational DDL
or DML, migration, canonical backfill, provider request, paid credit, daily
pipeline, scheduler change, postseason activation, regular-season close,
prediction/model change, publication, upload, wager, or push was performed.

## Repository and retained source

Starting HEAD was `9cbe70643b679a54637fe25b71cc244889768844` and tracked/staged state was clean.
The `.venv` symlink and pre-existing DH publish lock retained their previously
recorded targets, metadata, and empty-file hash. Neither is modified or part of
this package.

The ignored authoritative response was verified before use:

- path: `backend/mlb/data/external/statsapi/raw/2026/schedule_2026-02-20_2026-03-25.json`;
- bytes: 642,447;
- SHA-256: `3ca8bff9e4361676ad3b34c5166f23f4d180d809083557ad8498062a9febd091`.

It remains ignored and uncommitted.

## Population transition

The exact transition is proven in `population_transition.json`:

| Measure | Count |
|---|---:|
| Prior canonical union | 2,901 |
| Previously missing and now resolved | 471 |
| Distinct gamePks in the new response | 490 |
| Returned outside the missing ledger | 19 |
| Net-new canonical identities | 18 |
| Previously classified overlap | 1 |
| Final retained population | 2,919 |

Missing classifications, unknown types, source conflicts, and duplicate
identity conflicts are all zero. Dates are never used to classify phase.

## Operational database findings

Snapshot transaction: repeatable-read and read-only at
`2026-09-22 02:13:31.429905+00:00`.

- Engine: PostgreSQL 15.8.
- Target: `aws-0-us-west-1.pooler.supabase.com:5432/postgres`.
- Target identity SHA-256: `5fe30ccd585f3ccb9781999afafab6d42d793aee336998421e395cb5e9c5fb7d`.
- Schema owners: `mlb=postgres`; `mlb_cleanroom_v1=postgres`.
- Migration mechanism: repository-owned SQL files executed by the guarded
  canonical Python activation/rollback utilities. There is no matching
  application migration record and no `supabase_migrations.schema_migrations`
  relation; the catalog migration tables belong only to auth, realtime, and
  storage subsystems.
- Current migration state: phase columns absent from both tables and
  `mlb.canonical_game_phase_v1` absent.
- Relevant external relation locks: 0; advisory locks: 0.
- Other sessions observed: 6, with query text and client addresses deliberately
  omitted from evidence.

`mlb.game_info` has 10,337 rows, 10,337 distinct gamePks, no duplicate gamePk,
and 2,809 exact intersections with the 2,919-game proposal. Its primary and
unique key are both `game_id`; current secondary indexes cover game date and
home/away team identifiers.

`mlb_cleanroom_v1.games` has 590 append-only observation rows representing 86
distinct gamePks. Eighty-five gamePks have multiple observations, totaling 504
extra observation rows with a maximum of 20 rows per gamePk. These are expected
source snapshots, not identity conflicts. Its primary key is
`(game_pk, source_payload_sha256)`. The `reject_mutation` trigger unconditionally
rejects every update or delete.

The complete columns, data types, defaults, constraints, indexes, keys,
triggers, policies, privileges, sessions, and locks are retained in
`operational_database_read_only_snapshot.json`.

## Proposed mutation counts

The guarded loader proposes no row creation or deletion:

| Table | Updates | Inserts | Deletes |
|---|---:|---:|---:|
| `mlb.game_info` | 2,809 | 0 | 0 |
| `mlb_cleanroom_v1.games` | 590 rows / 86 gamePks | 0 | 0 |

The remaining 7,528 historical/nonproposal `game_info` rows remain unchanged.
The exact 2,919-game manifest is required and verified even though only rows
already present in each governed table are updated. The expected exact-gamePk
view population after activation is 2,809 distinct gamePks.

## Migration and writer review

The prepared migration and rollback were hardened into transaction-neutral SQL
bodies. The activation loader owns one transaction covering explicit
nonblocking locks, migration, exact-gamePk backfill of both tables, validation,
and commit. Direct SQL execution without an enclosing single transaction is
forbidden.

Static checks prove:

- all inserts into the two game tables use named columns;
- complete legacy and complete activated schemas are supported, while partial
  phase schemas fail closed;
- every phase column and index is idempotently declared;
- the exact join view is replaceable and contains no calendar/team fallback;
- no missing source type defaults to `R` and no calendar field derives phase;
- source type, source season, raw round, relationship fields, and source hashes
  are retained;
- existing rows may remain all-null only until the governed backfill, while
  classified existing/future rows must satisfy the type/phase/round contract;
- the rollback removes only V1 view/index/constraint/column objects.

Writers identified:

- `backend/mlb/scripts/insert_mlb_stat_derived.py` for `mlb.game_info`;
- `backend/mlb/scripts/cleanroom_v1/run_cleanroom_source_cycle.py` and
  `admit_exact_roster_bridge.py` for `mlb_cleanroom_v1.games`;
- `cleanroom_game_insert_sql()` is the shared schema-aware named-column insert.

The loader verifies the exact clean-room trigger definition, temporarily
disables only `reject_mutation` while holding an exclusive maintenance lock,
updates phase columns, and re-enables it before commit. Any error rolls back
both data and trigger state. Static compatibility passes; runtime writer
compatibility remains unproven because the isolated PostgreSQL rehearsal could
not run.

## Isolated rehearsal blocker

Only PostgreSQL 17.6 `libpq` client wrappers are installed locally. The
required `postgres` server companion is absent, so `initdb` cannot create a
disposable cluster. Installing a package is prohibited, and operational DDL or
DML—even in a rollback transaction—is also prohibited.

Therefore the following required scenarios remain unexecuted: first and repeat
migration; first and repeat backfill; rollback; reactivation; concurrent writer
compatibility; database-enforced invalid-type failure; exact-gamePk joins; and
zero unintended row creation/deletion. The first failed local initialization
created no schema or rows, and production DDL/DML remained zero.

This is a blocking failure. Static tests do not substitute for a PostgreSQL
rehearsal.

## Tests and recommendation

- New standard-library actual assertions: 15 passed, 0 failed, 0 skipped, 0
  unexecuted.
- Existing canonical phase scenarios: recorded separately by the validator.
- Full preflight validator: `validation_report.json`.

Every abort threshold is zero, including unexpected rows/gamePks, hash
mismatches, missing/unknown/conflicting types, duplicate identities, unexpected
locks, writer incompatibility, row creation/deletion, and a disabled trigger at
commit.

The smallest justified next action is to provide an isolated disposable
PostgreSQL server with matching major-version semantics and rerun the committed
rehearsal. Do not install software or use the operational database for that
rehearsal under this task.

See `RUNBOOK.md` for the prepared but unexecuted activation and rollback
commands.
