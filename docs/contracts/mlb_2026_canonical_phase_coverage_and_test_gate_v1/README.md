# MLB 2026 canonical phase coverage and test gate V1

Status: **VALIDATED; ACTIVATION GATE BLOCKED**

Contract: `MLB_2026_CANONICAL_PHASE_COVERAGE_AND_TEST_GATE_V1`

Evidence date: 2026-09-21

No migration, database write, live API call, paid request, daily pipeline, schedule change, postseason activation, regular-season close, prediction change, publication, wager, push, or missed-snapshot compensation was performed. Database access was an explicit `READ ONLY` transaction. Calendar dates select 2026 identity populations and scope the proposed acquisition; they never determine phase.

## Repository safety

- Starting HEAD was `01c610c4ed88c2a7fa814cce3dae6d3e25a5aa46`; it includes commit `01c610c4`.
- Tracked and staged state was clean before work.
- `.venv` remained the same 78-byte symlink, inode `137012602`, mtime `1790025129`, targeting `/Users/jerrystrain/Projects/.proppadia-py311-scipy1152-macos12-arm64-candidate`.
- The pre-existing DH publish lock remained the same empty regular file, inode `135377680`, mtime `1789577504`, SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Neither protected untracked item is part of this package or commit.

## Executed test gate

The standard-library harness imports the actual target test module, supplies only its two used pytest conveniences (`parametrize` and `raises`), invokes each actual test function and parameter case, and executes the original assertions. It does not merely import or compile the file.

| Measure | Count |
|---|---:|
| Intended scenarios | 25 |
| Executed scenarios | 25 |
| Passed | 25 |
| Failed | 0 |
| Skipped | 0 |
| Unexecuted | 0 |

Every existing validator check is reconciled: 13 checks, 13 mapped, zero missing mappings, validator status `PASS`. See `executed_dependency_free_test_report.json`. The gate validator re-executed the assertions and passed all 12 package checks.

## Canonical population reconciliation

The full machine-readable report contains exact `gamePk` intersections and source-only arrays for every population. Principal results:

| Source | Distinct gamePk | With proposal | Source-only | Proposal-only | Authoritative type covered | Type missing | Identity-date range |
|---|---:|---:|---:|---:|---:|---:|---|
| Retained authoritative StatsAPI schedules | 2,430 | 97 | 2,333 | 0 | 2,430 | 0 | 2026-03-25..2026-09-27 |
| Offline proposal | 97 | 97 | 0 | 0 | 97 | 0 | 2026-07-27..2026-08-02 |
| `mlb.game_info` | 2,809 | 97 | 2,712 | 0 | 2,338 | 471 | 2026-02-20..2026-09-20 |
| `mlb_cleanroom_v1.games` | 86 | 86 | 0 | 11 | 86 | 0 | 2026-07-28..2026-08-02 |
| Immutable Moneyline predictions | 633 | 0 | 633 | 97 | 633 | 0 | 2026-08-05..2026-09-21 |
| Immutable Moneyline outcomes | 630 | 0 | 630 | 97 | 630 | 0 | 2026-08-05..2026-09-20 |
| Normalized retained games | 1,528 | 0 | 1,528 | 97 | 1,528 | 0 | 2026-03-25..2026-09-22 |

Eleven additional immutable SQLite inventory tables were enumerated read-only. Their distinct populations range from 139 to 631 games; every identity is covered by retained authoritative schedule type. Exact populations and pairwise intersection counts are in `population_reconciliation/canonical_population_reconciliation.json`.

The authoritative schedule corpus is 463 source-hashed files containing 8,602 observations of 2,430 distinct games. It has 6,172 consistent duplicate observations, zero missing/unknown type observations, zero identity/type conflicts, and exact authoritative counts of 2,430 `R` / `REGULAR_SEASON`, zero known postseason, and zero known special events.

The canonical union is 2,901 distinct `gamePk` values. Only 2,430 have retained authoritative type, leaving exactly 471 unclassified identities. All 471 occur in `mlb.game_info`; their observed identity dates span 2026-02-20 through 2026-03-25. Those dates are not used to infer that they are preseason or any other phase. The exact set is `population_reconciliation/missing_game_pk_ledger.jsonl`.

Therefore, the existing 97-game proposal is conclusively a retained clean-room-file subset, not a population-complete canonical backfill proposal.

## Missing and conflict ledgers

- Missing authoritative type: 471 unique `gamePk` rows.
- Missing/unknown retained schedule observations: 0.
- Duplicate identity/type conflicts: 0.
- Normalized-versus-schedule type conflicts: 0.
- Conflict ledger rows: 0.

An empty conflict ledger does not clear activation because the missing ledger is nonempty.

## Smallest source-completion proposal — not executed

One free authoritative request is the smallest proposed completion acquisition:

- Provider: MLB StatsAPI.
- Endpoint: `/api/v1/schedule`.
- Parameters: `sportId=1`, `startDate=2026-02-20`, `endDate=2026-03-25`.
- Expected requests: 1.
- Expected paid credits: 0.
- Target set: 471 gamePks; sorted-set SHA-256 `3fd0199caa8240d43d0c89ceb99da124444686162c59c0b9bf7ebfc8ef624eb8`.
- Proposed storage: `backend/mlb/data/external/statsapi/raw/2026/schedule_2026-02-20_2026-03-25.json`.
- Hashing: SHA-256 over exact response bytes before parsing.
- Idempotence: refuse overwrite; reuse only byte-identical retained content; require every target identity, `season=2026`, exact contract-V1 type, zero type conflicts, and a source hash before rebuilding the gate.

The date range scopes acquisition only. It does not classify any game. This request was not made and is not authorized by this package.

## Activation recommendation and exact next step

Activation is blocked because 471 existing canonical identities cannot be classified from retained authoritative evidence. Passing code tests cannot substitute for population coverage.

Exact next step: review and separately authorize the one-request, zero-paid-credit StatsAPI source-completion proposal; retain and hash the exact bytes; rebuild the canonical proposal and this gate; and require zero missing, unknown, and conflict rows before migration activation. Do not compensate for September 21 missed snapshots.

## Reproduction

Only the canonical interpreter is supported:

```bash
PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.run_mlb_canonical_phase_stdlib_tests_v1

PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.validate_mlb_canonical_phase_coverage_and_test_gate_v1
```

The database collector is separately invoked with configured credentials and always begins `READ ONLY`. Rebuilding population evidence does not authorize the proposed source-completion request.

## Deliverables

- Executed assertion report: `executed_dependency_free_test_report.json`
- Read-only database snapshot: `database_population_snapshot.json`
- Population reconciliation: `population_reconciliation/canonical_population_reconciliation.json`
- Exact missing ledger: `population_reconciliation/missing_game_pk_ledger.jsonl`
- Conflict ledger: `population_reconciliation/conflict_ledger.jsonl`
- Retained schedule hash inventory: `population_reconciliation/authoritative_schedule_source_manifest.jsonl`
- Proposed completion acquisition: `population_reconciliation/source_completion_acquisition_proposal.json`
- Activation recommendation: `population_reconciliation/activation_recommendation.json`
- Validation report: `validation_report.json`
- Package manifest: `sha256_manifest.txt`

CANONICAL_PHASE_ACTIVATION_GATE_BLOCKED
