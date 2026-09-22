# Architecture Decision Record

## Decision identity

- Contract: `MLB_2026_GAME_PHASE_SIDECAR_DESIGN_V1`
- Status: `SUPERSEDES_DUAL_TABLE_DESIGN; READY_FOR_REVIEW; NOT_IMPLEMENTED`
- Scope: authoritative game-phase persistence and consumer cutover only

## Context

The frozen classifier and retained StatsAPI evidence establish the scientific invariant: phase derives from exact authoritative source type, never from date, schedule status, market participation, or model participation.

The operational metadata snapshot recorded:

- `mlb.game_info`: 10,337 rows and gamePks; 2,809 intersect the 2,919-game proposal;
- `mlb_cleanroom_v1.games`: 590 append-only observations for 86 gamePks;
- 504 clean-room rows are additional observations of an already represented gamePk;
- the clean-room `reject_mutation` trigger rejects every update and delete;
- no phase columns or canonical phase view are currently present.

The rejected migration would update both tables, temporarily disable the append-only trigger, and rebuild authority by unioning duplicated columns. Its resulting view was expected to expose 2,809 gamePks, not the complete 2,919-game authoritative population. Repository tracing found no production consumer of that proposed view or its stored `season_phase` columns.

Observed contamination hazards instead exist at consumer boundaries: missing type defaults to `R`, a stat-derived regular-season option admits postseason, and an agreement-study label derives postseason from calendar month.

## Priority 0: automatic migration discovery

Searches covered GitHub workflows, Make targets, shell and Python programs, Supabase layout, migration/version references, SQL execution calls, and direct references to the rejected migration.

Finding: **not automatically discoverable or applicable**.

The directory `backend/mlb/sql/migrations` is an operator-managed SQL collection, not a configured migration-runner input. The only executable path found for this migration is explicit manual selection by `activate_mlb_canonical_phase_v1.py` or direct operator `psql` use. The guarded loader does not discover the file: it requires `--migration`, `--execute`, and `--authorization-phrase EXECUTE_MLB_2026_CANONICAL_PHASE_ACTIVATION_V1` plus exact hashes and counts.

If repository automation later begins globbing this directory, the rejected migration becomes an immediate blocker and must be quarantined before that automation is enabled. This design does not authorize moving or editing it now.

## Decision

1. Preserve authoritative phase once in `mlb.game_phase_authority_v1`, keyed by exact `game_pk`.
2. Keep `mlb.game_info` unchanged.
3. Keep `mlb_cleanroom_v1.games` physically append-only and unchanged.
4. Never derive authority by unioning duplicated phase columns.
5. Expose a read-only `mlb.canonical_game_phase_v1` consumer view backed only by the sidecar.
6. Use the same application interface for the current hashed file proposal and the future database view.
7. Apply phase only at admission, grading, evaluation, reporting, close, and feature-eligibility boundaries. Do not insert phase into probability computation.

## Requirement classification

### Required invariant

- exact gamePk identity;
- exact source type and source season;
- deterministic normalized phase and postseason round;
- raw round description and evidence identity retained;
- absent, missing, unknown, special, or conflicting phase never treated as regular season;
- rescheduled/resumed relationships cannot alter phase;
- strictly prior 2027 feature eligibility remains separate from 2026 evaluation membership.

### Observed contamination defect

- missing-to-`R` defaults in evaluation/research paths;
- postseason admitted by a regular-season-named stat-derived filter;
- calendar-month postseason reconstruction;
- reports without mandatory phase partitions.

### Currently blocked risk

- Moneyline, totals, grading, close, and combined reporting do not yet have one enforced canonical join;
- the current 2,919-game proposal ends at 2026-09-27 and contains no postseason rows;
- file-backed authority becomes stale as soon as a retained authoritative game is not represented.

### Future postseason requirement

- prospective admission from every new retained StatsAPI schedule/feed observation;
- round-separated postseason reporting;
- reusable exact-gamePk eligibility for 2027 strict-prior features.

### Optional hardening

- duplicated stored columns in canonical tables;
- clean-room historical mutation;
- trigger-disable recovery machinery;
- dual-table activation/rollback rehearsal;
- database observation ledger duplicating immutable retained source artifacts.

## Consequences

Positive:

- complete 2,919-game authority can exist independently of whether a game is already present in another table;
- the clean-room immutability claim remains literally true;
- one classification cannot drift across two tables;
- deployment is additive and consumer cutover can be incremental;
- phase activation does not alter model inputs or probabilities;
- rollback is consumer routing, not restoration of rewritten source rows.

Costs:

- one new relation and one view eventually require design review, privileges, migration testing, and deployment;
- prospective producers must share an admission contract;
- consumers must treat a missing join as an error;
- a provider correction requires governed revision evidence rather than an ordinary upsert.

## Rejected alternatives

- Dual-table columns: unnecessary duplication and clean-room mutation.
- View directly over repository JSON: not a stable hosted-Supabase interface.
- Permanent file-only authority: sufficient for bounded offline reports but weak for shared prospective consumers.
- Doing nothing: safe only while every affected evaluation and report remains blocked.
