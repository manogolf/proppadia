# Proposed Implementation Sequence

No step below is authorized by this design task.

## 0. Review and freeze architecture

- Review this package.
- Confirm that the dual-table migration and its runbook are superseded.
- Recheck automatic migration discovery before any deployment tooling change.

## 1. Smallest justified first implementation task

Implement only the stable read-only application authority interface with the hashed-proposal backend, then cut over one observed defect: Hits candidate evaluation's missing-type-to-`R` filter.

Boundaries:

- no database change;
- no API request;
- no scheduler change;
- proposal and source-manifest hashes pinned exactly;
- exact gamePk lookup only;
- typed fail-closed errors;
- regular rows admitted only through positive `REGULAR_SEASON` membership;
- all previously admitted authoritative regular rows retain identical input rows and probability values;
- fixtures cover late/rescheduled regular, every postseason round, special, missing, unknown, conflict, and absent gamePk;
- deterministic before/after cohort identity manifest.

This task creates a real consumer of the stable semantics while minimizing blast radius. An interface with no consumer would not yet reduce contamination risk; changing many consumers at once would obscure cohort differences.

## 2. Complete Priority 1 file-backed cutovers

- remove every missing-to-`R` default identified in the cutover map;
- make stat-derived regular selection positive-membership-only;
- replace calendar-derived agreement phase;
- build close and report partitions from the same authority interface;
- retain exact exclusion ledgers for every failed lookup.

No prediction model, artifact, threshold, or scoring feature changes.

## 3. Review additive sidecar physical design

Prepare—but do not yet apply—a new additive migration for only:

- `mlb.game_phase_authority_v1`;
- its constraints and minimal index/primary key;
- a read-only `mlb.canonical_game_phase_v1` view sourced only from the sidecar;
- least-privilege grants and any required RLS policy.

The rejected migration must not be edited into this shape; use a new identity after review.

## 4. Isolated sidecar rehearsal

Test the material risks:

- first and repeated additive migration;
- initial and repeated 2,919-row admission;
- exact-gamePk uniqueness;
- missing/unknown/special/conflict behavior;
- identical repeat no-op;
- concurrent prospective admissions;
- conflict-block status behavior;
- evidence-only null enrichment;
- governed correction rejection without explicit authorization;
- view privileges and fail-closed membership;
- rollback by backend routing and additive-object removal in an isolated clone.

There is no clean-room trigger scenario because clean-room is not modified.

## 5. Sidecar load and parity proof

- verify exact file hashes;
- load all 2,919 authority rows through a separately guarded transaction;
- create zero rows in any existing table;
- prove file/backend key and field equality;
- prove identical cohort membership hashes and typed failure results;
- leave consumers on file backend until review accepts parity evidence.

## 6. Prospective producer admission

Route retained, hashed schedule/feed classifications from the stat-derived and clean-room producers through one shared admission contract. Repeated identical observations are no-ops; conflicts block.

Do not make producer success depend on mutating historical clean-room rows.

## 7. Backend switch and Priority 2 cutovers

- switch the stable interface from file to database after parity passes;
- cut over Moneyline, totals, grading, Full-board Hits, external splits, Ops Brief, and daily indexes;
- require probability and immutable prediction/outcome hashes to remain unchanged.

## 8. Priority 3 cutovers

- BvP reporting;
- feature-lineage health;
- market coverage;
- separately governed 2027 strict-prior feature eligibility.

## 9. Retire temporary authority operational use

Retain every file-backed package as immutable evidence. Stop using it operationally only after prospective sidecar admission and all required consumer parity gates have passed.
