# MLB 2026 canonical phase source completion V1

Status: **SOURCE COMPLETION READY; NOT ACTIVATED**

Contract: `MLB_2026_CANONICAL_PHASE_SOURCE_COMPLETION_V1`

Evidence date: 2026-09-21 local / 2026-09-22 UTC

The one public MLB StatsAPI schedule request explicitly authorized for this
contract succeeded without a retry. No paid provider was called and no paid
credit was consumed. All subsequent parsing, reconciliation, proposal
building, tests, and validation were offline.

## Acquisition

- Request made: yes.
- Request count: 1; retries: 0.
- Paid-provider requests: 0; paid credits: 0.
- Exact URL: `https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2026-02-20&endDate=2026-03-25`.
- HTTP status: 200.
- UTC request interval: `2026-09-22T01:17:17.248811Z` through `2026-09-22T01:17:17.776856Z`.
- Raw response: `backend/mlb/data/external/statsapi/raw/2026/schedule_2026-02-20_2026-03-25.json`.
- Exact byte count: 642,447.
- SHA-256: `3ca8bff9e4361676ad3b34c5166f23f4d180d809083557ad8498062a9febd091`.
- Acquisition identity SHA-256: `b19445bf3d7587ca4de2c3bae89b982a86bc04a3779df202fbc6cfcfd7a2d611`.

The response was created atomically at an absent destination and was not
overwritten. The repository already ignores `backend/mlb/data/external/`, so
the immutable raw response remains retained locally and is not committed. Its
identity is anchored by `acquisition_receipt.json`, the authoritative source
manifest, and this package's SHA-256 manifest.

## Source reconciliation

The response contains 490 observations and 490 distinct `gamePk` values. All
were classified only from exact authoritative `gameType` through the frozen
phase contract; dates were not used to determine phase.

| Population | Count |
|---|---:|
| Prior missing gamePks | 471 |
| Prior missing gamePks resolved | 471 |
| Prior missing gamePks unresolved | 0 |
| Returned outside the prior missing ledger | 19 |
| Missing or unknown response types | 0 |
| Response duplicate gamePk observations | 0 |
| Source/type conflicts | 0 |
| Duplicate-identity conflicts | 0 |

The 471 resolved identities comprise 440 `S` and 31 `E` values, all normalized
to `PRESEASON`. The complete response comprises 451 `S`, 38 `E`, and one `R`,
normalizing to 489 `PRESEASON` and one `REGULAR_SEASON`. The exact 19 identities
outside the prior ledger are recorded in
`returned_outside_prior_missing_ledger.jsonl`; 18 are additional preseason
identities and one is the already-retained March 25 regular-season identity.

## Rebuilt retained population and proposal

The rebuilt retained corpus has 464 source-hashed files, 9,092 observations,
6,173 consistent duplicate observations, and 2,919 distinct canonical
gamePks. Its authoritative counts are:

- `S`: 451;
- `E`: 38;
- `R`: 2,430;
- `PRESEASON`: 489;
- `REGULAR_SEASON`: 2,430;
- `POSTSEASON`: 0;
- special/excluded: 0.

The canonical universe and the rebuilt offline backfill proposal both contain
2,919 distinct gamePks. All 2,919 have authoritative classifications. The
missing and conflict ledgers are empty; unknown types, source conflicts, and
duplicate-identity conflicts are all zero.

## Tests and validation

- Dependency-free actual assertion scenarios: 25 intended, 25 executed, 25
  passed, 0 failed, 0 skipped, 0 unexecuted.
- Existing canonical-phase validator: 13 checks, status `PASS`.
- Rebuilt coverage-gate validator: 12 checks, status `PASS`.
- Source-completion validator: see `source_completion_validation_report.json`.

Validation uses the canonical repository interpreter only. It rehashes the
ignored raw response, reclassifies all returned games through the frozen
contract, reexecutes the actual assertions, verifies the complete backfill
proposal, checks the retained-source hashes, and confirms the acquisition
client has exactly one request call site and no retry execution.

## Boundaries and recommendation

No migration or canonical backfill was applied. No database was written, no
downstream lane was modified, no daily pipeline or schedule was started, no
missed snapshot was compensated, and no regular-season close, postseason
activation, prediction/model change, publication, upload, wager, or push was
performed.

The source-completion gate is ready. This does not itself authorize canonical
phase activation. The exact next step is the separately governed migration
activation preflight. Do not apply the migration or backfill until that action
is separately authorized.

## Deliverables

- `acquisition_receipt.json`: exact request and response metadata.
- `source_completion_reconciliation.json`: 471-game and retained-population reconciliation.
- `canonical_backfill_proposal/`: complete source-hashed offline proposal.
- `population_reconciliation/`: rebuilt population, empty missing/conflict ledgers, and activation recommendation.
- `returned_outside_prior_missing_ledger.jsonl`: exact 19-game outside set.
- `unresolved_prior_missing_ledger.jsonl`: empty.
- `executed_dependency_free_test_report.json`: executed assertions and counts.
- `coverage_gate_validation_report.json`: rebuilt coverage validation.
- `source_completion_validation_report.json`: full offline source-completion validation.
- `sha256_manifest.txt`: package and ignored raw-response integrity manifest.
