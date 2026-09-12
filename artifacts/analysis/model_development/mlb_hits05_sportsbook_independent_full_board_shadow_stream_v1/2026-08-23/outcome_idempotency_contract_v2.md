# Hits 0.5 substantive outcome idempotency contract

Contract version: `HITS05_SUBSTANTIVE_OUTCOME_IDEMPOTENCY_V2`

This contract changes comparison behavior only. Existing outcome rows, their
payload bytes, and their recorded SHA-256 hashes remain legacy evidence under
`HITS05_FULL_BOARD_OUTCOME_PAYLOAD_LEGACY_V1` and are not rewritten.

## Substantive identity

The substantive SHA-256 is calculated from canonical JSON containing exactly:

- contract version;
- canonical prediction identity;
- slate date;
- MLB game identifier;
- MLB player identifier;
- proposition family (`hits`);
- target line (`0.5`);
- actual hits, normalized as a number or null;
- appearance state;
- outcome/finalization state;
- grading source authority; and
- stable source-state SHA-256.

The stable source-state SHA-256 contains source authority, source contract,
the canonical actual-value identity count and identity-state SHA-256, plus the
row's actual sample count and distinct-value count. For minimal legacy payloads
that predate embedded source state, the originally recorded grading-source
SHA-256 remains the explicit compatibility fallback.

## Excluded operational metadata

The substantive comparison excludes:

- completeness-file path and file-content hash;
- completeness-file modification time;
- artifact rewrite time;
- grading execution time;
- later verification time;
- report-generation time;
- temporary paths; and
- versioned payload-envelope fields and full-payload SHA-256.

These values may remain in the immutable payload or a grading report for
provenance, but cannot create a substantive outcome conflict.

## Timestamp provenance

`grading_timestamp_utc` is the first grading execution time for newly admitted
V2 rows. `timestamp_provenance` distinguishes the first grading time,
authoritative source observation time, later verification time, and
completeness-file modification time. A filesystem mtime is never labeled as an
authoritative source observation time. When the source supplies no canonical
observation timestamp, that field is null. Later checks return a verification
timestamp in the grading summary and do not replace the stored first grading
time or create a second outcome row.

## Compatibility and classifications

Validators first verify every row's original full-payload hash. Rows without a
V2 marker are reported as legacy V1 and their hashes are not reinterpreted.
V2 rows additionally verify their declared stable-source and substantive
hashes.

Existing identities are classified as one of:

- `IDEMPOTENT_SUBSTANTIVE_MATCH`
- `METADATA_ONLY_DIFFERENCE`
- `OUTCOME_VALUE_CONFLICT`
- `APPEARANCE_STATE_CONFLICT`
- `IDENTITY_CONFLICT`
- `SOURCE_PROVENANCE_CONFLICT`
- `UNRESOLVED_CONFLICT`

The first two insert no row and do not fail daily integrity. Every conflict
classification remains fail closed and preserves the retained row.

## Rollback

Revert the implementation commit to restore the legacy full-payload equality
comparison. Do not restore the ignored SQLite ledger from Git and do not alter
existing outcome rows. Before rollback, retain the ledger SHA-256 and verify
that the outcome count and each recorded payload hash remain unchanged.
