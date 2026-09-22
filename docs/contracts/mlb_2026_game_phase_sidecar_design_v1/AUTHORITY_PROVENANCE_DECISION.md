# Authority and Evidence Provenance Decision

## Decision

Use one current authority row per gamePk and retain observation history in the existing immutable source files and source-hash manifests. Do not add a PostgreSQL observation ledger in V1.

## Why another ledger is unnecessary

The completed source package already provides:

- 464 retained source files;
- exact byte SHA-256 for each file;
- 9,092 schedule observations;
- 2,919 distinct gamePks;
- 6,173 consistent repeat observations;
- zero duplicate-identity conflicts;
- every proposal row's chosen source path/hash and all supporting source paths/hashes.

A database observation ledger would repeat evidence already preserved more faithfully as exact provider bytes. It would also create a second ingestion and retention obligation and raise questions about whether the database or raw bytes are authoritative.

The division of responsibility is:

- raw files: immutable provider observations;
- retained-source manifest: file identity and completeness;
- proposal/admission package: deterministic reduction from observations to one candidate per gamePk;
- sidecar: current operational authority and fail-closed status;
- conflict/correction package: immutable evidence for exceptional decisions.

## Admission evidence chain

Every admitted row must be reproducible through this chain:

1. exact provider response bytes;
2. response SHA-256;
3. retained-source manifest entry;
4. exact gamePk observation within that response;
5. frozen contract/version and contract hash;
6. normalized authority record;
7. proposal/admission batch SHA-256;
8. sidecar admission identity.

If any link is missing or mismatched, admission fails.

## Repeated observations

Repeated identical classifications are evidence of consistency, not separate authority records. They are checked during proposal/admission validation and remain in the manifest. They do not cause a database write.

## Conflicts and corrections

A new conflict must not be hidden by the previously admitted row. The future admission operation must emit immutable failure evidence and ensure the operational authority status is fail-closed. The original core values remain available in the sidecar and their raw evidence remains immutable.

A confirmed provider correction requires a separate decision package containing:

- old authority revision and core hash;
- new provider observation and raw SHA-256;
- explanation of why it is a correction rather than a conflict;
- proposed new core values and core hash;
- explicit approval identity and timestamp;
- affected consumer/cohort diff;
- proof that no other gamePk changes.

Ordinary acquisition may never execute this correction path.

## When a database observation ledger would become justified

Reconsider only if one of these observed requirements appears:

- retained raw files cannot meet required availability or retention guarantees;
- consumers require temporal “authority as of observation time” queries inside PostgreSQL;
- provider corrections become frequent enough that external correction packages are operationally unsafe;
- multiple providers become co-equal authorities requiring database-time reconciliation;
- compliance requires database-resident evidence history.

None is currently observed.
