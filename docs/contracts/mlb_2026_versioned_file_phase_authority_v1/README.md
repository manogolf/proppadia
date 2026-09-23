# MLB 2026 Versioned File Phase Authority V1

This change implements immutable, full-file authority snapshots without
activating a child snapshot. The operational selection remains pinned to the
new V1 descriptor, which in turn references the original proposal and source
manifest at their original paths and exact hashes.

## V1 preservation

- Proposal: 2,919 rows and 3,835,134 bytes; SHA-256
  `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`.
- Source manifest: 464 files and 9,092 observations; SHA-256
  `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`.
- Authority-record hash:
  `5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84`.
- Counts remain 2,430 regular-season and 489 preseason games, with no
  postseason authority in V1.
- Every original proposal line remains byte-identical. The proposal and source
  manifest were not regenerated, reordered, normalized, relocated, or edited.

The governed descriptor is
`backend/mlb/season_transition/authority_snapshots/v1/descriptor.json`, SHA-256
`543fda06d3c066bb6f0608ee8c05987829216fa441459a8fc08b4f87ee8c245a`.
The active selection points only to that descriptor and is
`PINNED_V1_NOT_ACTIVATED_CHILD`.

## Offline builder

`build_mlb_versioned_file_phase_authority_v1.py` accepts one explicit,
hash-bound source-set JSON and an absent output directory. It reads retained
StatsAPI schedule files only. It has no network or database client and reports
zero network/database requests.

For a valid child it:

- copies the complete parent proposal and source manifest as byte prefixes;
- appends only previously absent exact gamePks;
- retains raw game type, source season, raw round, scheduled start, source
  hashes/paths, and rescheduled/resumed relationships;
- derives broad phase and normalized postseason round only through
  `contract_v1`;
- rejects changes to an existing identity or classification;
- emits proposal, source manifest, candidate descriptor, validation report,
  and SHA-256 manifest into the explicit output directory; and
- never reads or writes the active-selection path.

If verified inputs contain no new exact gamePk, it returns
`NO_NEW_AUTHORITY_EVIDENCE` and creates no output directory.

Synthetic fixtures validated all supported postseason wire types (`F`, `D`,
`L`, `W`, `P`, and `C`) and a rescheduled regular-season game after September
27. They prove code behavior only and are not operational postseason evidence.

## Loader and close integrity

The no-argument `HashedProposalAuthority` now resolves the configured selection,
verifies the descriptor, every referenced file, the complete parent chain,
counts, classifications, source bytes, and V1 anchors, then exposes the same
`CanonicalGamePhaseAuthority` interface. Invalid configured evidence raises a
typed failure and never falls back.

Explicit proposal/manifest arguments remain solely for existing V1 tamper tests.
Explicit candidate validation uses `VersionedFileAuthority` and must supply the
expected descriptor hash.

The no-argument regular-season close checker bypasses active selection and loads
the exact V1 descriptor/hash directly. Its population remains 2,430 regular
games and its present result remains `REGULAR_SEASON_CLOSE_BLOCKED`; it remains
check-only and cannot create or execute a close package.

## Governance preserved

- Agreement V4 source bytes are unchanged and its horizon remains frozen
  through 2026-09-27.
- Training still admits only authoritative `REGULAR_SEASON` membership.
- No model was trained, fitted, rescored, registered, or replaced.
- No prediction, probability, threshold, price, outcome, metric, selector,
  publication, wagering, schedule, database, or pipeline state changed.
- Completed consumer contracts retaining V1 hashes were not rewritten.
- No candidate snapshot was generated or committed by this task.

## Validation

- New dependency-free versioned-authority suite: 14 passed, 0 failed, 0
  skipped.
- Existing authority/training/close/Moneyline/Hits/Totals/agreement/reporting
  compatibility suite: 135 passed, 0 failed, 0 skipped.
- Total executed: 149 passed, 0 failed, 0 skipped.
- Network requests: 0; database connections: 0; paid credits: 0; activations: 0.

See `activation_rollback_runbook.md` for future separately authorized actions
and `remaining_activation_blockers.json` for the exact current blockers.
