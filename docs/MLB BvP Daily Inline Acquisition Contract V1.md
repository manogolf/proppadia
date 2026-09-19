# MLB BvP Daily Inline Acquisition Contract V1

Frozen contract: `MLB_BVP_DAILY_INLINE_ACQUISITION_V1`.
Operating timezone: America/Los_Angeles. Effective slate date: **2026-09-19**.
September 18's manual acquisition is retained and is not repeated or certified
retroactively by this change. No production acquisition occurs during installation.

## Architecture and ownership

| Component | Authority and dependency |
| --- | --- |
| `com.proppadia.mlb.refresh.daily` | Existing installed daily wrapper; 05:30, 08:30, 11:00, 13:00, 16:30 local |
| Daily wrapper | Owns existing `mlb-daily-refresh` and `mlb-pipeline` locks |
| Moneyline + agreement capture | Existing earlier ordering retained to preserve durable-barrier timing; independent of BvP |
| Roster refresh | Existing four attempts and failure semantics unchanged |
| `bin/mlb_bvp_inline_daily_hook.sh` | Immediately after successful roster refresh, before derived features/scorers; verifies parent shared lock, owns `mlb-bvp-prewarm` lock |
| `run_mlb_bvp_inline_daily.py` | Calls unchanged canonical `_build_rows_for_date` and `_upsert_rows`; verifies committed rows and source journal |
| Local acquisition ledger | Durable date claims, results and success receipts; acquisition lock spans eligibility through publication |
| Three canonical feature readers | Existing `bvp_identity.certified_rows` plus exact same-date inline receipt row/hash admission |
| Qualified BvP-dependent scorer | Requires certified same-date state and declared BvP features; no heuristic fallback on dependency block |
| Independent Totals/market/Hits lanes | Existing execution order and rules unchanged; inline failure is visible WARN, not a false model skip |

Daily and retired prewarm wrappers remain **installed-owned**, not repository-owned.
Complete `.txt` sources in the reconciliation package are non-executable deployment
and rollback evidence, not competing executable templates. Existing PostgreSQL
schemas and historical data are unchanged. Source acquisition target remains
`mlb-bvp-pvb-refresh` / `refresh_mlb_bvp_pvb.py`; the inline adapter calls its
canonical implementation directly without running legacy downstream prewarm work.

Current qualified MLB model authority is `NO_QUALIFIED_MLB_MODEL`. Acquisition is
independent of that authority; model application and impact remain governed skips.
The current independent Hits parent model uses player-stat inputs, not BvP rows.

## Runtime evidence

September 11–17 successful acquisition including Make startup: 71, 85, 70, 51,
73, 78, 46 seconds; median 71, maximum 85. Their downstream prewarm work took
9m52s–16m05s, full wrappers 10m38s–17m23s. September 18 manual BvP-only run:
17:12:42–17:13:44 UTC (10:12:42–10:13:44 PT), **62 seconds**, 392 successful
fresh BvP fetches and 1,937 written rows. It was neither dry-run nor cache-only;
in-run duplicate-pair caching was retained. Identity/pitcher limitations remain.
See `retained_runtime_evidence.json` for segment hashes and exact counts.

## Durable state and schema

Authority: `artifacts/ops/bvp_inline_v1/acquisition.sqlite3`, permission 0600,
private directories 0700. SQLite foreign keys and synchronous FULL are enabled.
This local operational schema stores claims/receipts, not replacement baseball
data. Existing source identity journals alone cannot provide transactional date
claims; this ledger fills that missing role without changing PostgreSQL schemas.

| Append-only table | Key / content |
| --- | --- |
| attempts | attempt_id; date, type, window, automatic flag, authorization ID, canonical claim JSON/SHA-256 |
| events | event_id; attempt FK, immutable event JSON/SHA-256 |
| results | attempt FK/PK; immutable result JSON/SHA-256 |
| successes | slate-date PK, unique result FK; immutable receipt JSON/SHA-256 |

Unique automatic date/window and manual date/authorization indexes prevent replay.
Eight UPDATE/DELETE rejection triggers protect retained evidence. A stable
OS `flock` inode spans eligibility, BEGIN IMMEDIATE claim, acquisition, verification,
and transactional receipt publication. OS lock release follows exit/crash; the
inode is never deleted. Canonical shell lock directories retain their existing
stale-owner policy; a killed wrapper can require an operator lock diagnosis.

Claims commit **before** any request. Results record attempt/run/date, actual claim,
request-attempt and acquisition timestamps, retry telemetry, games, prepared/written
rows, identity/starter exclusions, empty responses, code/source/journal hashes,
classification and actual collection time. Success receipt is admitted only after
committed source journal and exact read-back row identity/features/hash validation.
Rows alone, logs alone, partial upserts or invalid identity cannot establish success.
Read-only admission recomputes receipt hashes; filesystem metadata is used only to
invalidate a bounded row-lookup cache, never as certification authority.

## Bounded attempt policy

First eligible daily window for a current effective slate date claims PRIMARY
(normally 05:30). A failed primary permits **one** RECOVERY at the next strictly
later natural window (normally 08:30). Maximum **two automatic acquisitions per
date**. Same-window process restarts cannot retry. After two failures remaining
windows do not acquire. A certified success causes all later windows to skip.
If the first dispatch is missed, the first actual eligible window is primary;
timestamps always reflect actual collection. No future-window or backdated claim.

Manual recovery requires explicit user authorization, a once-only authorization
ID, the same ledger and both governed locks. It does not reset the automatic
budget. Existing success blocks manual reacquisition too. Operator entry:
`bin/mlb_bvp_inline_manual_recovery.sh YYYY-MM-DD AUTHORIZATION_ID`;
do not invoke without separate authorization.

Statuses: `BVP_INLINE_PRIMARY_SUCCESS`, `BVP_INLINE_SUCCESS_ALREADY_EXISTS`,
`BVP_INLINE_PRIMARY_FAILED`, `BVP_INLINE_RECOVERY_SUCCESS`,
`BVP_INLINE_RECOVERY_FAILED`, `BVP_INLINE_NOT_DUE`,
`BVP_INLINE_IDENTITY_VALIDATION_FAILED`.
Failure is not relabeled `SKIPPED_NO_QUALIFIED_MODEL`. Current-day Ops Brief
reads the date ledger, not historical prewarm stderr. A BvP-only wide dependency
block has its own exit 76 marker and is converted to a governed skip only if
prediction artifact hashes did not change; ordinary technical failures stay nonzero.

## Preserved acquisition and certification

Complete canonical collector bytes remain SHA-256
`5e6a9bf7a7204ee650b56c4e08b6b1918b06c6833788256e5ef5016c2f58de36`.
`BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1`, game-ID/date/off-date gate, probable-starter
exclusion journal, empty-response journal, feature formulas, request policy and
idempotent upserts are retained. Missing optional local ID never replaces official
MLB identity. Missing opposing starter is a journaled subset skip, not fabrication.
Legacy September 18 quarantine and historical certification limits are unchanged.
Future-date runtime hydration rejects prior-date and wrong-game BvP fallback rows;
missing game identity/date cannot satisfy a declared dependency. Historical replay
and unrelated rolling-feature fallback retain their original behavior.

Crash before request consumes its durable attempt; one later recovery remains.
Crash after partial upserts likewise has no success receipt; later recovery uses
canonical idempotent upserts and full read-back certification. Crash after writes
but before result publication must not be inferred successful. Claim-write failure
prevents requests. Failure to publish a durable result remains fail-closed.

## Retirement and discovery limits

Only `com.proppadia.mlb.bvp.prewarm.daily` is disabled and booted out. Its original
03:30 plist bytes and historical logs/data remain. The installed prewarm wrapper
has an immediate retirement guard (exit 78) before environment, locks or requests;
its complete original body remains beneath the guard for exact rollback.
Daily schedules, NHL schedules and 05:27 wake are unchanged.

Relevant visible launchd/plist, repository, user cron, shell-startup and Automator
references were inspected: only the dedicated legacy plist automatically invokes
the prewarm wrapper. User cron is empty. Root cron has retained September 2
interactive verification as empty, not current re-verification. Protected Shortcuts
storage is not readable under current TCC permissions; no exhaustive claim is made.
The direct-invocation retirement guard prevents an undiscoverable obsolete alias or
shortcut from acquiring through the old wrapper. Historical/manual research
commands are retained, not converted into automatic fallback.
