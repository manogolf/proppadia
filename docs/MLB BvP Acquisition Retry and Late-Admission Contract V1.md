# MLB BvP initial-schedule retry and late-admission review V1

Authorized September 18, 2026. Retry contract: `BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1`. September 18 recovery decision: `BVP_RECOVERY_CONTRACT_REQUIRES_AMENDMENT`; no recovery performed. This document does not authorize a network request, data write, late population admission or changed downstream eligibility.

## Narrow correction and installed ownership

The loaded 03:30 local agent calls `/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh`, which acquires the BvP and shared pipeline locks and invokes `make mlb-bvp-pvb-refresh`. That target calls the tracked `backend/mlb/scripts/refresh_mlb_bvp_pvb.py`. The changed function is `_fetch_initial_schedule_json`, called only by `_fetch_schedule_games`. Other roster/vsPlayer reads retain `_fetch_json` and its original retry contract. Models, grading, database writes, market acquisition and publication do not gain retries.

The installed wrapper is unchanged, SHA-256 `23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb`, executable mode 0755. Its September 10 reconciliation manifest remains authoritative for installed bytes and rollback. No tracked competing wrapper or installation was created. The new tracked source is used at the next natural invocation without reloading launchd.

## Frozen initial-request policy

- Maximum attempts: three; a caller explicitly requesting fewer attempts keeps that lower limit. Higher counts are capped at three for this initial request only.
- Waits after completed transient failures: 10 seconds, then 20 seconds. Maximum added retry delay is 30 seconds (25.5 seconds more than the previous default waits).
- Maximum added pre-response wall time after the first completed failure: approximately 40 seconds (30 seconds of waits plus two 5-second request limits), constrained by the total deadline.
- Per-attempt pre-response wall limits: first attempt min(configured timeout, 20 seconds); second and third min(configured timeout, 5 seconds). Default sequence is 20/5/5 seconds. Limits also respect remaining monotonic budget.
- Maximum pre-response retry window: approximately 60 seconds, including request waits and backoff, plus ordinary thread scheduling/logging overhead. A monotonic deadline prevents admission beyond the budget. This is not a 60-second limit on the entire BvP acquisition or subsequent valid-response parsing.
- Each attempt makes the actual governed schedule GET, with `stream=True` and a urllib3 total/connect/read timeout. There is no ping, separate DNS probe, readiness request, sleep assertion or paid market request.
- Because an OS DNS lookup can ignore requests' timeout, a daemon worker is restricted to fetching this GET's response headers. The caller's queue wait enforces its wall limit. An unfinished worker is an **immediate fail-closed technical failure**, not permission to start a second request. It cannot parse BvP inputs, acquire downstream data or write rows; a late response is closed. The CLI exits nonzero and its unchanged wrapper releases locks. An unresolved public schedule request might complete before process termination, but no concurrent replacement is admitted.
- Clearly typed pre-response retry classes: `requests.Timeout` without a response; `requests.ConnectionError` containing socket.gaierror/urllib3 NameResolutionError; or NewConnectionError/typed ENETDOWN, ENETUNREACH, EHOSTUNREACH, ECONNREFUSED, ECONNRESET, ECONNABORTED or ETIMEDOUT. Exception chains/args are inspected for types, not string matching.
- Non-retryable: HTTP response errors (including 401/403/429/5xx), SSL/proxy failures, an exception carrying a response, unclassified connection errors, unfinished requests, programming exceptions, valid-response body-read/JSON/schema failures, database/partial-write/integrity failures. Header receipt is the request retry boundary; after it, the loop never re-enters.
- Initial schedule must be a JSON object with a dates list. A legitimate empty dates list remains a valid empty slate. Failed or malformed responses are not fabricated into an empty slate.

Telemetry contains stage `INITIAL_SCHEDULE_FETCH`, UTC timestamp, attempt, fixed sanitized classification, next wait and response-received state. Unknown response state on an unfinished request is explicitly `UNKNOWN`, not guessed false. No URL, exception message, body, address, identifier, environment value or credential is logged by this layer. Terminal exceptions suppress raw chained messages.

Recovered initial request: `ACQUISITION_SUCCESS_AFTER_TRANSIENT_NETWORK_RETRY`, attempts used, accumulated retry delay, first-failure timestamp and successful-attempt start timestamp; the event timestamp records completion. This describes the initial schedule stage, not blanket success of later acquisition. First-attempt success emits `INITIAL_SCHEDULE_REQUEST_SUCCESS`.

Completed transient attempts exhausted: `ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED`, raised as a sanitized acquisition exception. Non-retryable and unfinished requests also remain nonzero. The installed wrapper records acquisition FAILED, downstream/impact NOT_STARTED, releases locks, and creates no successful DONE marker. A later successful acquisition still reaches the unchanged `SKIPPED_NO_QUALIFIED_MODEL`/impact skip and exit-zero behavior.

## Duplicate, date and durable-write safety

Retries precede successful schedule parsing, local-game mapping and BvP acquisition. They cannot repeat a database write or durable admission; this path has no acquisition claim to duplicate. All date/range selection, player/game mapping, per-row construction and the existing upsert key remain unchanged. The entire requested set is assembled before `_upsert_rows` in main. No request retry wraps that function. The existing table is mutable operational storage, not an append-only evidence ledger; this correction does not change or retroactively certify that fact.

The installed BvP/pipeline locks remain held during retries. Later daily fallback remains disabled by default and is not enabled or made missing-only by this change. No later daily invocation is introduced. Expected 03:30 dispatch remains a calendar event that does not wake a sleeping Mac: sleep is acceptable, delayed wake/coalesced execution is explicit, and actual acquisition timestamps must never be replaced with 03:30. Retry resilience does not promise exact 03:30 execution.

## September 18 late-admission decision

Authorities reviewed: `bvp_data_production_alignment_audit.md`, `bvp_lineage_recovery_window_dry_run_summary.md`, `docs/MLB Daily Workflow Status.md`, the installed wrapper, Makefile and acquisition/lineage implementation. The approved historical lineage recoveries use exact prediction/run-tag-aligned retained inputs or prepared feature exports; DB-only or freshly refetched reconstruction is excluded. Those approvals are not a general late StatsAPI acquisition contract.

The intended observation time is material for reproducibility, population and contamination checks. Actual source observation time is required, not just slate date, a feature named `prior`, or filesystem mtime. Schedule/probable starters, active rosters and optional DB starter references can change after 03:30. No confirmed-lineup freeze is implemented by this BvP collector. Career vsPlayer requests have no implemented slate-date/as-of cutoff; consequently all returned statistics cannot be certified strict-prior by this code, especially after same-day play. Even a pregame later fetch cannot prove an identical original population or source state.

The failed initial schedule request retained no complete original BvP source population. Current mutable feature-table upserts cannot reconstruct it. Partial unstarted-game recovery is not authorized as a replacement daily observation under the existing contract. A separately labelled operational collection may be conceptually useful, but the existing target lacks missing-only/per-game timing admission and immutable recovery provenance. September 18's intended observation therefore remains explicitly missing; no new contract below admits it retroactively. No recovery command is represented as governed or equivalent.

## Future-only prospective amendment proposal

Identifier: `BVP_LATE_OPERATIONAL_CAPTURE_AMENDMENT_V1`. State: **PROPOSED — NOT ACTIVATED**. Scope: future missed captures on slate dates September 19, 2026 or later, only after separate review/authorization. This proposal does not alter a frozen contract, production eligibility, schema, schedules or September 18 evidence.

1. Keep the original missed observation and its failure/run identity immutable; never mark a late fetch as the original capture or assign it 03:30.
2. Separately authorize network/write access and a bounded BvP-only entry point; do not use the full prewarm wrapper or automatically enable the daily fallback. Existing BvP/pipeline locks must serialize it.
3. Record `LATE_PREGAME_OPERATIONAL_CAPTURE_NOT_ORIGINAL_SNAPSHOT`, a distinct actual-time identity, slate date, original failed identity, source observation/request/completion times, immutable source bytes/hashes and actual starter/roster population.
4. Admit only games whose authoritative recorded status and scheduled start establish pregame timing at every relevant source observation. Enumerate all excluded/started/unverifiable games and quantify partial coverage. Unknown timing fails closed; no unchanged-population assertion is allowed.
5. Do not assume career statistics are strict-prior: require independently verifiable cutoff authority before any evidence-grade use. An unqualified source remains operational-only even when pregame.
6. Protect existing mutable keys from unintended replacement; specify missing-only storage behavior and durable idempotency before implementing any late collector. Preserve source-authority, identity and append-only downstream constraints.
7. No alteration/backfill of already committed model predictions, prospective risk sets, bookmaker prices, outcomes, selection/publication artifacts or their original lineage. No automatic entry into evidence-grade populations. Any separate evidence admission requires an additional reviewed amendment and qualified source/timing proof.
8. Require offline tests for complete/missing/partial capture, started-game exclusion, ambiguous authority, races, crashes, idempotency, source preservation and frozen-ledger isolation before activation.

## Validation and rollback

Focused offline tests cover first success; DNS, connection and timeout recovery/exhaustion; the September 18 readiness-at-7.4-seconds fixture; HTTP/security/programming/schema/parser/body failures; pre-response wall-limit fail-closed behavior with no replacement request and late-response close; date preservation, one write and no write retry; credential containment; unchanged other-read contract; and the actual installed wrapper with its real lock helper in isolated fixture directories. No actual sleeping/network interruption or external/database request is used.

Installed wrapper mode/hash and September 10 manifest must still match. Source pre/post hashes and test evidence are recorded in `artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_initial_schedule_retry_manifest.json`. Source rollback baseline is commit `502ceb0ef3e1d1fbe7b0e993b737de073f32695f`; SHA-256 `f552c348c4dcf16edaa2ec7a13b63d276110e3cad58a82e96420a44d5e7e1b21`.

Rollback, only with separate approval: revert the local retry-correction commit (`git revert <retry-correction-commit>`), preserving unrelated work and all operational/history evidence, then repeat focused tests, syntax/compilation and wrapper-manifest/hash checks. No installed-wrapper replacement, launchctl reload, power event rollback or data deletion is necessary. The future-only amendment proposal can be retired without removing failed-date evidence.
