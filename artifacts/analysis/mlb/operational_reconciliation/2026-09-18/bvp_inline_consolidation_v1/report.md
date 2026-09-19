# BvP daily inline consolidation V1

Contract `MLB_BVP_DAILY_INLINE_ACQUISITION_V1`, effective September 19 PT.

At the preparation snapshot, dedicated label
`com.proppadia.mlb.bvp.prewarm.daily` was loaded/enabled but not running (last
exit 2), from `~/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist`
and installed-only `/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh` at
03:30 local. Daily label `com.proppadia.mlb.refresh.daily` was loaded/enabled,
not running (last exit 0), with unchanged 05:30/08:30/11:00/13:00/16:30
local schedules and installed-only wrapper.

Recent BvP acquisition median 71s / maximum 85s, versus 10–17 minute old
full-wrapper runtime driven by downstream work. September 18's fresh manual
acquisition was 62s. Consolidation does not repeat or recertify those retained rows.

Placement: unchanged daily/shared locks -> successful existing roster refresh ->
inline canonical schedule/pairing + BvP -> committed-row/source-journal validation
and durable receipt -> dependent feature readers/scorers. Existing earlier
Moneyline/agreement stage remains earlier to preserve capture timing. Independent
market/Totals/Hits ordering, formulas and authority are unchanged.

Date authority is transactional append-only local operational SQLite: one primary,
one strictly later natural recovery, at most two automatic acquisitions per date.
Claims commit before requests; success requires full same-date canonical identity,
source-journal and exact committed-row hash verification. Successful dates skip all
remaining windows. Explicit authorized manual recovery uses the same ledger.

BvP failures remain visible WARN and do not manufacture a model-skip or receipt.
Independent lanes continue; BvP-dependent rows and declared features fail closed
without heuristic fallback. Existing wake-readiness retry and source acquisition
implementation are byte-identical. Current model authority remains no-qualified.

Installation preserves complete old installed daily/prewarm/plist/active-manifest
bytes, metadata and hashes. Only the obsolete dedicated 03:30 BvP label is
disabled/unloaded. Its retained wrapper gets an early retirement guard; original
body, plist, logs and database data remain. No new tracked executable daily/prewarm
source ownership is invented. See reconciliation_manifest.json for actual post-
installation evidence, not merely a proposed state.

Daily five windows, NHL schedules and 05:27 repeating wake remain unchanged.
The protected Shortcuts store cannot be exhaustively inspected; user cron is empty
and root cron has older interactive empty evidence only. The old wrapper guard
prevents an undiscovered obsolete direct invocation from acquiring.

Offline validation: 128 focused tests, including deterministic two-entry/one-
acquisition race; process crashes before request and after idempotent fixture
write; real shell lock ownership/release; canonical collector with mocked official
inputs and committed-row reads; valid optional ID, empty response and missing
starter journals; private-log sanitation; dependent fail-closed; full reverse-diff
wrapper equivalence. Compilation, individual shell syntax and diff checks pass.
No live acquisition, external API/network request or API credit use in this task.

Principal instructions: docs/MLB BvP Daily Inline Acquisition Contract V1.md and
docs/MLB BvP Daily Inline Operator Runbook V1.md. Runbook includes exact rollback
source/hash/topology verification, coordinated code/installed rollback order and
next natural 05:30 process-validation checklist. No historical rebuild performed.

Preserved unrelated worktree changes are outside this commit. Local commit only;
push: no. Manifest excludes its own hash and mutable operational authority file;
the latter is structurally checked with installation-time zero-acquisition counts.
