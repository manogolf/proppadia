# NHL/MLB preseason scheduler guard repair V1

Authorized September 16, 2026, after the completed read-only schedule/collision
audit. No schedule, morning-orchestration, model, grading, credential or market
rule change. No live acquisition or synthetic preseason canary. No commit/push.

## NHL acquisition contract

Before: eligibility/prior-status checks -> HTTP -> capture-processing lock ->
publication -> final status. No durable pre-request claim.

After: nonblocking per-slate acquisition `fcntl` lock -> input export and current
phase evaluation -> existing-phase/legacy-paid-status/claim checks -> create-only
`NHL_PAID_ATTEMPT_CLAIM_V1` claim with file and directory fsync -> durable request
intent -> HTTP -> durable preserved-response status -> existing capture-processing
lock and phase publication -> durable final success/failure -> acquisition unlock.

Acquisition lock: `artifacts/operational/nhl/cross_market_shadow/.locks/<slate>.acquisition.lock`.
Claims: `artifacts/operational/nhl/cross_market_shadow/paid_attempt_claims/<slate>/<phase>_<unique-run>.claim.json`.
Existing capture lock and phase semantics are retained. MIDDAY and FINAL_PREGAME
have independent claim namespaces. Claims contain no credential. A claim means
request intent, not proof of a charge; charge state may remain unknown after crash.
Any claim (including a partial one) blocks automatic retry. Failure to durably
write the claim or pre-request status permits zero requests. Process exit releases
the lock but does not remove claims. The existing explicit operator `--force`
override can authorize a new claim; it is recorded and never bypasses locking.
Never remove a claim merely to trigger automatic retry. All ordinary failures
retain WARN-only CLI exit semantics. Failure to persist status is itself a warning;
the durable claim remains the retry barrier.

## DH shared publication

Prediction/outcome/source-lineage execution and their existing locks are unchanged.
Only publication uses a common blocking `fcntl` lock at
`rolling_forward_evidence_status_v1.json.publish.lock`, unique same-directory
temporary files, fsynced bytes and atomic replacement. Capture/grading computations
do not hold this publication lock. A failed replacement preserves the prior file;
temporary files are cleaned up. This is publication serialization, not a new
cross-ledger transaction or grading rule.

## Installed-agent retirement and rollback

Only these confirmed fixed historical-date, 300-second dry-run agents were disabled
and booted out of `gui/501`:

- `com.proppadia.pregame-lineup-study.20260708`
- `com.proppadia.pregame-lineup-study.20260709`

Their installed plists and historical outputs/logs were not deleted or rewritten.
Current daily/BvP wrappers do not invoke or consume these July polling agents.
Historical packaging/documentation mentions are not active dependencies.

Preserved plist SHA-256:

```text
20260708 4de6abfe1842d13b9def0bf2c366ac204c3666b9b3c29413473f5db2938e624b
20260709 9e439bccd7039e49a621faa83ad38b4d3f31354c1e59799d4a8adebb4c759f0c
```

After separate rollback approval, re-enable and bootstrap only each preserved
label/plist. Example for July 8 (repeat with July 9 only if authorized):

```sh
launchctl enable gui/501/com.proppadia.pregame-lineup-study.20260708
launchctl bootstrap gui/501 /Users/jerrystrain/Library/LaunchAgents/com.proppadia.pregame-lineup-study.20260708.plist
```

Source rollback: reverse only this repair's diff after review; pre-change source
is retained in HEAD. Do not reset unrelated worktree changes or delete claims.
Pre/post SHA-256:

```text
NHL runner before 1474ef3778934337c0f58941f77d118e1d3df4b1fe91f702f9e726adb81a8493
NHL runner after  b119c848530820b9b1482de699baef9bb085acfd72f19bbf394b37b40cc79f6d
DH common before b7b3b5876f2c5777421cd1999e4c29dcc249e53d058e36f37b041889f69262e9
DH common after  e8b1c25747245989df4b1afc59b4415ac8b975e9e660ef5c4d9e3fbbc3275ed3
```

## Validation

Focused no-network tests cover two concurrent NHL entries/exactly one simulated
request; durable-claim crashes before/after HTTP with no automatic retry; claim,
pre-request-status and publication failures; partial claims; phase separation;
explicit override; WARN-only exit; unchanged AUTO boundaries; concurrent DH writer
serialization and preservation on replacement failure. Existing NHL/DH regression
tests are also run. Fixtures mock publication and do not create a preseason canary.

Seven current Proppadia agents remain loaded: MLB BvP, daily refresh, DH capture,
DH grading, workload research; NHL morning and 900-second cross-market poll.
All retained plist hashes/schedules must match the pre-retirement inventory.
Remaining operational next step: separately authorized real preseason canary;
guard fixtures do not establish real-slate performance or promote any model.

Final local validation: 13 focused guard tests plus 14 existing NHL/DH tests,
27/27 PASS. Compilation PASS; `git diff --check` PASS. Both retired labels absent
from the loaded GUI domain and explicitly disabled; all eight other installed
Proppadia plist hashes unchanged. Simulated concurrency used two NHL entries and
exactly one fake request; crash-before/after cases made zero/one fake requests,
respectively, with zero additional requests on automatic retry. Live API calls and
credits used by implementation/validation: zero. Unrelated study-report changes
remain untouched and all repair changes remain uncommitted.
