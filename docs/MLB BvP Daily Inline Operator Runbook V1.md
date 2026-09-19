# MLB BvP Daily Inline Operator Runbook V1

Contract: `MLB_BVP_DAILY_INLINE_ACQUISITION_V1`; effective September 19, 2026 PT.
Do not rerun acquisition merely to validate deployment. No model or betting
authorization follows from acquisition certification.

## Next natural 05:30 process validation

1. Verify retained power history shows the unchanged 05:27 wake/stir. Do not alter
   sleep/display/network behavior or infer causality from the schedule alone.
2. Inspect `artifacts/ops/mlb_refresh_daily.out.log` / `.err.log`: actual daily
   START/run tag, roster retries/success, one inline BvP phase, final DONE/exit.
3. Inspect `artifacts/ops/bvp_inline_v1/runs/2026-09-19/`: per-run result and private
   acquisition log. Request starts, bounded wake retries, actual acquisition
   timestamps and duration must be retained. Never expose raw credentials/URLs.
4. Read the acquisition ledger **read-only**. Expect one PRIMARY attempt, one
   result, one success receipt. Verify SHA, date/run linkage, games, prepared =
   written = verified rows and source/request-journal certification.
5. Inspect receipt-linked canonical identity journal: official game IDs, no off-date
   admissions, explicit unresolved-starter skips and successful empty responses.
6. Confirm BvP-dependent readers require receipt row keys/hash. Independent
   Moneyline/Totals/markets continue. With no qualified model, downstream model
   application/impact remain governed skips, not acquisition failures.
7. Inspect Ops Brief and daily index: inline acquisition status is current-date
   evidence, with no stale historical DNS explanation.
8. At natural 08:30, expect SUCCESS_ALREADY_EXISTS and no new acquisition. If
   primary failed, expect exactly one later RECOVERY with actual timestamp.
   Subsequent windows cannot make a third automatic attempt.
9. Confirm canonical `mlb-pipeline`, `mlb-daily-refresh`, `mlb-bvp-prewarm` locks
   release and no stale processes remain. Do not remove locks without diagnosis.

This is prospective process validation, not historical reconstruction. Installation
initializes empty claims schema only. `reconcile_mlb_bvp_inline_installation validate`
is specifically an installation-zero-acquisition check; do not use its zero-row
assertion after the first natural run. Thereafter inspect date receipts and normal
lane validators without modifying ledger evidence.

## Failure response

Read current ledger/logs first. No receipt means no certified same-date BvP input.
Leave independent lanes running. Wait for the single later natural recovery when
eligible; do not restart failed same-window acquisition. A killed wrapper may
leave canonical directory locks governed by existing stale-owner policy, requiring
operator diagnosis. No repair is authorized automatically by this runbook.

For an explicitly authorized manual recovery only, use an unambiguous once-only
authorization ID and the same canonical state:

```sh
/Users/jerrystrain/Projects/proppadia/bin/mlb_bvp_inline_manual_recovery.sh YYYY-MM-DD AUTHORIZATION_ID
```

It acquires the shared pipeline lock, then the BvP-specific hook lock; the hook
never reacquires the shared lock. It preserves actual timestamps and refuses
previously certified dates or repeated authorization IDs.

## Exact rollback evidence

Package: `artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_inline_consolidation_v1`.
`reconciliation_manifest.json` records absolute installed paths, full pre/post
SHA-256, owner/group, permissions, unchanged plist topology and repeating power.
Non-executable `.txt` files contain complete original bytes:

- Daily: `daily_wrapper.prechange.rollback-source.txt`, SHA
  `96b371f3b8d52cd73c9950b031829e7e7c3d49edf8d90857a81e9afab261038b`.
- Prewarm: `bvp_prewarm.prechange.rollback-source.txt`, SHA
  `23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb`.
- Plist: `bvp_prewarm.prechange.rollback-plist.txt`, exact 03:30 source.
- Active Hits manifest: `hits_governance_manifest.prechange.rollback-source.txt`.

Rollback is a separately approved operational change, not a command to run now.
Order is important because reverting wrappers without reverting future-date
admission guards would leave the old prewarm unable to certify new inputs:

1. Preserve these rollback files and manifest outside the repository in a private
   `mktemp -d` directory **before reverting the consolidation commit**. Retain all
   local acquisition state, request journals, logs and PostgreSQL rows unchanged.
2. Confirm both relevant agents/processes inactive and all three governed lock
   directories absent, well away from natural dispatch. Obtain separate rollback
   authority, then pause the daily label too; both labels must stay disabled while
   installed and tracked code are temporarily between contracts:

   ```sh
   launchctl disable gui/501/com.proppadia.mlb.refresh.daily
   launchctl bootout gui/501/com.proppadia.mlb.refresh.daily
   ```

3. While both labels are disabled, atomically restore the exact
   original daily/prewarm source bytes at their recorded installed paths. Restore
   recorded modes/uid/gid, run `zsh -n`, and verify full original SHA-256 values.
   Original dedicated and daily plists are unchanged; verify hashes rather than
   replacing unrelated plist content. If replacement is needed, use exact escrowed
   original bytes only.
   One exact same-filesystem procedure (run only under the separate rollback
   authorization) is:

   ```sh
   daily_tmp="$(mktemp /Users/jerrystrain/bin/.proppadia_mlb_refresh_daily.rollback.XXXXXX)"
   prewarm_tmp="$(mktemp /Users/jerrystrain/bin/.proppadia_mlb_bvp_prewarm.rollback.XXXXXX)"
   cp /Users/jerrystrain/Projects/proppadia/artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_inline_consolidation_v1/daily_wrapper.prechange.rollback-source.txt "$daily_tmp"
   cp /Users/jerrystrain/Projects/proppadia/artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_inline_consolidation_v1/bvp_prewarm.prechange.rollback-source.txt "$prewarm_tmp"
   chmod 0755 "$daily_tmp" "$prewarm_tmp"
   test "$(shasum -a 256 "$daily_tmp" | awk '{print $1}')" = 96b371f3b8d52cd73c9950b031829e7e7c3d49edf8d90857a81e9afab261038b
   test "$(shasum -a 256 "$prewarm_tmp" | awk '{print $1}')" = 23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb
   zsh -n "$daily_tmp"
   zsh -n "$prewarm_tmp"
   mv -f "$daily_tmp" /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
   mv -f "$prewarm_tmp" /Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh
   ```

4. `git revert CONSOLIDATION_COMMIT` (replace with the exact local commit hash in
   the implementation handoff) restores tracked admission guards, new hook,
   Ops Brief logic and active Hits-manifest binding as one coherent code change.
   Preserve unrelated worktree changes; do not reset or clean the repository.
5. Only once wrapper/code/manifest coherence is verified, restore original launchd
   topology using `launchctl enable gui/501/com.proppadia.mlb.bvp.prewarm.daily`
   then `launchctl bootstrap gui/501 /Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist`.
   These commands require rollback authorization. Do not kickstart acquisition.
   Re-enable/bootstrap the unchanged daily plist as well, then verify it has the
   original five local calendar windows. These are rollback actions, never validation
   commands for the current implementation.

6. Verify restored 03:30 label loaded/enabled, daily five windows unchanged,
   full old wrapper hashes and Hits validator passing. The 05:27 repeating wake
   and other power settings remain unchanged throughout.

Historical reconciliation and audit snapshots are preserved, not rewritten to
pretend they bind current installed files. This package supersedes old installed
state assertions; the active Hits integrity manifest is updated explicitly.
