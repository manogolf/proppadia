# configd / Time Machine / external-drive read-only diagnostic

Date: 2026-08-31 (America/Los_Angeles)

Host: Mac14,12, macOS 26.6.2 (25G83)
Scope: read-only diagnosis; no service, launchd, disk, mount, Time Machine, or source configuration was changed.

## Executive finding

The 2026-08-31 panic is conclusively a `configd` userspace-watchdog panic. Its preliminary stackshot names `IPConfigurationAgentQueue`, and the blocked queue's kernel dependency is the built-in wired-Ethernet poller `skywalk_netif_poller_en0`. Repeated router-ARP failures on `en0` and DHCP retries on `en8` preceded the hang. This is the strongest direct technical evidence.

There is also a serious, concurrent external-device abnormality: the directly attached RTL9210B USB storage enclosure emitted approximately 464 USB pipe-stall errors per minute throughout the incident window, failed SCSI power commands at 07:14:41 and 07:30:38, and a bounded `diskutil info` read later took about 175 seconds. An automatic Time Machine activity was dispatched at 06:58:57 and did not show the normal immediate activity-end transition before the 07:05:45 stackshot. No August 31 backup snapshot was completed.

The stackshot does **not** place `backupd`, `backupd-helper`, APFS, Disk Arbitration, or an external-drive process in configd's mutex/turnstile chain. Time Machine or the USB enclosure therefore cannot be declared causal from this evidence. The enclosure is nevertheless a meaningful isolation-test target because its error rate and timing are abnormal.

The older incident report and its preliminary stackshot are no longer present in the bounded system diagnostic-report locations. The two-incident configd/`IPConfigurationAgentQueue` signature cannot be independently confirmed from retained local evidence. The current event has that exact signature; the first remains unverified. If the earlier incident was the likely August 20 unexpected reboot that motivated the August 21 storage audit, it predates creation/configuration of this Time Machine volume and cannot have been caused by this destination.

## Panic and activity timeline

| Time (PT) | Evidence |
|---|---|
| 2026-08-21 21:45:02 | Birth time of the `Time Machine` APFS volume. This bounds destination creation; it is not by itself proof of the exact UI configuration instant. |
| 2026-08-22 02:17:44 | First retained external Time Machine backup snapshot. The destination was configured no later than this time. |
| 2026-08-30 08:57:44 | Latest completed external Time Machine snapshot. |
| 2026-08-30 20:31:18 | Current `/Library/Preferences/com.apple.TimeMachine.plist` birth/mtime. The plist was recreated or rewritten then; contents require Full Disk Access and were not read. |
| Before 06:45 on Aug 31 | `configd` was already logging repeated router ARP failures on `en0`; DHCP on `en8` repeatedly reached `no server`. RTL9210B USB pipe stalls also predated the Time Machine activity by at least 59 minutes in the inspected logs. |
| 06:46:50 | launchd spawned `backupd-helper` once because of an XPC event. No respawn loop was observed. |
| 06:58:57.370 | `com.apple.backupd-auto` XPC activity began. Earlier hourly activity at 05:58:54 reached state 5/end-running within milliseconds; this occurrence had no corresponding end-running record before the stackshot. |
| 07:05:45.3918 | Preliminary configd report: service check-in timed out, 60 seconds since last check-in; queue `IPConfigurationAgentQueue`, thread 2790898. Boot session `EF0D27D9-E861-4264-AD25-E474AED3F764`; uptime field 170000 seconds. |
| 07:05:45 stackshot | Thread 2790898 was `TH_WAIT, TH_UNINT`, blocked on a kernel mutex owned by kernel thread 3863, named `skywalk_netif_poller_en0`. `backupd-helper` and `backupd` were in ordinary interruptible waits and were not in this turnstile chain. |
| 07:14:41.895 | RTL9210B SCSI `START_STOP_UNIT [0]` power command failed. |
| 07:30:38.992 | RTL9210B SCSI `START_STOP_UNIT [1]` power command failed. |
| 07:30:50.22 | Kernel panic: no successful `configd` check-in for 180 seconds, one induced configd crash; other monitored services remained responsive. Same boot-session UUID as the preliminary report. Memory report showed 3 swapfiles and `OK` swap, not low-swap pressure. |
| 07:30:55–07:34:41 | Post-restart mount sequence: unencrypted sibling volumes mounted immediately; encrypted Time Machine volume initially could not unwrap metadata while locked, then mounted successfully at 07:34:41 after unlock. No APFS recovery object was needed. |

The 25-minute separation between the preliminary report and final panic means the preliminary stackshot was not simply the final 180-second countdown. The final panic records one induced configd crash and a later 180-second failure. Unified logs did not provide a clean successful-recovery marker between them.

## Current panic and stackshot comparison

- Preliminary report: `/Library/Logs/DiagnosticReports/configd-2026-08-31-070545.ips`
  - incident: `90686EB6-F8F9-4950-96A2-B6DCE70AE848`
  - boot session: `EF0D27D9-E861-4264-AD25-E474AED3F764`
  - termination: `monitoring timed out for service`
  - unresponsive queue: `IPConfigurationAgentQueue(tid:2790898)`
  - wait chain: configd thread 2790898 -> kernel mutex -> thread 3863 `skywalk_netif_poller_en0`
- Full panic: `/Library/Logs/DiagnosticReports/Retired/panic-full-2026-08-31-073050.0002.panic`
  - incident/boot session: `EF0D27D9-E861-4264-AD25-E474AED3F764`
  - panic: userspace watchdog, configd, 180 seconds without check-in
  - configd successful check-ins: 17,283 over 173,032 seconds
  - `logd`, WindowServer, and `opendirectoryd`: responsive at panic
  - watchdog backtrace only; no storage kext is named in the panic backtrace
- Older event:
  - no earlier `panic`, `configd*.ips`, or watchdog report was present under `/Library/Logs/DiagnosticReports`, `/private/var/db/PanicReporter`, or bounded `/var/db/diagnostics`, even with approved protected-directory read access;
  - unified-log searches could not recover a prior boot's stackshot signature;
  - therefore, “both panics name configd” and “both preliminary stackshots name `IPConfigurationAgentQueue`” are **insufficient evidence**, not confirmed facts.

## Time Machine configuration and state

- Destination: `Time Machine`, local, `/Volumes/Time Machine`
- Time Machine destination ID: `8B1C3872-2790-4DA2-B394-611B118F99B5`
- Volume UUID: `444637B5-61DE-4957-BED5-A39D88745636`
- APFS container: `disk8`; physical store: `disk7s2`
- Device/media: `TBU401E` in an RTL9210B enclosure; external fixed USB device
- Filesystem: case-sensitive APFS, Backup role
- Encryption: FileVault yes; currently unlocked
- Device capacity: 2,000,189,177,856 bytes
- Current container free: 1,613,241,200,640 bytes
- Time Machine quota: 1.4 TB; volume reports about 361,961,848,832 bytes used
- Mount options: `apfs, local, nodev, nosuid, journaled`; no custom fstab/automount entry found
- Automatic backups: enabled (`AutoBackup = 1`)
- Current status at audit: `Running = 0`
- Completed snapshots: nine daily snapshots, August 22 through August 30
- `tmutil latestbackup` and `tmutil listbackups`: not read because those operations require Full Disk Access; no Full Disk Access was requested
- Configuration date: destination volume created 2026-08-21 21:45:02; first completed snapshot 2026-08-22 02:17:44. Exact UI selection time is not stored in accessible metadata.
- Spotlight: `mdutil` reported the Spotlight server disabled; neither checked volume root has `.metadata_never_index`.
- Aliases/symlinks: `/Volumes/Time Machine` is a real mount directory, not a symlink. No drive-related alias, bind-like mount, custom automount, cron, login-item, or shell startup reference was found in the bounded configuration review.

### Backup state near the incident

Proven:

- an automatic-backup XPC activity was dispatched 6 minutes 48 seconds before the preliminary configd report;
- that activity did not show the normal end-running transition before the report;
- no August 31 completed snapshot exists;
- no explicit “backup completed,” thinning, verification, destination-loss, cancellation, or snapshot-completion milestone appeared in the focused pre-panic log search;
- `backupd-helper` was alive at the stackshot but not in configd's blocking chain.

Not proven:

- that file copying had begun;
- that Time Machine caused the configd mutex wait;
- that backupd was thinning or verifying;
- that the Time Machine volume itself disconnected before the panic.

## External-device health evidence

### Proven abnormal findings

- RTL9210B endpoint `0x81` repeatedly returned `0xe0005000 (pipe stalled)` with zero bytes transferred.
- Per-minute counts were approximately 464 throughout 06:45–07:05; 9,768 such records occurred across those 21 displayed minute buckets.
- The error stream existed before `com.apple.backupd-auto` was dispatched, so Time Machine did not initiate the underlying USB error condition.
- The enclosure failed SCSI power-state commands at 07:14:41 and 07:30:38.
- The same pipe-stall pattern resumed after restart.
- A later bounded `diskutil info disk8; diskutil info disk8s4` operation completed but took approximately 175 seconds, unusually long for metadata-only reads.

### Evidence not found

- no explicit unsafe-eject notification before panic;
- no explicit device-reset or disk-disappeared record in the focused pre-panic window;
- no APFS corruption or recovery requirement after restart;
- no exposed SMART health status (`SMART Status: Not Supported` over this interface).

The physical enclosure contains `ACASIS 1`, `Music`, and encrypted `Time Machine` APFS volumes. Therefore, a physical-drive isolation test affects all three, not just backup coverage.

## Launchd and local automation inventory

### Time Machine services

- `com.apple.backupd`: Apple sealed-system LaunchDaemon; currently running on demand, one run in the current boot, no prior exit.
- `com.apple.backupd-helper`: Apple sealed-system LaunchDaemon; spawned once at 06:46:50 in the incident boot and once in the current boot; no rapid respawn sequence.
- No Apple Time Machine job was unloaded, disabled, booted out, bootstrapped, or modified.

### Proppadia jobs

Nine `com.proppadia.*` user LaunchAgents were present. All accessible plists passed `plutil -lint`, use existing program/working-directory targets, have writable log-parent directories, mode 0644, and no quarantine flag. No duplicate labels were found. None reference `/Volumes/Time Machine`, `/Volumes/ACASIS 1`, `diskutil`, mount operations, or the manual odds-history offload utility.

Notable triggers:

- `com.proppadia.mlb.dh-forward-capture`: 600-second interval, RunAtLoad; exited at 06:55:43 before the stackshot.
- `com.proppadia.pregame-lineup-study.20260708` and `.20260709`: 300-second interval, RunAtLoad; both exited successfully at 07:03:53, about 112 seconds before the stackshot.
- scheduled MLB jobs: 03:30, 05:30/08:30/11:00/13:00/16:30, 08:15, 11:20/13:20, and weekly Wednesday 23:05.
- NHL morning orchestration: 07:30.

No Proppadia job was active in launchd immediately before the 07:05 report. The recurring jobs' current post-restart counters are normal for a fresh GUI bootstrap: interval jobs have successful exits; calendar jobs not yet due show zero runs. The weekly retrain job is present on disk but was not found in the current loaded GUI domain.

The two dated July pregame-lineup studies remain configured at five-minute intervals. They are unrelated to the external drive and had clean exits, but are operationally stale-looking and warrant separate owner review only if cleanup is later authorized.

### Other local jobs

- `/Library/LaunchDaemons`: Microsoft updater/licensing helpers and Zoom daemon; accessible plists valid. The Teams updater plist was permission-restricted and not validated without broader access.
- `/Library/LaunchAgents`: Microsoft OneDrive/update agents; accessible plists valid.
- user agents: Google/Microsoft updater agents and LG SwitchApp, plus Proppadia agents.
- no loaded `homebrew.mxcl.*` service or Homebrew plist was found. `brew services list` was not used as evidence because it attempted cache refresh under the restricted environment and failed; no cache cleanup or service change was made.
- `crontab -l` was denied by the privacy boundary; bounded launchd, shell-startup, login-item output, fstab, and automount searches found no external-drive automation.

### Repository design versus active configuration

`bin/mlb_odds_history_offload.sh` and `docs/Prod12 Automation Runbook.md` describe a manual/on-demand Odds API offload path under `/Volumes/ACASIS 1`. No launchd, cron, shell-login, or loaded-service reference invokes that tool. The storage-audit archive proposal similarly contains no active scheduler. No source, model, or operational job depends on the Time Machine mount.

## Causality assessment

### Proven relevant

1. Current panic and preliminary report both identify configd in the same boot session.
2. `IPConfigurationAgentQueue` was blocked uninterruptibly on the built-in `en0` Skywalk poller.
3. Wired-network router ARP failures and DHCP instability preceded the hang.
4. The external USB enclosure was simultaneously in a sustained, high-rate pipe-stall state.
5. Time Machine automatic activity began shortly before the stackshot and failed to reach its normal activity-end transition.

### Plausible association

- An unhealthy USB storage enclosure or its power/bridge behavior may have contributed to wider system stalls, and the automatic Time Machine activity could have exercised that device during the vulnerable period.
- The timing and device errors justify isolation, but there is no stackshot wait chain from configd to storage or backupd.

### Unrelated or weakened hypotheses

- Custom launchd/external-drive automation: no active automation was found.
- Proppadia scheduled work: no job was active immediately before the configd stackshot and none references the drive or network configuration.
- Low swap: the current panic recorded three swapfiles and OK swap status.
- Time Machine service respawn storm: only one helper spawn was seen; no loop.
- APFS corruption: none observed.

### Insufficient evidence

- Exact first-incident timestamp and stackshot signature.
- Whether both incidents share the configd/`IPConfigurationAgentQueue` chain.
- Whether the external Time Machine destination was involved in the first incident.
- Whether Time Machine file copying, thinning, or verification had started on August 31.

## Safe isolation recommendation

Temporarily operating with the entire TBU401E/RTL9210B device physically disconnected would be a meaningful A/B test because the enclosure is generating reproducible USB errors and SCSI power-command failures. It tests the physical device/bridge/cable/power path, not just Time Machine.

A reasonable observation window is **14 consecutive days**, chosen to exceed the roughly 10–11 day spacing between the likely August 20 storage-pressure incident and the August 31 panic. Because the first event timestamp is not independently verified, absence of recurrence would reduce suspicion but not prove causality; recurrence with the drive absent would strongly weaken the drive hypothesis.

Tradeoffs:

- no new backups to this external Time Machine destination during the test;
- local APFS snapshots may continue but are not equivalent protection against internal-drive failure;
- `ACASIS 1` and `Music` on the same physical device would also be unavailable;
- before any human-performed disconnection, complete and verify a current backup if the device remains responsive, then eject safely or shut down. Those actions are recommendations only and were not performed here.

No Time Machine setting, Apple service, drive connection, or launchd definition was changed during this diagnostic.

## Commands and access

Representative read-only commands used:

- `diskutil list`, `diskutil info disk8`, `diskutil info disk8s4`, `diskutil apfs listsnapshots disk8s4`, `df`, `mount`, `stat`, `mdutil -s`
- `tmutil destinationinfo`, `tmutil status`, `tmutil latestbackup`, `tmutil listbackups`
- focused `/usr/bin/log show` predicates for configd, IPConfiguration, watchdogd, Time Machine, backupd/helper, launchd, Disk Arbitration, APFS, USB, RTL9210B, mds/mdworker, and local labels
- `launchctl print`, `plutil -lint`, bounded plist parsing, target/path/permission checks, and configuration-reference searches
- bounded diagnostic-report discovery and JSON stackshot parsing
- `ifconfig`, `ioreg`, `last reboot`, and Git read-only status commands

Approved protected read access was used for `diskutil`, focused unified logs, Time Machine metadata, and bounded diagnostic-report discovery. Full Disk Access was not requested; consequently `tmutil latestbackup`, `tmutil listbackups`, and direct Time Machine preference-plist parsing were unavailable. No secrets or environment-variable values were printed.

## Repository integrity

Initial Git state was clean on `main` at `8aa24583d6606da3155f935d3c71afc2e88e0a09`. This Markdown report is the only intended repository write from the diagnostic.
