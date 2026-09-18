# configd / Time Machine / external-drive read-only diagnostic

Date: 2026-08-31 (America/Los_Angeles)

Host: Mac14,12, macOS 26.6.2 (25G83)
Scope: read-only diagnosis; no service, launchd, disk, mount, Time Machine, or source configuration was changed.

> **Superseding topology correction (connected-test addendum, 2026-08-31):** The
> high-rate USB pipe-stall and SCSI power-command records described below identify
> USB address 8, `RTL9210B-CG@01120000`. Live I/O-registry tracing proves that the
> ACASIS TBU401E containing `ACASIS 1`, `Music`, and `Time Machine` is a different
> device at USB address 9, `ACASIS USB Drive@01130000`, mapped to physical `disk7`.
> Address 8 is a separate Realtek bridge with no mounted media and zero block I/O.
> The prior attribution of address-8 errors to the TBU401E is therefore withdrawn.
> See **Connected reliability characterization addendum** for the measured result.

## Executive finding

The 2026-08-31 panic is conclusively a `configd` userspace-watchdog panic. Its preliminary stackshot names `IPConfigurationAgentQueue`, and the blocked queue's kernel dependency is the built-in wired-Ethernet poller `skywalk_netif_poller_en0`. Repeated router-ARP failures on `en0` and DHCP retries on `en8` preceded the hang. This is the strongest direct technical evidence.

There is also a serious, concurrent external-device abnormality: a separate RTL9210B-CG bridge at USB address 8 emitted approximately 464 USB pipe-stall errors per minute throughout the incident window and failed SCSI power commands at 07:14:41 and 07:30:38. The TBU401E is USB address 9 and did not reproduce those errors in the connected characterization below. A bounded `diskutil info` read later took about 175 seconds, but that timing alone does not identify which device caused the delay. An automatic Time Machine activity was dispatched at 06:58:57 and did not show the normal immediate activity-end transition before the 07:05:45 stackshot. No August 31 backup snapshot was completed.

The stackshot does **not** place `backupd`, `backupd-helper`, APFS, Disk Arbitration, or an external-drive process in configd's mutex/turnstile chain. Time Machine or the TBU401E therefore cannot be declared causal from this evidence. The address-8 bridge is a separate diagnostic target; its errors must not be attributed to the TBU401E.

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

- The separate USB-address-8 RTL9210B-CG endpoint `0x81` repeatedly returned `0xe0005000 (pipe stalled)` with zero bytes transferred.
- Per-minute counts were approximately 464 throughout 06:45–07:05; 9,768 such records occurred across those 21 displayed minute buckets.
- The error stream existed before `com.apple.backupd-auto` was dispatched, so Time Machine did not initiate the underlying USB error condition.
- The address-8 device failed SCSI power-state commands at 07:14:41 and 07:30:38.
- The same pipe-stall pattern resumed after restart.
- A later bounded `diskutil info disk8; diskutil info disk8s4` operation completed but took approximately 175 seconds, unusually long for metadata-only reads.

### Evidence not found

- no explicit unsafe-eject notification before panic;
- no explicit device-reset or disk-disappeared record in the focused pre-panic window;
- no APFS corruption or recovery requirement after restart;
- no exposed SMART health status (`SMART Status: Not Supported` over this interface).

The address-9 TBU401E contains `ACASIS 1`, `Music`, and encrypted `Time Machine` APFS volumes. The address-8 error source has no mounted media. A TBU401E isolation test would therefore affect all three volumes but would not isolate the device that produced the logged stalls.

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
4. A separate USB-address-8 RTL9210B-CG bridge was simultaneously in a sustained, high-rate pipe-stall state.
5. Time Machine automatic activity began shortly before the stackshot and failed to reach its normal activity-end transition.

### Plausible association

- The address-8 USB bridge abnormality may have contributed to wider system stalls, but the automatic Time Machine activity used the different address-9 TBU401E.
- The timing justifies characterizing the address-8 device separately, but there is no stackshot wait chain from configd to either storage device or backupd.

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

The prior recommendation to disconnect the TBU401E as the source of the logged stalls is withdrawn. The connected topology proves that the reproducible USB errors and historical SCSI power-command failures belong to the separate address-8 RTL9210B-CG bridge. Any future physical isolation should first identify that device by serial `012345681550` and change only that one variable; no disconnection was performed here.

A reasonable observation window is **14 consecutive days**, chosen to exceed the roughly 10–11 day spacing between the likely August 20 storage-pressure incident and the August 31 panic. Because the first event timestamp is not independently verified, absence of recurrence would reduce suspicion but not prove causality; recurrence with the drive absent would strongly weaken the drive hypothesis.

Tradeoffs:

- isolating only address 8 should not interrupt the address-9 Time Machine destination, but its physical identity must be confirmed before touching cables;
- disconnecting the TBU401E would remove `ACASIS 1`, `Music`, and Time Machine backup coverage without isolating the observed error source;
- cable/port tests should vary only one physical factor at a time and repeat the same bounded observation;
- those actions are recommendations only and were not performed here.

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

## Connected reliability characterization addendum

Date/time: 2026-08-31 08:42–09:30 PT

Authority remained diagnostic-only. No disk, APFS, encryption, snapshot, Time
Machine, cable, port, or source-control configuration was changed. No synthetic
file or write-mode media test was used. The addendum was left uncommitted for
owner review as requested.

### Corrected physical topology

| Layer | Resolved identity |
|---|---|
| ACASIS enclosure USB node | `ACASIS USB Drive@01130000`; USB address 9; serial `012345678931`; Realtek vendor/product `0x0bda:0x9210` |
| USB path | `AppleUSB20HubPort@01130000`; negotiated 480,000,000 bit/s (USB 2.0); reported sink allocation 500 mA |
| SCSI identity | vendor `ACASIS`; product `TBU401E`; product revision `1.00` |
| Physical disk | `disk7`, GUID, 2,000,398,934,016 bytes; `disk7s1` EFI and `disk7s2` APFS physical store |
| APFS container | synthesized `disk8`, UUID `9DE34D98-850C-4BA6-8D71-DD62B871FC60`, 2,000,189,177,856 bytes |
| APFS volumes | `disk8s1` `ACASIS 1`; `disk8s2` `Music`; `disk8s4` encrypted/unlocked `Time Machine` with 1.4 TB quota |
| Separate noisy device | `RTL9210B-CG@01120000`; USB address 8; serial `012345681550`; adjacent hub port; 480,000,000 bit/s; no `IOMedia` child and zero block operations/bytes |

Both USB devices use the same Realtek vendor/product identifiers, which made a
product-name-only log search ambiguous. USB address, location path, SCSI identity,
and `IOMedia` ancestry provide the deterministic mapping.

The live bridge path exposes neither the installed drive model nor rotational/
solid-state status (`Solid State: Info not available`). ACASIS specifies the
[TBU401 family as an M.2 NVMe SSD enclosure](https://www.acasis.com/en-in/products/acasis-usb4-0-mobile-m-2-nvme-enclosure-40gbps-compatible-with-typec-thunderbolt-3-interface-solid-state-nvme-ssd-universal-tools?variant=43694516601061),
so the installed medium is consistent with NVMe SSD use, but macOS did not expose
the SSD identity needed to independently confirm its model. The SCSI product
revision is `1.00`; no unambiguous RTL9210 firmware revision was exposed.
`smartctl` is not installed, `diskutil` reports SMART unsupported through this
bridge, and no temperature sensor is exposed. No utility was installed.

### Idle observation

An automatic Time Machine attempt entered `PreparingSourceVolumes` at 08:49:04
while the initial baseline was being established and then returned to
`Running = 0`. It was not manually started or stopped. Disk7 continued background
I/O briefly, then reached a full zero-I/O interval. The formal idle clock was
therefore reset and ran from 08:56:03 through 09:27:16 PT (31 minutes 13 seconds).
All three volumes remained mounted and Time Machine remained stopped.

| Measure | TBU401E address 9 | Separate address 8 bridge |
|---|---:|---:|
| Focused USB/SCSI events | 0 | 14,536 |
| Pipe stalls | 0 | 14,536 |
| Pipe-stall rate | 0.0/min | 465.6/min |
| USB resets | 0 | 0 |
| SCSI/power failures | 0 | 0 in this interval |
| Disconnects/device disappearance | 0 | 0 |

TBU401E driver-counter deltas during the formal observation were 413 read
operations / 13,717,504 bytes and 596 write operations / 9,936,896 bytes. These
were incidental macOS metadata accesses, not a synthetic test. Most one-minute
samples were 0 MB/s; brief samples peaked at 0.27 MB/s. Driver read errors,
write errors, and retries remained zero. Thus the historical “approximately 464
per minute” condition is still occurring now, but on address 8—not on the ACASIS
TBU401E at address 9.

The five minutes immediately preceding the formal window independently contained
2,328 address-8 events (465.6/min) and zero matching address-9 events, including
the short natural Time Machine preparation attempt. That attempt was not a
controlled workload phase and is insufficient for a Time Machine association
finding.

### Read-only verification and workload boundary

After the clean idle phase, `diskutil verifyDisk disk7` and one bounded
`diskutil verifyVolume '/Volumes/ACASIS 1'` attempt were rejected before starting:

```text
This operation is restricted by Sandbox; check your settings in
System Settings > Privacy & Security > Files and Folders (-69464)
```

No verification I/O occurred. After the identical second preflight denial, no
container/volume verification and no raw physical-device read were attempted.
Completing those phases requires the user to grant the running Codex host
**Removable Volumes** permission; Full Disk Access is not requested by this
addendum. Consequently, errors per unit of intentional data read, sustained
throughput, and workload-latency stability are not yet measured.

The manually initiated Time Machine phase was not started. Its prerequisite
read-only verification/workload phases are incomplete, and the task separately
requires explicit user confirmation immediately before starting a backup. Cable
and port comparisons also remain pending human action; neither variable was
changed.

### Current classification

Overall classification: **`INCONCLUSIVE`**.

The completed no-load subphase is **`CONNECTED_TEST_CLEAN`** for the TBU401E:
zero address-9 USB/SCSI faults, resets, retries, power failures, or disappearances
were observed. There is no current `MEDIA_OR_FILESYSTEM_FAILURE_EVIDENCE`,
`ENCLOSURE_CONTROLLER_OR_POWER_PATH_EVIDENCE`, `TIME_MACHINE_WORKLOAD_ASSOCIATION`,
or `IDLE_POWER_STATE_ASSOCIATION` for the TBU401E from this bounded test.

The separate address-8 RTL9210B-CG bridge does exhibit continuing controller/USB
path abnormality. That evidence must not be attributed to the address-9 TBU401E.
The earlier panic's direct `configd`/`IPConfigurationAgentQueue`/`en0` evidence is
unchanged, and no new storage-to-configd causal chain was found.

Pre-addendum Git state was clean on `main` at
`f423ecdfcbb24ed092beea6d3fa35615e51f8d7b`. This addendum is the only repository
write in the connected test and is intentionally not committed pending review.

### Removable-Volumes permission continuation

At 09:36:57 PT the user confirmed that Removable Volumes permission had been
granted to the running Codex host and authorized resumption of only the read-only
verification/physical-read phase. The topology was re-resolved before testing:
TBU401E remained USB address 9 / physical `disk7` / APFS `disk8`; the noisy
RTL9210B-CG remained the separate address-8 device with no mounted media. Time
Machine reported `Running = 0`, and `ACASIS 1`, `Music`, and `Time Machine` were
all mounted.

macOS still rejected the physical partition-map verification before it began:

```text
diskutil verifyDisk disk7
Error starting partition map verification for disk7: This operation is
restricted by Sandbox; check your settings in System Settings > Privacy &
Security > Files and Folders (-69464)
```

A single 4 KiB raw-read permission probe was then attempted against the correctly
resolved physical device, with output directed to `/dev/null`:

```text
dd if=/dev/rdisk7 of=/dev/null bs=4096 count=1
dd: /dev/rdisk7: Operation not permitted
```

The probe transferred zero bytes. Testing stopped at that boundary; no alternate
privilege path, `sudo`, direct filesystem utility, short read scan, extended read
scan, repair, or write was attempted.

The monitored interval ran from 09:36:57 through 09:38:48 PT (111 seconds):

| Measure | Result |
|---|---:|
| TBU401E/address-9 USB or SCSI fault events | 0 |
| TBU401E read errors / write errors | 0 / 0 |
| TBU401E retries | 0 |
| TBU401E resets | 0 |
| TBU401E SCSI/I/O/power failures | 0 |
| TBU401E disconnect/reconnect events | 0 |
| ACASIS mount-state changes | 0 |
| Separate address-8 pipe stalls | 872 (471.4/min) |
| Time Machine state transitions | 0; `Running = 0` at both boundaries |

Disk7 `iostat` samples were zero except for one incidental 1.27 MB/s metadata
interval. TBU401E driver counters increased by 50 read operations / 2,535,424
bytes and 289 write operations / 4,370,432 bytes during preflight and permission
checks; these were naturally occurring macOS metadata operations, not the blocked
raw-read probe. Error and retry counters remained zero. Intentional read
throughput and errors per unit of intentional data read remain unmeasured.

Read-phase classification: **`READ_TEST_INCONCLUSIVE`**. The observation contains
no TBU401E fault, but macOS denied the verification and physical-read operations
before data transfer, so it cannot support `READ_TEST_CLEAN`. No Time Machine
workload was initiated. A Time Machine workload test remains separately gated by
explicit human authorization and must not begin from this continuation.

### Full Disk Access verification/read continuation

This subsection supersedes the permission-blocked read classification immediately
above. At 09:53:43 PT the user temporarily granted Visual Studio Code Full Disk
Access and authorized only the previously defined read-only verification and
physical-read phase. Topology remained TBU401E at USB address 9 / physical
`disk7` / APFS `disk8`; the address-8 RTL9210B-CG remained a separate no-media
device. Time Machine reported `Running = 0` and all three ACASIS volumes were
mounted at preflight.

Read-only verification results:

- `diskutil verifyDisk disk7`: partition map appears OK; exit 0; 0.76 seconds.
- `diskutil verifyVolume disk8`: invoked `fsck_apfs -n -x /dev/disk7s2`.
- APFS container superblock, checkpoint, space manager/queues, object map, and
  encryption key structures: checked without error.
- `ACASIS 1` (`disk8s1`): appears OK.
- `Music` (`disk8s2`): appears OK.
- `Time Machine` (`disk8s4`): appears OK, including all ten retained snapshots
  from August 22 through the new natural August 31 08:53 snapshot.
- Allocated space and container `disk7s2`: appear OK.
- Storage-system check exit code: 0; duration 219.62 seconds.
- No repair action was invoked and no filesystem error was reported.

`diskutil verifyVolume` performed an offline read-only check, temporarily
unmounting `disk8s1`, `disk8s2`, and `disk8s4` at 09:54:30–09:54:31 and remounting
all three successfully at 09:58:09. All were mounted at the final boundary. These
three unmount/remount cycles are verification-induced mount-state changes, not
device disconnects.

The TBU driver recorded 2,435,041,280 bytes read during the APFS verification,
averaging 11.09 MB/s (10.57 MiB/s) across its metadata-heavy 219.62-second run.
Observed five-second `iostat` samples ranged from idle to 24.72 MB/s. Across the
full 09:53:43–10:00:27 phase, the driver recorded 3,343,099,904 bytes read. Read
errors, write errors, and retries remained zero.

No synthetic or user-directed write was issued. The device driver nevertheless
recorded 377,925,632 bytes written by macOS during the full interval, including
filesystem unmount/remount and post-verification system activity. Time Machine
remained `Running = 0` at both boundaries and no Time Machine state transition was
found. The observed system writes are therefore disclosed rather than attributed
to a synthetic test, repair, or Time Machine backup.

The planned 256 MiB sequential raw read was attempted only as:

```text
dd if=/dev/rdisk7 of=/dev/null bs=4m count=64
```

It was rejected immediately with `Permission denied` and transferred zero bytes.
Full Disk Access permits the privileged `diskutil` verification helper but does
not override `/dev/rdisk7` ownership (`root:operator`, mode `0640`). No `sudo`,
alternate privilege path, second raw attempt, or extended scan was used.

Phase-level event results:

| Measure | Result |
|---|---:|
| TBU401E/address-9 USB/SCSI events | 0 |
| TBU401E read/write errors | 0 / 0 |
| TBU401E retries | 0 |
| TBU401E resets | 0 |
| TBU401E SCSI/I/O/power failures | 0 |
| TBU401E disconnect/reconnect events | 0 |
| Verification-induced volume unmount/remount cycles | 3 / 3 successful |
| Separate address-8 pipe stalls | 3,144 (466.9/min) |
| Time Machine state transitions | 0 |

Final read-phase classification: **`READ_TEST_CLEAN`** for the completed bounded
workload. The partition map and full APFS storage system passed while the
TBU401E successfully serviced more than 2.4 GB of verified reads without an
address-9 fault, driver error, retry, reset, or disconnect. This classification
does not claim a full-device surface scan or raw sequential benchmark; that
specific `dd` path remained unavailable without root device access.

Testing stopped after this phase. No Time Machine workload, cable/port/hub change,
repair, erase, reformat, device alteration, commit, or push was performed.

## September 18, 2026 addendum — BvP prewarm delayed dispatch and wake-time DNS failure

Investigation date: 2026-09-18. Evidence cutoff: 09:39 PT (16:39 UTC); retained event window queried: 03:15–05:40 PT (10:15–12:40 UTC). PT is America/Los_Angeles, UTC−07:00. This addendum is the sole diagnostic write authorized by the investigation's deliverable section. No network request, database query, acquisition, recovery, service signal, configuration change, commit or push was performed. Private network addresses, hardware addresses, resolver addresses and DHCP identifiers are omitted. Existing unrelated worktree changes were preserved.

### Decisions and direct answers

- Network: `SLEEP_WAKE_NETWORK_READINESS_RACE`.
- Recovery: `BVP_RECOVERY_CONTRACT_UNCLEAR` for evidence-grade admission of a late capture. Operational collection may still be useful, but is not an original-03:30 replay and is not authorized by this investigation.
- Dispatch delay: the Mac was in actual system sleep at 03:30. The missed calendar event dispatched in a subsequent Ethernet/network-triggered dark wake, before en0/DHCP/DNS initialization completed. This is not explained by waiting for either application lock.
- Acquisition failed genuinely, rather than reaching a governed no-qualified-model skip. Three fast DNS failures exhausted the existing 1.5-second and 3-second waits. en0/DNS returned approximately two seconds after wrapper termination.
- Smallest prospective correction: narrowly extend and instrument the existing initial-schedule DNS/transport retry budget, retaining bounded failure, locks and acquisition-before-write ordering. Do not introduce whole-wrapper launchd retries or change wake times on this evidence alone. No correction was implemented.

### Launch, power and network timeline

Times below are on September 18; UTC column is the corresponding date and time. Application log timestamps have whole-second precision; unified-log timestamps retain microseconds. A wake-summary timestamp is not the beginning of the wake transition.

| PT | UTC | Retained event and interpretation |
|---|---|---|
| 03:23:09 | 10:23:09Z | Maintenance dark wake from Deep Idle, rtc/Maintenance. |
| 03:23:54 | 10:23:54Z | Actual system sleep, reason `Maintenance Sleep`, TCP keepalive active. This sleep spans 03:30. |
| 03:30:00 expected | 10:30:00Z | BvP StartCalendarInterval due; no application dispatch at this time. Calendar triggers do not wake the Mac. |
| 03:39:34.282673 | 10:39:34.282673Z | launchd: `xpcproxy spawned with pid 20135`. Earliest retained dispatch/process timestamp. |
| 03:39:34.286380 | 10:39:34.286380Z | Kernel Ethernet route resolution on en0 fails with err 50; private destination redacted. |
| 03:39:34.304386 | 10:39:34.304386Z | powerd updates wake-start timestamp. Its logging order does not imply the process ran before the underlying wake began. |
| 03:39:34.463295 | 10:39:34.463295Z | launchd confirms installed BvP wrapper spawned because an XPC event. |
| 03:39:34 | 10:39:34Z | Wrapper acquires both `mlb-bvp-prewarm` and `mlb-pipeline`, records START and `local_prewarm_20260918T103934Z`; PID 20135, PPID 1. |
| 03:39:34.515330 | 10:39:34.515330Z | Aquantia Ethernet en0: wake reason 15, `TCP Keep Alive Timeout`. |
| 03:39:34.634790–.635150 | 10:39:34.634790–.635150Z | configd: en0 link INACTIVE, link changed at wake, DHCP removes address and records network changed. |
| 03:39:34.651590 | 10:39:34.651590Z | configd removes IPv4 en0/DNS/proxy configuration. |
| 03:39:34.652893–.672260 | 10:39:34.652893–.672260Z | mDNSResponder/Network.framework: `[65: No route to host]`, failed connection and cancellation. |
| 03:39:34.670402 | 10:39:34.670402Z | Resolver has zero nameservers; subsequently no DNS service is available for multiple questions. |
| 03:39:34.836335–.837426 | 10:39:34.836335–.837426Z | Python PID 20168, first StatsAPI A/AAAA lookup: null DNS service and no answer; 1.091 ms from A start to last stop. |
| 03:39:36.414238–.414825 | 10:39:36.414238–.414825Z | Second same-name A/AAAA lookup, same failure; 0.587 ms. |
| 03:39:36.983608 | 10:39:36.983608Z | powerd dark-wake summary: Deep Idle, enet/SMC/lan-10gb. pmset rounds this summary to 03:39:36, after dispatch began. |
| 03:39:39.566008–.567097 | 10:39:39.566008–.567097Z | Third same-name A/AAAA lookup, same failure; 1.089 ms. |
| 03:39:39 | 10:39:39Z | Acquisition fails; wrapper end records rc 2, acquisition FAILED, downstream/impact NOT_STARTED. Both locks released. No successful DONE marker. |
| 03:39:39.654330 | 10:39:39.654330Z | launchd service becomes inactive; child removal logged at .654399. |
| 03:39:41.629550–.632783 | 10:39:41.629550–.632783Z | configd link-active transition and en0 ACTIVE. |
| 03:39:41.633241–.645271 | 10:39:41.633241–.645271Z | DHCP INIT/REBOOT, router ARP detection, response, lease identified. Positive local reachability/lease-validation evidence, not proof of a new DHCP lease. |
| 03:39:41.658897–.686748 | 10:39:41.658897–.686748Z | IPv4/DNS/DHCP published successfully; en0 route-bearing configuration returns, followed by an en0-scoped Do53 service with two nameservers. |
| 03:39:41.710380 | 10:39:41.710380Z | Unrelated outbound connection's path becomes Satisfied on en0, IPv4/DNS. First retained usable-route evidence; no literal historical default-route table snapshot exists. |
| 03:39:41.751821 | 10:39:41.751821Z | Positive DNS A answer. Resolver usability restored; a cached answer cannot alone prove a fresh upstream DNS exchange. |
| 03:39:41.778315–.810909 | 10:39:41.778315–.810909Z | Unrelated internet connection receives SYN/ACK, connects TCP, and reports ready after TLS. Positive external connectivity, without disclosing destination identifiers. |
| 03:39:42.667772; 03:39:43.317882 | 10:39:42.667772Z; 10:39:43.317882Z | DHCP BOUND and subsequent bound processing/republishing. |
| 04:43:58; 04:59:13; 05:00:19 | 11:43:58Z; 11:59:13Z; 12:00:19Z | Maintenance sleep, maintenance dark wake, maintenance sleep. Recovery at 03:39 does not establish uninterrupted later health. |
| 05:15:47; 05:16:47 | 12:15:47Z; 12:16:47Z | SleepService dark wake followed by Back-to-Sleep. |
| 05:25:32; 05:26:17 | 12:25:32Z; 12:26:17Z | Maintenance dark wake followed by maintenance sleep. |
| 05:26:19 | 12:26:19Z | Retained powerd wake-request record identifies UserWake scheduled for 05:27. This is historical evidence, not projection from today's settings. |
| 05:27:00; 05:27:03; 05:27:33 | 12:27:00Z; 12:27:03Z; 12:27:33Z | Display ON, full Wake with rtc/HIDActivity, display OFF. Scheduled 05:27 wake participated. HID wording alone does not establish human action. |
| 05:30:03 | 12:30:03Z | Natural daily job acquires daily/pipeline locks and starts Moneyline lifecycle; run `local_daily_20260918T123003Z`. The main refresh START banner occurs later, after this prerequisite. |
| 05:30:03.413569–.414482 | 12:30:03.413569–.414482Z | Daily Python PID 20871 resolves the same hostname hash that failed during BvP; positive A and AAAA results. |
| 05:34:00.664362 | 12:34:00.664362Z | Retained original daily immutable prediction timestamp. |
| 05:34:02 | 12:34:02Z | Successful Moneyline lifecycle, with 15 games discovered/predictions written in original run; proves StatsAPI access by this point. Exact first HTTP response-completion timestamp is not retained. |
| 05:34:02–05:34:19 | 12:34:02–12:34:19Z | Roster refresh succeeds on attempt 1/4, all 30 teams, 840 roster rows upserted. |
| 05:34:19 | 12:34:19Z | Daily explicitly skips BvP fallback. No further BvP attempt in the inspected 03:15–05:40 window. |

Timing separation: expected calendar to first process spawn **574.282673 seconds**; first spawn to first Python DNS request **0.553662 seconds**; first-to-third DNS start **4.729673 seconds**; first spawn to service inactive **5.371657 seconds**. These are distinct from the individual ~1 ms resolution failures and from the ~7.4-second dispatch-to-network-readiness interval. Exact initial hardware wake onset is not independently timestamped before launchd; the driver's reason and powerd records establish the same wake episode. Both locks were obtained in the START second, without a retained lock wait.

Current state inspected read-only: agent `com.proppadia.mlb.bvp.prewarm.daily` is loaded, not running, runs=1, last exit=2; installed plist is `/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist`, calendar Hour=3/Minute=30, RunAtLoad=false, no KeepAlive or StartInterval retry. Program is `/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh`; working directory is the repository; stdout/stderr are `artifacts/ops/mlb_bvp_prewarm_daily.out.log` and `.err.log`. Local launchd.plist documentation describes missed calendar events coalescing on wake, not waking the system themselves.

Current pmset reports AC `sleep=0`, `displaysleep=0`, `powernap=1`, `tcpkeepalive=1`, `womp=1`, `standby=0`, `disksleep=0`, and daily wakepoweron 05:27. Current sleep=0 does not prevent explicit/manual sleep and is not historical proof the machine remained awake. One-time events shown by the current schedule are later than this failure, not causes of it. Retained logs show an mDNS maintenance-wake assertion near 03:39:34 and powerd AC-wake linger. No retained human-activity/keypress evidence near 03:39 was found in the focused power records; the affirmative hardware wake reason is network-related. Small clock offsets near wake cannot explain nine minutes; later timed records include multi-second corrections, so microsecond precision denotes recorded event times, not a claim of absolute clock accuracy. Why the Mac initially entered system sleep before this window is not established by the maintenance-sleep records.

### Exact application error and retry/storage semantics

The terminal exception chain, scoped to today's structured prewarm invocation, contains:

```text
socket.gaierror: [Errno 8] nodename nor servname provided, or not known
urllib3.exceptions.NameResolutionError: <urllib3.connection.HTTPSConnection object at 0x109dddb10>: Failed to resolve 'statsapi.mlb.com' ([Errno 8] nodename nor servname provided, or not known)
requests.exceptions.ConnectionError: HTTPSConnectionPool(host='statsapi.mlb.com', port=443): Max retries exceeded with url: /api/v1/schedule?sportId=1&date=2026-09-18&hydrate=probablePitcher (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x109dddb10>: Failed to resolve 'statsapi.mlb.com' ([Errno 8] nodename nor servname provided, or not known)"))
make: *** [mlb-bvp-pvb-refresh] Error 1
[2026-09-18T10:39:39Z] ERROR BVP_ACQUISITION_STATUS=FAILED rc=2
[2026-09-18T10:39:39Z] BVP_PREWARM_RUN_END run_tag=local_prewarm_20260918T103934Z MLB_DATE_ET=2026-09-18 wrapper_rc=2 acquisition_status=FAILED downstream_status=NOT_STARTED impact_status=NOT_STARTED
```

The first two retry logs have the same gaierror text and URL, with connection objects `0x109dcde90` and `0x109dcfe90` respectively; next-attempt labels 2/3 and 3/3, waits 1.5 and 3.0 seconds. The OS resolver evidence supplies the exact three lookup times above. This is not evidence of authoritative upstream NXDOMAIN, a 20-second resolver timeout, or a browser-only failure: DNS services were absent while the interface was inactive. Multiple mDNS questions, unrelated network activity and kernel route failures were affected; both A and AAAA requests failed. Full historical IPv4/IPv6 route tables and an ARP/DHCP packet capture were not available.

Acquisition path: installed wrapper → `make mlb-bvp-pvb-refresh` → `backend/mlb/scripts/refresh_mlb_bvp_pvb.py`. `_fetch_json` already implements **three total application attempts**, 20-second requests timeout, and 1.5/3-second waits; it catches `requests.RequestException` (including ConnectionError, Timeout and HTTPError). The phrase “Max retries exceeded” in urllib3 is not four wrapper attempts. DNS errors are retried, but each failed immediately because no resolver was available. HTTP/schema failures should not be normalized into harmless readiness skips.

The schedule fetch is the first operation in `_build_rows_for_date`, before local-game mapping, starter fallback, roster/BvP acquisition or `_upsert_rows`. Therefore this failed invocation produced no BvP data or database writes; no durable acquisition/paid-attempt claim is used by this path. Data writes normally occur after the date's rows are assembled. Acquisition exceptions can be distinguished from post-acquisition failures here because the wrapper records stage status. Existing nonzero failure was correct; the no-qualified-model exit-0 correction did not apply because downstream stages were never reached.

Later daily runs **can** invoke the same target through `MLB_DAILY_BVP_FALLBACK_ENABLED=1`, but the installed default is 0 and today's 05:34:19 and 08:33:52 logs explicitly skipped it. There is no automatic launchd retry. This explains the missing date without claiming later MLB collection failed. The prior daily review reported zero September 18 BvP rows; no new database query was made in this investigation. Subsequent retained daily DONE/lock releases were 06:06:11 PT and 09:04:39 PT.

### Comparison with earlier en0 incidents

| Earlier signature | September 18 evidence |
|---|---|
| Same en0 Ethernet path | Present: Aquantia en0 driver wake/link messages. |
| System-level No route to host | Present in mDNSResponder/Network.framework during link/address withdrawal. |
| Multiple DNS questions and A/AAAA failures | Present; absent DNS service affects more than the BvP client. |
| Repeated router ARP failure despite continuous awake operation | Not found in the focused retained network window; affirmative router-detection response on reactivation. September 18 actually slept, unlike the established earlier awake/display-off failures. |
| configd missed check-ins/stall/watchdog panic | Not found in the focused window; configd promptly removes and republishes network state. August 31's watchdog panic is not reproduced. |
| DHCP/ARP involvement | Present, but observed as wake-time INIT/REBOOT, lease/router validation and successful BOUND, not proven renewal/expiration failure. |
| Persistent failure requiring a later stir | Not reproduced: recovery occurred during the 03:39 dark wake, long before 05:27. |
| RTL9210B/Time Machine causal association | Unavailable in this focused diagnosis; no attribution made. Earlier concurrent storage errors were not proven causal. |

The established September 1–2 incidents recovered after en0/router failures; September 3's clean active-monitor observation had traffic/promiscuity/restart confounders and was not a natural-idle control. Shared interface and No-route wording do not establish a shared root cause. This addendum does not replace the earlier incident classifications.

### Recovery validity and lineage

Existing BvP feature-lineage authority consulted: `artifacts/analysis/mlb/feature_lineage/bvp_data_production_alignment_audit.md` and `bvp_lineage_recovery_window_dry_run_summary.md`. The acquisition reads mutable probable starters, active rosters and career vsPlayer statistics, including optional current database starter references. The endpoint has no implemented 03:30 as-of cutoff; fields named `*_prior` are not proof of timestamp-frozen replayability. No complete immutable 03:30 BvP input set was retained by the failed schedule request.

At this morning's cutoff the retained daily evidence discovers 15 games; the retained early Pinnacle raw response places its earliest event at 15:41 PT (22:41 UTC). No retained evidence indicates a started September 18 game by the cutoff. Market commence times are not substituted for official state, and no live official-state check was performed. Later collection can therefore plausibly be pregame at its **actual** acquisition time, but cannot be admitted as observations made at 03:30. Actual captured rows cannot be predicted reliably from 15 games: starter availability, active hitters, empty vsPlayer statistics and skips determine the count.

The existing target writes a mutable operational feature table, `mlb.prop_features_precomputed`, keyed by `(prop_type, player_id, game_id, feature_set_tag)`. ON CONFLICT merges features and replaces model_tag/computed_at; this is not an append-only historical capture ledger. There is no existing missing-only/per-game-start gate in this target. A late invocation could change current input state, and must not retroactively alter any frozen prediction, risk classification, outcome or original lineage. Existing daily lineage health passing and compact BvP payload coverage do not prove a fresh September 18 BvP acquisition.

Required late label, if separately approved: `LATE_PREGAME_RECOVERY_NOT_0330_REPLAY`, a distinct actual-time run identity, exact acquisition/observation times, source hashes, explicit admitted/skipped game population and individual pre-start checks. Such data could support subsequently authorized operational features or separately admitted late evidence, not the existing immutable morning ledgers automatically. Partial not-yet-started recovery must enumerate exclusions rather than claim a full original population. No governed late-admission/locking/missing-only entry point proving these requirements was found; hence `BVP_RECOVERY_CONTRACT_UNCLEAR`, not a claim that recovery is already valid.

No recovery command is recommended as contract-valid yet. The existing `make mlb-bvp-pvb-refresh MLB_BVP_DATE=2026-09-18` is an acquisition/upsert target, **not** a safeguarded evidence-grade recovery command; its `--dry-run` still makes network requests. The installed full prewarm wrapper is also unsuitable for BvP-only recovery because it proceeds into prediction/market-context work after acquisition. Any later collection requires separate network and write authorization, review of late admission, and existing lock coordination before execution. Keeping the missed date is safer than silently backdating or repopulating original evidence.

### Prospective resilience options — proposals only

| Option | Failure addressed; duplicate/late/failure risk | Locks/storage and observation semantics | Recommendation |
|---|---|---|---|
| 1. No change | Preserves visible missing date; no duplicate or concealed acquisition failure. Does not prevent next wake-readiness race. | No mutation; preserves gap and original schedule. | Safe default pending approval. |
| 2. Extend existing bounded initial-request retry | Short absent-resolver/link transition; repeated public schedule reads are not paid market requests. Persistent failure still nonzero; preserve exception/attempt/recovery warnings. | Hold existing locks; no write before assembled acquisition. Record actual delayed observation rather than claim 03:30. | **Preferred smallest correction.** Restrict to initial schedule DNS/transport failure, not arbitrary downstream/HTTP/schema faults. |
| 3. Short readiness gate | Detect inactive en0/missing route/resolver before request. Local readiness cannot prove gateway/internet health. False-ready or false-block is possible. | Existing locks; explicit timeout/failure, no fabricated rows. Adds macOS coupling and actual-time delay. | Optional, less minimal than fixing retry budget. |
| 4. launchd retry | Re-enters after calendar failure, but can repeat successful portions and paid downstream context calls if wrapper fails later. Risks hiding partial successes. | Whole-wrapper restart is not a per-phase durable claim; mutable upserts and late observation remain. | Do not enable generic KeepAlive retry. Requires durable phase guards first. |
| 5. Later BvP-only window | Repairs a missed date operationally; late/post-start and duplicate risks require explicit contracts. | Acquire existing locks; missing-only guards, game-start exclusions and separate late provenance required; no rewrite of immutable evidence. | Defer: larger policy/scheduler change. |
| 6. Power-wake reconciliation | Could provide wake lead time before 03:30, but changes sleep intent and existing timing test; does not address arbitrary network loss. | Does not itself govern acquisition/append-only behavior; changes overnight operation, not formulas. | Do not change power policy from this incident alone. |
| 7. Missing-only later daily fallback | Uses later natural dispatch; default fallback currently reruns unconditionally when enabled, not missing-only. Duplicate/upsert/post-start risks remain. | Existing pipeline lock helps serialize, but needs durable completion/eligibility guards and late lineage before admission. | Do not simply enable current fallback. Consider only after late contract is defined. |

Required tests/rollback per option:

1. No-change: validate continued visibility of missing acquisition and no stale stderr; rollback not applicable.
2. Retry: deterministic no-network fixtures for readiness returning after ~7.4 seconds, persistent DNS failure, transport timeout, nonretryable HTTP/schema failure, interruption and zero premature writes; assert attempts, deadline, locks, explicit recovered warning and nonzero exhaustion. Proposed example: same three initial-schedule attempts with 5/10-second waits and a hard monotonic 90-second total deadline (20-second request timeout is not a total-run bound); no implementation or tuning experiment was performed. Roll back only the authorized retry diff to the recorded code baseline, rerun focused tests; keep wrapper hash/provenance unchanged if untouched.
3. Gate: fixtures for absent link/route/resolver, false-positive local readiness and deadline; rollback remove only gate, retain acquisition failure handling.
4. launchd retry: crash-before-write/after-write/after-paid-context fixtures, durable retry eligibility and zero duplicate paid calls; rollback the exact proposed plist change and reload only after separate authorization. No plist change now.
5. Recovery window: fixtures for missing-only, started game exclusions, concurrent entry, explicit late lineage, downstream isolation and idempotency; rollback new proposed window/entry point without deleting captured evidence.
6. Wake policy: preserve complete repeating/one-time schedule and pmset baseline, test actual sleep and wake lead time separately; rollback only proposed event to preserved baseline. No power change now.
7. Fallback: fixtures for complete/missing/partial capture, start cutoff, locks, repeated daily windows and immutable-ledger isolation; rollback missing-only integration and restore fallback default 0, retaining all admitted evidence.

### Source integrity and evidence limits

Installed prewarm SHA-256: `23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb`, exactly the authorized post-change hash in `artifacts/analysis/mlb/operational_reconciliation/2026-09-10/reconciliation_manifest.json`. That manifest, the non-executable byte-exact rollback source and `backend/mlb/tests/test_mlb_bvp_prewarm_exit_semantics.py` govern future validation/rollback; no competing executable source was created. Rollback-source SHA-256 remains `d258117434b64377a2a00a1dc5ec2b10727bd21d054f2bdfee849c721c69e662`. Acquisition source SHA-256: `f552c348c4dcf16edaa2ec7a13b63d276110e3cad58a82e96420a44d5e7e1b21`. The older runbook wrapper example is not a byte-exact current installed source and must not replace it.

Evidence commands were read-only launchctl/plist/pmset inspection, local file/hash inspection and `/usr/bin/log show --info --debug` over bounded retained windows, with identifier redaction before display. Unified-log access required approved execution outside the filesystem sandbox, not interactive sudo; no root command was necessary. Retained processes covered powerd, launchd, configd, mDNSResponder/Network.framework, symptomsd, timed and relevant kernel Ethernet events. No networkd entries were retained in the queried window; this is an availability limit, not proof it was inactive. No authoritative negative upstream DNS packet, historical literal default-route snapshot, renewal packet exchange, full original BvP source capture or explicit governed late-recovery admission contract was available. These limits do not negate the affirmative sleep/link/resolver/recovery sequence.

Files modified by this investigation: this diagnostic Markdown addendum only. Production and ledger state unchanged. Commit: none. Push: none.

### September 18 authorized prospective correction and late-contract resolution

This follow-up supersedes the preceding unresolved recovery decision without changing retained incident evidence. Root cause remains `SLEEP_WAKE_NETWORK_READINESS_RACE`: a missed sleeping-system calendar dispatch began during network dark wake and exhausted its initial DNS retries before en0/resolver readiness returned. No persistent gateway-ARP failure or configd watchdog stall was reproduced. The short No-route messages documented above occurred during wake-time configuration withdrawal, not a proven recurrence of the previous persistent routing defect.

Implemented contract: **`BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1`**, in the tracked acquisition source only. `_fetch_schedule_games` now calls `_fetch_initial_schedule_json`; the old `_fetch_json`, date/population construction, row upsert, main and all other existing functions remain unchanged. No installed-wrapper change, replacement, schedule change, power change or live acquisition was necessary.

Frozen limits: at most three attempts (explicit lower caller limits remain); waits **10 then 20 seconds**, maximum **30 seconds** retry delay; default pre-response attempt wall limits **20/5/5 seconds**, capped by configured timeout and remaining **60-second monotonic budget**. Maximum added pre-response wall time after the first completed failure is approximately **40 seconds**, including the two later request limits. The 30-second delay is 25.5 seconds more than the former default. Normal first-attempt parsed payload is preserved, with retry telemetry added. Receiving response headers ends retry eligibility; HTTP, body-read/parser/schema, database/integrity and programming failures do not trigger another request.

Typed DNS/name-resolution, connection-establishment/unreachable and pre-response timeout failures may retry. SSL/proxy failures, errors carrying a response, unclassified connection errors and unfinished request workers fail closed. The actual schedule GET establishes readiness; no separate connectivity probe is made. A header-only daemon worker bounds an otherwise uninterruptible resolver wait: if it remains unfinished at the wall limit, **no replacement request** is started, and acquisition exits nonzero; a late response is closed. That worker cannot construct BvP inputs or write rows. The approximate 60-second bound covers pre-response attempts/backoff, not the whole subsequent valid-response/BvP processing stage.

Every failed attempt records UTC timestamp, attempt, fixed sanitized classification, next wait and response-received state (`UNKNOWN` rather than an inference for unfinished requests). Recovered initial requests record `ACQUISITION_SUCCESS_AFTER_TRANSIENT_NETWORK_RETRY`, attempts, accumulated delay and first-failure/successful-attempt timestamps, scoped explicitly to `INITIAL_SCHEDULE_FETCH`. Exhausted completed transient requests record `ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED` and remain nonzero. No raw exception message, URL, body, credential or private identifier enters these events or terminal errors.

The installed wrapper still releases both locks on exit. Acquisition success followed by no qualified model remains an explicit downstream/impact skip and exit zero; true exhausted acquisition remains FAILED/nonzero with no DONE marker or data write. Existing acquisition has mutable operational upserts, not an append-only BvP capture ledger; this correction preserves that contract and does not certify late evidence. Later daily fallback remains disabled by default. No agreement-study, Moneyline, Totals, Hits, market, publication or wagering behavior was changed.

Sleep remains acceptable: a calendar schedule cannot itself wake the Mac, missed events may run on wake, and short initialization races are handled by bounded request resilience. This correction does not promise exact 03:30 execution or conceal a delayed observation. No sleep-prevention assertion or wake-policy change was made; actual acquisition timestamps remain material.

Final September 18 recovery classification: **`BVP_RECOVERY_CONTRACT_REQUIRES_AMENDMENT`**. The frozen lineage contracts allow exact aligned retained-source reconstruction, not fresh mutable StatsAPI inputs masquerading as original observations. Probable starters/active rosters/database starter references may change; career vsPlayer reads have no implemented strict-prior date cutoff. September 18's original observation remains missing. A separate operational late capture is not automatically evidence-grade and no recovery command or execution was authorized here.

The reviewed decision and a future-only, **PROPOSED / NOT ACTIVATED** amendment (`BVP_LATE_OPERATIONAL_CAPTURE_AMENDMENT_V1`, earliest slate date September 19) are documented in `docs/MLB BvP Acquisition Retry and Late-Admission Contract V1.md`. It requires separately authorized missing-only/locked acquisition, actual-time late labels, immutable provenance, game-specific pre-start exclusions, qualified source cutoff and frozen-ledger isolation. It cannot retroactively admit September 18 or alter downstream eligibility by itself.

Validation: **36 offline tests passed** in the available python3/pytest environment; the production .venv has no pytest and no package installation/download was attempted. Tests include first success, DNS/connection/timeouts, readiness at 7.4 seconds succeeding exactly once at the next attempt, exhaustion, HTTP/security/semantic/parser/body failures, stuck-worker no-replacement/late-response close, no premature/duplicate writes or write retries, current date, sanitized telemetry and the actual installed wrapper with its real lock helper in isolated fixtures. Wrapper success/skip returns zero; exhaustion is nonzero with both locks released and no DONE. Source-AST validation confirms every pre-existing function unchanged except the initial fetch callee. Production-interpreter compilation, shell syntax, source/installed manifest/hash/mode/launchd-target checks and `git diff --check` passed. Live network/API/database requests: **0**; recovery/production pipeline runs: **0**.

Source pre-change SHA-256: `f552c348c4dcf16edaa2ec7a13b63d276110e3cad58a82e96420a44d5e7e1b21`; post-change: `01472f9ea4f03cd26ed8ea95661d2578a9ce57c7677fc45e6e350ac0e7438866`. Installed wrapper pre/post remains `23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb`, mode 0755; its September 10 governance manifest remains unchanged. New source governance/provenance is `artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_initial_schedule_retry_manifest.json`.

Rollback with separate authorization: `git revert <local retry-correction commit>`; baseline is `502ceb0ef3e1d1fbe7b0e993b737de073f32695f`. Preserve unrelated work/history, then repeat focused tests, compilation/syntax and hash checks. No installed replacement, launchd reload, network/power rollback or data deletion is needed. Changed files: acquisition source; focused retry tests; existing wrapper tests; contract document; new source manifest; this canonical diagnostic. Local commit is authorized for these files only; unrelated study changes remain outside it. Push: none.
