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
