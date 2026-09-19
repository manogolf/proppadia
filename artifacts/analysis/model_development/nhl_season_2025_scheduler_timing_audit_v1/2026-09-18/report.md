# NHL season-2025 scheduler timing audit

Audit date: 2026-09-18 PT
Mode: retained evidence only; no network/API request and no NHL pipeline execution

## Direct answer

Season 2025 did not have one stable clock. GitHub Actions changed from **03:15 PDT** daily, to **04:15 and 22:15 PDT**, then to four fixed UTC cron expressions (`12:30`, `20:30`, `00:45`, `07:30`) whose PT meanings changed at DST. The later local era has two strongly proven automatic clusters: **05:45 PT (106 logs)** and **14:30 PT (96 logs)**. The other 68 local invocations are manual/recovery/test unresolved; they are not additional intended windows.

The 186 GitHub runs comprise 86 scheduled and 100 manual dispatches (80 success, 105 failure, one cancelled) from 2025-09-27T02:19:49Z through 2025-11-03T07:36:47Z. Exact per-run GitHub timestamps were not preserved in the local audit evidence, so this package does not fabricate them. The Automator population is exact: 270 logs on 124 dates, 235 success markers, 35 non-success/incomplete logs, and 212 full-pipeline markers. Log bodies do not timestamp completion, so exact ends and durations are unavailable.

Across the local era, 34 dates had one intended-window invocation and 84 had both; 6 observed dates had only an off-window invocation. Internal wrapper retries occurred in 34 invocation logs and remain part of those invocations, not extra daily windows. The five dates with no retained invocation at all were 2026-02-14, 2026-02-17, 2026-02-20, 2026-02-23, 2026-02-24. Median starts were 05:45:01 and 14:30:02 PT.

## Scheduler eras

1. GitHub `70ccd17b` (from 2025-09-23): `10:15 UTC` = `03:15 PDT`.
2. GitHub `5a80166e` (2025-10-04 through 2025-10-23): `11:15 UTC` = `04:15 PDT`; `05:15 UTC` = `22:15 PDT` on the prior local date.
3. GitHub `3d6d919d` (from 2025-10-24; last observed run 2025-11-03): `12:30/20:30/00:45/07:30 UTC`. Before the 2025-11-02 DST transition those map to `05:30/13:30/17:45/00:30 PDT` with local-date caveats; afterward they map to `04:30/12:30/16:45/23:30 PST`, again with prior-local-day mapping for 00:45 and 07:30 UTC.
4. No retained invocation evidence spans 2025-11-04 through 2025-12-08.
5. Local Automator evidence begins 2025-12-09 14:30:04 PST and ends 2026-04-16 14:30:01 PDT. Its exact workflow/Calendar definition is missing, but execution headers and filename clusters prove the two intended local windows.

No season-2025 NHL LaunchAgent was found. The `% to Odds.app` application is an unrelated Safari web app, not Automator. `bin/nhl_ops.sh` is a manual alias, not a scheduler. Current user cron could not be read in this sandbox; retained root-cron verification was empty on 2026-09-02, but that current fact is not projected backward.

## Odds timing and coverage

The archive contains exactly 153 historical provider-as-of bundles and 42 contemporaneous final daily files. The historical bundles all use a 16:00 PT as-of timestamp but were retrieved March 5-9, 2026; they are independent provider-time evidence, not proof a local 16:00 scheduler ran. The contemporaneous files are final overwrite-capable daily copies and usually bind to the last 14:30 run. Exact HTTP response timestamps are absent; the ledger labels provider market timestamps as proxies rather than local capture times.

The retained historical archive has zero full-game Moneyline and zero puck-line objects; it is player-prop evidence. SOG, Points, and Saves are separated in the lead-time histogram. The only paired historical operational comparison available is weak: later final files survive, while the 05:45 raw state was overwritten. Therefore the audit cannot prove that later runs materially improved Points, SOG, Saves, goalie confirmation, or lineup availability. It can prove that the later 14:30 run produced usable player-prop files, including April 16's pregame Points/SOG/Saves coverage.

At bundle level, the closest nonnegative historical as-of snapshot was 0.000 minutes before first puck. The closest contemporaneous final-file proxy was 15.617 minutes. Neither is an exact local HTTP completion time: the former is a provider historical as-of and the latter uses the latest provider market update, so the latter is only an upper bound on response lead time.

## Phase terminology

- `MIDDAY` is a comment in the final GitHub cron era and a formal current season-2026 phase. It is not proven as the name of the 14:30 Automator invocation.
- `FINAL_PREGAME` is a current 2026 rehearsal/runtime label. No retained season-2025 Automator artifact applies that label.
- `PRE_PUCK` is a historical GitHub workflow comment for the `00:45 UTC` cron.
- Phase labels are therefore not inferred solely from the local clock.

## Current season-2026 scheduler

`com.proppadia.nhl.morning-orchestration` is loaded/enabled at 07:30 local, runs prerequisites only, and matches its repository plist byte-for-byte. It is the only fixed NHL calendar time. It is **not** the only NHL automation: loaded `com.proppadia.nhl.mainline-cross-market-shadow` runs every 900 seconds with `RunAtLoad`, selects `MIDDAY` only at 12:00-12:30 PT and `FINAL_PREGAME` when the first future puck is 20-75 minutes away, and uses a per-slate acquisition lock plus durable create-only paid-attempt claim. A prior attempt blocks automatic retry.

MLB's fixed 08:30 refresh does not exactly overlap the 07:30 NHL start. Real-slate duration still needs observation because both use the host, network, Python environment, and PostgreSQL. Outputs and locks are isolated. AC settings currently show system sleep disabled (`sleep=0`) and a repeating 05:27 wake; calendar LaunchAgents do not themselves wake a sleeping Mac.

## Minimal September 19 recommendation (first puck 16:00 PT)

**Do not install another schedule.** Retain the existing 07:30 prerequisite run and existing conditional poll:

- 07:30: construct/certify morning prerequisites and governed prediction inputs; no odds credit.
- One MIDDAY acquisition during 12:00-12:30: immutable phase artifacts; Moneyline and Points remain prediction/market shadow, SOG remains authorized prediction/market/candidate-lineage shadow without upload/execution, Saves remains conditional prediction/market shadow only.
- One FINAL_PREGAME acquisition between 14:45 and 15:40 for a 16:00 puck: a new immutable phase identity, not an overwrite or model-policy change.
- Recovery only after operator review of the durable attempt claim; never automatic replacement of a possibly charged request.

This reproduces the demonstrated two-part operational shape with the fewest paid windows while using current atomic duplicate protection. Expected incremental coverage is **unquantified**, because the historical early raw snapshot was not retained. The 16:30 MLB run is after first puck and cannot serve as a fallback capture slot.

## Evidence boundary

See `scheduler_inventory.csv`, `invocation_ledger.csv`, `historical_time_histogram.csv`, `odds_capture_lead_time_ledger.csv`, `market_family_lead_time_histogram.csv`, `phase_reconciliation.csv`, `current_season_2026_schedule_snapshot.json`, and `proposed_cadence_comparison.csv`. `summary.json` is the machine-readable decision record. No schedule, pipeline, database, model, capture, or publication state was changed.
