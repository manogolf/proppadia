# NHL FINAL_PREGAME capture root-cause investigation

Investigation date: 2026-10-18
Scope: retained local NHL code and operational evidence only. No provider,
database, scheduler, or pipeline action was run.

## Finding

October 17 was a qualifying 14-game regular-season slate. The separate
cross-market observer ran every 15 minutes, but every retained poll for that
slate ended `NOOP_MORNING_NOT_READY` with gate reason
`MORNING_READINESS_RECEIPT_ABSENT_OR_BLOCKED`. The observer made zero paid
calls and created no `FINAL_PREGAME` attempt. A daily run later created one
valid immutable `REFRESH` package, but the observer continued to reject the
daily readiness state even though the remaining games still had a
final-pregame timing window.

This is a producer-readiness defect. The morning failure explains why the
first game's window was unavailable; the observer's overly strict gates then
missed a later valid window for games still to be played.

## Producer and trigger

The producer is
`backend/nhl/scripts/run_nhl_mainline_cross_market_capture_warn_only.py`.
Its CLI defaults to `--phase AUTO`; `main()` loads `backend/.env`, runs the
prediction-only observers, checks `morning_capture_allowed()`, then calls
`observe()`. In AUTO mode, `phase_for()` selects `FINAL_PREGAME` when the
first future scheduled start is 20–75 minutes away. Otherwise it selects
`MIDDAY`. Successful mainline artifacts are written under:

```text
artifacts/operational/nhl/cross_market_shadow/
  season=2026/slate_date=YYYY-MM-DD/
    run_type=FINAL_PREGAME/state=<content identity>/
```

The actual local trigger is the
`com.proppadia.nhl.mainline-cross-market-shadow` LaunchAgent. Its checked-in
plist invokes the script with `--slate-date today --phase AUTO`,
`RunAtLoad=true`, and `StartInterval=900` seconds. October 17 status records
classify the invocations as `LAUNCHAGENT` with parent `launchd`, confirming
the schedule was active. This is a polling schedule, not an exact one-time
calendar alarm.

The comprehensive daily CLI is a separate path. Its `EARLY` odds phase maps
to cross-market `REFRESH`; only a daily odds phase explicitly resolved to
`FINAL_PREGAME` maps to that run type. Therefore, `daily --with-odds` alone
does not guarantee a final capture.

## October 17 execution sequence

| Evidence | Observed result |
| --- | --- |
| First scheduled game | 13:00 ET (17:00Z); later remaining starts were 20:00, 21:00, and 22:00 ET |
| 07:30 PT and 14:08 ET daily attempts | Both failed before current-slate lanes started: `IMMUTABLE_POINTS_FINAL_PREGAME_RUN_CARDINALITY:0` in governed roster acquisition. No provider calls or writes were reported by those failed preflights. |
| 95 observer polls on October 17 | All were `NOOP_MORNING_NOT_READY`, gate reason `MORNING_READINESS_RECEIPT_ABSENT_OR_BLOCKED`; every retained status has `live_calls=0`. No paid-attempt claim or final capture was created. |
| 14:34 ET daily attempt | Current-slate Moneyline failed with `CROSS_MARKET_FINAL_SCORE_NOT_OBSERVED_BEFORE_TARGET`; the parent was `FAILED_BLOCKING`. |
| 19:26 ET daily attempt | Parent completed `READY_WITH_BOUNDED_LANE_WARNING`. Required roster, SOG, Points, and Saves lanes completed; their prediction artifacts and the odds observation were retained. The only bounded warning was the nonblocking SOG integrity diagnostic exit 2. Prior-day learning was final and its verified reconciliation package was reused. |
| 19:29 ET daily output | One `REFRESH` cross-market package was created. At the next observer poll, the first remaining game was about 25 minutes away, inside AUTO's 20–75 minute `FINAL_PREGAME` band. |
| Next and subsequent observer polls | They still returned `NOOP_MORNING_NOT_READY`; no `FINAL_PREGAME` attempt or package exists for October 17. |

The morning failures removed the early window for the 13:00 ET game. They do
not explain the missing late capture by themselves: after the 19:29 ET
`REFRESH`, three later games remained and the first was still inside AUTO's
final-pregame window.

## Exact readiness defect

The live `main()` path calls `morning_capture_allowed()` before `observe()`.
Its `_daily_receipt_is_capture_ready()` required exact top-level
`final_classification == "READY"` and completed SOG, Points, and Saves lanes,
their prediction artifacts, and the daily odds-observation manifest. It
therefore rejected the Oct. 17 `READY_WITH_BOUNDED_LANE_WARNING` receipt
before its actual mainline source checks could run, even though the warning
was limited to the nonblocking SOG-integrity diagnostic.

The `auto_capture_readiness_decision()` helper separately rejected
`REUSED_VALID_PACKAGE` in its prior-learning status set. Call-graph inspection
shows this helper was only used by offline readiness tests, not by the
LaunchAgent's live `main()` or `observe()` path. It was not a second live
blocker on Oct. 17. The live observer instead validates prior-day official
outcomes through `load_official_outcomes()` and its immutable source manifest.

The 19:29 ET daily receipt had completed prior-day outcomes, Moneyline and
Puck Line grades, a reused reconciliation package, completed current-slate
lanes, predictions, and a valid odds observation. At the next 15-minute poll,
the first remaining game was about 25 minutes away. The exact daily gate
blocked before `observe()` could validate the mainline schedule/history or
select the phase; no capture was attempted.

The scheduler is expected to create one final phase package for a qualifying
regular-season slate when its readiness and timing gates pass. It is not
created automatically by the daily CLI, and the present implementation does
not promise one separate final snapshot per start-time group. With staggered
starts, the AUTO poll evaluates the first future start: the later starts can
leave another opportunity after an earlier game begins, as they did on
October 17, but a readiness block lasting through all remaining start windows
can still miss the slate's only final capture.

## Bounded date check

The retained cross-market tree contains one `FINAL_PREGAME` state for each
regular-season date from September 29 through October 16, 2026. Scheduler
status records on sampled dates September 29, October 1, October 10, October
14, and October 16 include `CAPTURED` `FINAL_PREGAME` invocations. October 17
is the first regular-season date in that retained range with REFRESH evidence
but no final-state directory or successful final observer status. Preseason
captures before September 29 are not part of this regular-season comparison.
This local check establishes a bounded onset, not a claim about dates after
October 17.

## Consequences and fallback

The October 17 REFRESH remains valid REFRESH evidence and is not a substitute
for a final-pregame capture. No capture-bound Moneyline or Puck Line final
predictions and prices exist to grade against that slate's official outcomes.
Those games are absent from paired final-capture evaluation and from any
capture-based cumulative Moneyline/Puck Line grading for this slate. The
source-backed official outcomes remain available; the immutable prediction
and price lineage needed for those grades does not.

Commit `1553e365`'s reference-only prior-learning fallback is not a capture
repair. It correctly lets source-backed learning continue and leaves missing
Moneyline/Puck Line capture grades unavailable; direct reconciliation still
requires exactly one valid final capture. These controls prevent fabrication
and substitution, but without a producer health metric they do not prevent
recurring capture gaps.

## Recommended next implementation

Correction completed locally: the live mainline observer no longer consumes a
daily receipt as its readiness authority. It proceeds to its existing
source-bound official-outcome, schedule/history, model, odds chronology,
create-only identity, and paid-attempt checks. The schedule-only rehearsal
reports window eligibility but marks mainline input validation as deferred.
The LaunchAgent cadence, 20–75 minute AUTO window, capture identity, provider
controls, and direct reconciliation's exact-one FINAL_PREGAME requirement
remain intact. Focused offline validation and local commit are recorded with
the correction; no Oct. 17 artifact was created or relabeled.
