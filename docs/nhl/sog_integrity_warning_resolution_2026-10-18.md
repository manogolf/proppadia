# NHL SOG integrity warning resolution — 2026-10-18

## Finding

The October 17 daily parent receipt records the research-integrity child
`backend/nhl/scripts/sog_integrity_report.py` exiting with status 2. The parent
receipt retains the command, duration, and exit code, but not child stdout or
stderr. The run directory contains no separate child log.

The script's exit-2 paths include a failed database SOG contract query or
eligible player-game pairs without a prediction. TOI anomalies in the optional
30-day report are printed as counts; they do not determine its exit status.

Using the restricted NHL research identity for bounded read-only diagnosis, the
prediction contract found 44 pairs missing from `nhl.predictions`. Grouping
those keys by game showed:

| Game ID | Start time UTC | Missing pairs | Start relative to run cutoff |
| --- | --- | ---: | --- |
| `2026020121` | 2026-10-17 17:00 | 43 | Before cutoff |
| `2026020130` | 2026-10-17 23:00 | 1 | Before cutoff |

The retained feature cutoff was `2026-10-17T23:28:49Z`; both games were
already started and are listed in the parent receipt's `started_excluded_game_ids`.
The report had used prior shift-game count as its only eligibility criterion,
so it incorrectly demanded predictions for those started games.

## Correction and bounded verification

The daily invocation now passes the run's already-computed
`eligible_pregame_game_ids` to the integrity report. The database contract
scopes its completeness check to those IDs. Standalone use without the option
keeps its previous all-slate behavior; an explicit empty list scopes to no
games. No feature construction, prediction eligibility, scoring, or quality
gate changed.

The corrected report was run read-only through the restricted research
connection for eligible games `2026020131`–`2026020134`. It reported 181 slate
pairs, 181 eligible pairs, and 0 missing predictions. Existing schema,
low-variance, and rolling feature checks ran unchanged. The report did print a
stale warning for the mutable global calibrated-predictions CSV, which currently
contains April 16 rows; this is later local artifact state and was not the
October 17 fatal contract condition.

The October 16 total-TOI check reported 72 rows and 72 players, with no NULL,
zero, sub-minute, over-35-minute, or negative total-TOI values. The separate
30-day query reported 140 NULL total-TOI values, one zero, no over-35-minute or
negative total-TOI values, and 3,419 NULL PP-TOI values. All 140 total-TOI NULLs
were in four completed game-type-1 games: `2026010002` (32), `2026010020` (36),
`2026010022` (36), and `2026010044` (36). The report assigns no exit threshold
to those window counts. Their source correctness remains unverified; the rows
were not changed.

## Verification and follow-up

- `./bin/python -m pytest backend/nhl/tests/test_sog_integrity_report.py -q`:
  9 passed (including 3 subtests).
- Related NHL daily startup and SOG prediction receipt tests, together with the
  focused integrity tests: 27 passed (including 3 subtests).
- `./bin/python -m py_compile backend/nhl/scripts/sog_integrity_report.py backend/nhl/cli.py`:
  passed.
- Restricted production read-only replay for eligible IDs: exit 0, zero missing
  eligible predictions.
- No database writes, source repairs, acquisition, backfill, or model changes.

At the next natural daily run, confirm the recorded integrity child command
contains the current run's eligible game IDs, the SOG contract reports zero
missing pairs in that population, and the child exits 0. Validate the separate
preseason TOI NULL population against official source evidence before proposing
any source correction.
