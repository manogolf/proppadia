# Roster Refresh Operations

One place for MLB/NHL roster refresh commands and expected behavior.

## Ops Quickstart

### In Season

Run both leagues (or each league independently) at least daily:

```bash
make roster-refresh-all
```

Target one league:

```bash
make mlb-roster-refresh-all
make nhl-roster-refresh-all
```

### Offseason / Break Windows

- Keep automation enabled, but sparse/empty roster windows can be normal.
- Use manual date overrides for historical checks:

```bash
make mlb-roster-refresh-all MLB_ROSTER_DATE=2025-08-15
make nhl-roster-refresh-all NHL_ROSTER_DATE=2025-11-20
```

## Local Commands

Run from repo root:

```bash
make mlb-roster-refresh-all MLB_ROSTER_DATE=2025-08-15
make nhl-roster-refresh-all NHL_ROSTER_DATE=2025-11-20
make roster-refresh-all MLB_ROSTER_DATE=2025-08-15 NHL_ROSTER_DATE=2025-11-20
```

Notes:
- `mlb-roster-refresh-all` runs `backend/mlb/scripts/refresh_mlb_players_rosters.py`.
- `nhl-roster-refresh-all` runs `python -m backend.nhl.cli refresh-rosters-all`.
- `roster-refresh-all` is an umbrella target that runs MLB then NHL.

## Scheduling and Execution

GitHub-hosted cron automation is retired. The Mac-based local pipelines remain
the operational authority; these commands and their launchd schedules are
unchanged by the hosted-workflow retirement.

### MLB

The Mac-based local MLB pipeline is the operational authority. Its roster
refresh runs through the existing Make target and restricted ingest routing:

```bash
make mlb-roster-refresh-all MLB_ROSTER_DATE=$(date +%F)
```

GitHub-hosted MLB roster automation is retired. Do not provision a hosted
roster credential or use the removed workflow.

#### Off-season roster-only job

In season, the roster refresh runs inside the daily MLB refresh
(`com.proppadia.mlb.refresh.daily`). While that job is paused for the season
closeout, `com.proppadia.mlb.offseason-roster-refresh` runs
`bin/mlb_offseason_roster_refresh.sh` daily at 06:15 local. It runs only
`make mlb-roster-refresh-all` through restricted ingest, with the same retries
as the daily wrapper. It shares the `mlb-pipeline` lock and skips itself while
the daily refresh is loaded. This keeps the ops page's 30-hour MLB Roster
Freshness check passing. Off-season, the StatsAPI `active` roster is the
40-man roster, so free agents leaving a 40-man roster are marked inactive.

```bash
bin/mlb_install_offseason_roster_refresh.sh             # install (operator)
launchctl kickstart gui/$(id -u)/com.proppadia.mlb.offseason-roster-refresh  # run now
bin/mlb_install_offseason_roster_refresh.sh --rollback  # remove; also at the 2027 restart
```

Logs: `artifacts/ops/mlb_offseason_roster_refresh.{out,err}.log`.

### NHL

NHL roster refresh is executed in NHL daily automation through:
- `python -m backend.nhl.cli daily --with-odds`

`backend.nhl.cli daily` calls full-team roster refresh near the start of the run.

## Offseason Expectations

- Sparse or empty upstream rosters can be normal during breaks.
- MLB/NHL refresh scripts are built to avoid destructive inactive-mark flips when fetch coverage is poor.
- Empty payload periods should be interpreted as data availability windows, not immediate pipeline failure.
