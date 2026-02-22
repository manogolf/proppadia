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

## Automation

### MLB

GitHub Actions workflow:
- `.github/workflows/mlb-refresh-player-ids.yml`

Required secret:
- `SUPABASE_DB_URL`

Manual dispatch input:
- `mlb_roster_date` (optional `YYYY-MM-DD`)

### NHL

NHL roster refresh is executed in NHL daily automation through:
- `python -m backend.nhl.cli daily --with-odds`

`backend.nhl.cli daily` calls full-team roster refresh near the start of the run.

Manual NHL refresh workflow:
- `.github/workflows/nhl-refresh-rosters.yml`
- Required secret: `SUPABASE_DB_URL`
- Manual dispatch input: `nhl_roster_date` (optional `YYYY-MM-DD`)

## Offseason Expectations

- Sparse or empty upstream rosters can be normal during breaks.
- MLB/NHL refresh scripts are built to avoid destructive inactive-mark flips when fetch coverage is poor.
- Empty payload periods should be interpreted as data availability windows, not immediate pipeline failure.
