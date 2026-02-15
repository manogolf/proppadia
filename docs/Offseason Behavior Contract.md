# Offseason Behavior Contract

Contract date: February 15, 2026

Purpose: define which empty/sparse states are normal in offseason windows, so operators and UI do not treat "no games" as pipeline failure.

## Core Principle

- `ok=true` with zero game/slate rows is a valid offseason state.
- Transport/auth/DB failures remain failures regardless of season.

## MLB Contract

Primary endpoints:

- `/api/mlb/schedule`
- `/api/mlb/standings`
- `/api/mlb/roster-freshness`

Expected offseason behavior:

1. `/api/mlb/schedule` may return `totalGames=0` for a date.
2. `/api/mlb/standings` can still return cached/upstream records and does not imply active games today.
3. `/api/mlb/roster-freshness` should continue reporting row counts, age, and stale status independently of game volume.

Interpretation rules:

1. `schedule.totalGames=0` means "no games today", not pipeline failure.
2. `roster-freshness.ok=true` plus `schedule.totalGames=0` is healthy offseason behavior.
3. Missing/failed HTTP response (non-2xx, `ok=false`, exception) is failure.

## NHL Contract

Primary endpoint:

- `/api/nhl/slate/meta`

Expected offseason behavior:

1. `ok=true` with component counts of `0` (`games_today`, `props_today`, `sog`, `saves`) is valid when no slate exists.
2. Players-by-team roster page may show full rostered population with optional "In today's slate" filter yielding empty.

Interpretation rules:

1. `ok=true` + zero counts means "no slate today".
2. `ok=false` or transport errors indicate pipeline failure and should surface as failure.

## Post-Deploy Gate Policy

Outside active slates, use strict-offseason gates:

- `make mlb-post-deploy-strict-offseason BASE_URL=<url>`
- `make nhl-post-deploy-strict-offseason BASE_URL=<url>`
- `make cross-sport-post-deploy BASE_URL=<url>`

These enforce strict transport/DB health while tolerating sparse probe data.

## UI Contract

Home snapshot:

1. MLB activity state comes from schedule game count (`totalGames`), not standings record count.
2. NHL activity state comes from slate meta component counts.
3. "No games today"/"No slate today" is informational, not error state.

Ops dashboard:

1. Data freshness cards report source/cache staleness and roster freshness separately.
2. Roster freshness stale/fail is operational concern even when no games are scheduled.

