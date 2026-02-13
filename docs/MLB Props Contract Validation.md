# MLB Props Contract Validation

## Purpose

Validate the DB-level contract consumed by frontend `PlayerPropsTable`.

Script:
- `backend/scripts/validate_mlb_props_contract.py`

## What It Checks

- Required columns exist on `public.player_props`
- Sample recent rows satisfy UI assumptions:
  - `game_date` parseable as date
  - `prop_value` numeric
  - `prop_type` non-empty
  - `over_under` in `over|under`

## Run Commands

```bash
.venv/bin/python backend/scripts/validate_mlb_props_contract.py
.venv/bin/python backend/scripts/validate_mlb_props_contract.py --sample-limit 100
```

Also available via:

```bash
make mlb-checks-props-contract
```

## Requirements

- `DATABASE_URL` or `SUPABASE_DB_URL` configured
- network path to DB reachable
- `psycopg` available in active Python env

## Exit Codes

- `0`: contract checks passed
- `1`: missing required columns, invalid sample rows, or DB/connectivity/runtime errors
