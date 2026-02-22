# Legacy Quarantine Map

These paths were moved out of active code locations into `backend/_legacy/*`
to prevent accidental runtime coupling. MLB legacy quarantine has since been
fully removed from the repo after Python pipeline replacement.

## MLB

- Legacy quarantine previously used `backend/_legacy/mlb/*`.
- As of 2026-02-22, MLB legacy quarantine files were removed from git.

## NHL

- `backend/nhl/data/archive` -> `backend/_legacy/nhl/data/archive`
- `backend/nhl/models/archive` -> `backend/_legacy/nhl/models/archive`
- `backend/nhl/scripts/archive` -> `backend/_legacy/nhl/scripts/archive`
- `backend/nhl/sql/archive` -> `backend/_legacy/nhl/sql/archive`

## Notes

- Runtime code should import only from active packages under `backend/app` and `backend/domains`.
- Boundary check script: `backend/scripts/check_runtime_import_boundaries.py`
