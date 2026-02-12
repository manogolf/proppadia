# Legacy Quarantine Map

These paths were moved out of active code locations and into `backend/_legacy/*`
to prevent accidental runtime coupling.

## MLB

- `backend/mlb/archive` -> `backend/_legacy/mlb/archive`
- `backend/mlb/legacy` -> `backend/_legacy/mlb/legacy`
- `backend/mlb/resolution/archive` -> `backend/_legacy/mlb/resolution/archive`
- `backend/mlb/shared/archive` -> `backend/_legacy/mlb/shared/archive`

## NHL

- `backend/nhl/data/archive` -> `backend/_legacy/nhl/data/archive`
- `backend/nhl/models/archive` -> `backend/_legacy/nhl/models/archive`
- `backend/nhl/scripts/archive` -> `backend/_legacy/nhl/scripts/archive`
- `backend/nhl/sql/archive` -> `backend/_legacy/nhl/sql/archive`

## Notes

- Runtime code should import only from active packages under `backend/app` and `backend/domains`.
- Boundary check script: `backend/scripts/check_runtime_import_boundaries.py`
