# Docs Index

Start here for architecture and operations context.

## Architecture

- `docs/Architecture Reset Plan.md`: reset goals, phases, progress snapshot.
- `docs/Runtime Surface.md`: currently served FastAPI routes and entrypoint.
- `docs/Legacy Quarantine Map.md`: backend archive/legacy move map to `backend/_legacy/*`.

## Shared Validation Lane

- `make shared-checks-offline`: cross-sport shared unit checks (`backend/tests/test_shared_*.py`).
- Included automatically by `make mlb-checks-offline` and `make nhl-checks-offline`.
- `make diagnose`: optimized local baseline (`runtime-boundaries` + `shared-checks-offline` + MLB core + NHL core) without rerunning shared checks twice.
- `make ci-offline-checks`: CI/offline baseline (same composition as `diagnose`).
- `make cross-sport-post-deploy BASE_URL=<url>`: one-command offseason-safe strict post-deploy checks for MLB + NHL.
- `docs/Roster Refresh Operations.md`: local + automation runbook for MLB/NHL full-team roster refresh.

## MLB API and Validation

- `docs/MLB Endpoint Matrix.md`: frontend caller -> MLB endpoint mapping.
- `docs/MLB API Contracts.md`: canonical MLB request/response contracts.
- `docs/MLB Smoke Testing.md`: runbooks for `make mlb-checks-*`.
- Includes scheduled market-cache warm command: `make mlb-market-cache-refresh`.
- `docs/MLB Metrics Validation.md`: metrics API-vs-DB validation details.
- `docs/MLB OpenAPI Review.md`: OpenAPI snapshot and drift process.
- `docs/openapi/openapi.snapshot.json`: OpenAPI contract snapshot.

## NHL UX and Validation

- `docs/NHL UX Contract.md`: canonical NHL v1 front-facing UX contract (research + board modes).
- `docs/NHL Smoke Testing.md`: runbooks for `make nhl-checks-*`.
- `docs/NHL OpenAPI Review.md`: NHL OpenAPI snapshot and drift process.

## Other

- `docs/Project Map.md`: broad project inventory notes.
- `docs/MLB Cutover Checklist.md`: final checklist to restore MLB site functionality.
- `docs/Prediction UX Unification Draft.md`: draft plan to unify MLB/NHL prediction UX into a shared research workspace model.
