# Prod12 Automation Runbook

Purpose: run and monitor the MLB production-12 prediction lane with daily and weekly automation.

Date reference: this runbook was aligned on February 17, 2026.

## Scope

- Prop lane set (`prod12`):
  - `hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed`
- Gate posture:
  - Daily health + logging strict gate (`mlb-prod12-daily-gate`)
  - Weekly promotion/readiness strict gate (`mlb-prod12-phase2-weekly-gate`)

## Render Shell Quickstart

Use this once after each deploy before relying on scheduler jobs:

```bash
cd /opt/render/project/src
make mlb-prod12-bootstrap-strict MLB_BASE_URL="https://baseball-streaks-sq44.onrender.com" MLB_DATE="$(date -u +%F)"
```

What it guarantees:
- weekly and daily cycles both run
- baseline auto-captures if missing
- latest weekly phase2 snapshot is strict-pass
- daily+weekly status is strict-pass with tight freshness checks

## Preferred Scheduler Mode (Thin Trigger)

Use scheduler jobs to call backend ops endpoints only. This keeps dependency/model runtime in one place (backend service) and avoids cron runtime drift.

Required env vars on the scheduler service:
- `PROPPADIA_BACKEND_URL` (example: `https://baseball-streaks-sq44.onrender.com`)
- `OPS_API_TOKEN` (must match backend `OPS_API_TOKEN`)

Trigger command:

```bash
bin/mlb_prod12_remote_trigger.sh
```

Default behavior:
- Trigger defaults to `run_mode=daily` (lighter resource profile).
- Weekly phase-2 is triggered separately.
- Daily cron now defaults to lean mode:
  - `MLB_DAILY_GATE_ENABLED=0` (skips heavy daily gate checks in cron path)
  - `MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED=0` (disables alias/extra market fetches)
  - `MLB_ODDS_MARKETS` scoped to prod12 lane markets only
  - `MLB_ODDS_BOOKMAKERS` defaults to `betonlineag,mybookieag,betopenly,draftkings,betmgm,espnbet,fanatics,williamhill_us,superbook,rebet`
  - `MLB_WIDE_PROP_TYPES` pinned to `MLB_PROD12_PROP_TYPES` unless explicitly overridden

Optional extra lean setting (if memory pressure persists):
- set `MLB_ODDS_BOOKMAKERS` to a small CSV (for example `betonlineag,mybookieag,betopenly,draftkings`)

Status command:

```bash
bin/mlb_prod12_remote_status.sh 120
```

One-command trigger + wait (recommended for manual checks):

```bash
bin/mlb_prod12_remote_trigger_and_wait.sh 2400 10 120
```

This exits non-zero if:
- the run fails,
- state disappears (idle/no `exit_code`),
- `mlb_book_upload.csv` is missing after a successful exit,
- or the post-run local sync of `mlb_book_upload.csv` fails.

Local sync target defaults to:
- `backend/mlb/data/processed/mlb_book_upload.csv`

Override target path with either:
- arg 4: `bin/mlb_prod12_remote_trigger_and_wait.sh 2400 10 120 <out_csv>`
- env var: `MLB_BOOK_UPLOAD_LOCAL_OUT_CSV=<out_csv>`

If the run already finished remotely and you only want the local upload CSV, run this:

```bash
MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=1 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED=1 \
PROPPADIA_BACKEND_URL="$PROPPADIA_BACKEND_URL" \
OPS_API_TOKEN="$OPS_API_TOKEN" \
make mlb-book-upload MLB_DATE="$(date -u +%F)"
```

Behavior:
- default remote kind is `book_upload`, so this writes the local upload CSV directly and exits.
- set `MLB_BOOK_UPLOAD_REMOTE_FETCH_KIND=slate_output` to fetch remote slate first, then build upload CSV locally.

Direct curl equivalents:

```bash
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  -H "X-Ops-Token: $OPS_API_TOKEN" \
  "$PROPPADIA_BACKEND_URL/api/ops/mlb/prod12/trigger" \
  -d '{}'

curl -fsS \
  -H "X-Ops-Token: $OPS_API_TOKEN" \
  "$PROPPADIA_BACKEND_URL/api/ops/mlb/prod12/status?tail_lines=120"
```

Weekly trigger (explicit):

```bash
bin/mlb_prod12_remote_trigger.sh '{"run_mode":"weekly"}'
```

Auto trigger (daily always, weekly only on selected UTC weekday):

```bash
bin/mlb_prod12_remote_trigger.sh '{"run_mode":"auto","weekly_day_utc":1}'
```

## Daily Schedule

Run once per day (UTC date is acceptable):

```bash
bin/mlb_prod12_daily_cycle.sh
```

Expected pass conditions:
- `prediction_gate`: pass
- `prediction_flow_audit`: pass
- `hits_expectation_sources`: pass
- no degraded prop lanes

Primary artifact updated:
- `artifacts/mlb_pipeline_history.jsonl`

## Weekly Schedule

Run once per week:

```bash
bin/mlb_prod12_weekly_cycle.sh
```

What this includes:
1. `mlb-prod12-release-manifest`
2. `mlb-prod12-replay-latency`
3. `mlb-prod12-track-weekly` (candidate eval, max drop `3.5`)
4. `mlb-prod12-phase2-log` and strict latest-status check (`mlb-prod12-phase2-last-strict`)
5. on failure, prints compact incident triage (`mlb-prod12-incident`)
6. always appends operator snapshot history (`mlb-prod12-ops-log`)

Expected pass conditions:
- release manifest: `ok=true`
- replay latency: `ok=true`, `predict p95 <= 4000 ms`
- weekly candidate eval: `ok=true`, `recommendation="promote"`

Primary artifacts updated:
- `artifacts/releases/mlb_prod12_release_manifest.json`
- `artifacts/releases/mlb_prod12_replay_latency.json`
- `artifacts/mlb_prod12_phase2_history.jsonl`

## Model Bundle Publish

When model artifacts are refreshed, publish the bundle with both keys:
- versioned key: `mlb/prod12/mlb_latest_<timestamp>.tgz`
- stable key: `mlb/prod12/latest.tgz`

Command:

```bash
make mlb-prod12-model-bundle-publish
```

This keeps backend `MLB_MODELS_OBJECT_PATH=mlb/prod12/latest.tgz` stable so weekly jobs do not need env updates.

## Operator Actions On Fail

1. Daily lane failure:
- Re-run the same daily command once.
- If still failing, run:
  - `make mlb-prod12-incident`
  - `make mlb-pipeline-check-prod12 MLB_BASE_URL="https://baseball-streaks-sq44.onrender.com" MLB_DATE="<same-date>" MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3`
  - `make mlb-pipeline-last`
- Hold production changes until lane returns to pass.

2. Weekly replay latency failure:
- Re-run weekly bundle once.
- If `predict p95` remains above threshold, keep current lane active but do not widen scope.
- Track `summary_latency.predict.p95_ms` week-over-week from `artifacts/releases/mlb_prod12_replay_latency.json`.

3. Weekly candidate eval failure:
- Keep current prod12 lane (no additional promotion).
- Run:
  - `make mlb-prod12-incident`
  - `make mlb-candidate-eval-prod12 MLB_CANDIDATE_MAX_PROP_DROP_PCT=3.5`
- Review degraded props and continue tracking only.

## Operator Snapshot

Use this command for a compact current-state check outside scheduler runs:

```bash
make mlb-prod12-ops-check
```

Optional history tracking:

```bash
make mlb-prod12-ops-log
make mlb-prod12-ops-last
```

Wrapper script preview:

```bash
make mlb-prod12-script-preview
```

## Preseason Checklist

- Automate bundle publish after retrain/update so `mlb/prod12/latest.tgz` is always refreshed without manual shell steps.
- Use UTC current date by default (`MLB_DATE=$(date -u +%F)`), and set `MLB_DATE` explicitly only for replay/backfill.

## Notes

- `MLB_REPLAY_ALLOW_SPARSE=1` is enabled by default in `Makefile` for sparse/offseason safety.
- The release manifest currently fingerprints artifacts from `models_out`; update `MLB_PROD12_ARTIFACT_DIRS` if MLB artifacts are moved to a dedicated path.
- Wrapper scripts auto-select Python runtime: `.venv/bin/python` when present, otherwise `python3`.
