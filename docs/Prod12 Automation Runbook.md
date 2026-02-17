# Prod12 Automation Runbook

Purpose: run and monitor the MLB production-12 prediction lane with daily and weekly automation.

Date reference: this runbook was aligned on February 17, 2026.

## Scope

- Prop lane set (`prod12`):
  - `hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis`
- Gate posture:
  - Daily health + logging strict gate (`mlb-prod12-daily-gate`)
  - Weekly promotion/readiness strict gate (`mlb-prod12-phase2-weekly-gate`)

## Daily Schedule

Run once per day (UTC date is acceptable):

```bash
make mlb-prod12-daily-gate \
  MLB_BASE_URL="https://baseball-streaks-sq44.onrender.com" \
  MLB_DATE="$(date -u +%F)" \
  MLB_PREDICT_SAMPLE=10 \
  MLB_PREDICT_MIN_SUCCESS=3
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
make mlb-prod12-phase2-weekly-gate \
  MLB_BASE_URL="https://baseball-streaks-sq44.onrender.com" \
  MLB_DATE="2025-08-15" \
  MLB_REPLAY_SAMPLE=10 \
  MLB_REPLAY_MIN_SUCCESS=3 \
  MLB_REPLAY_MAX_PREDICT_P95_MS=4000 \
  MLB_REPLAY_RETRY_ATTEMPTS=2 \
  MLB_REPLAY_RETRY_BACKOFF_MS=350
```

What this includes:
1. `mlb-prod12-release-manifest`
2. `mlb-prod12-replay-latency`
3. `mlb-prod12-track-weekly` (candidate eval, max drop `3.5`)
4. `mlb-prod12-phase2-log` and strict latest-status check (`mlb-prod12-phase2-last-strict`)

Expected pass conditions:
- release manifest: `ok=true`
- replay latency: `ok=true`, `predict p95 <= 4000 ms`
- weekly candidate eval: `ok=true`, `recommendation="promote"`

Primary artifacts updated:
- `artifacts/releases/mlb_prod12_release_manifest.json`
- `artifacts/releases/mlb_prod12_replay_latency.json`
- `artifacts/mlb_prod12_phase2_history.jsonl`

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

## Notes

- `MLB_REPLAY_ALLOW_SPARSE=1` is enabled by default in `Makefile` for sparse/offseason safety.
- The release manifest currently fingerprints artifacts from `models_out`; update `MLB_PROD12_ARTIFACT_DIRS` if MLB artifacts are moved to a dedicated path.
