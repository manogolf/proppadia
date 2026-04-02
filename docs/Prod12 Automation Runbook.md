# Prod12 Automation Runbook

Purpose: run and monitor the MLB production-12 prediction lane with daily and weekly automation.

Date reference: this runbook was aligned on February 17, 2026.

## Scope

- Prop lane set (`prod12`):
  - `hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis`
  - Candidate required-props stability gate remains scoped to 11 props (excludes `runs_rbis`) by design.
- Gate posture:
  - Daily health + logging strict gate (`mlb-prod12-daily-gate`)
  - Weekly promotion/readiness strict gate (`mlb-prod12-phase2-weekly-gate`)

## Stat-Derived Coverage Default

- Batter stat-derived insert coverage now defaults to full coverage (`1.0`), not sampled `0.2`.
- Make variable: `MLB_STAT_BATTER_SAMPLE_RATIO` (default `1.0`).
- Applies to:
  - `make mlb-insert-stat-derived`
  - `make mlb-stat-derived-refresh`
  - `make mlb-stat-derived-backfill`
  - `make mlb-daily-refresh`

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
- Daily trigger now defaults to running the daily gate:
  - `MLB_DAILY_GATE_ENABLED=1` unless explicitly set to `0`
  - `MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED=0` (disables alias/extra market fetches)
  - `MLB_ODDS_MARKETS` scoped to prod12 lane markets only
  - `MLB_ODDS_BOOKMAKERS` defaults to `betonlineag,mybookieag,betopenly,draftkings,betmgm,espnbet,fanatics,williamhill_us,superbook,rebet`
  - `MLB_WIDE_PROP_TYPES` pinned to `MLB_PROD12_PROP_TYPES` unless explicitly overridden

Optional extra lean setting (if memory pressure persists):
- set `MLB_ODDS_BOOKMAKERS` to a small CSV (for example `betonlineag,mybookieag,betopenly,draftkings`)
- set `MLB_DAILY_GATE_ENABLED=0` to skip daily gate checks

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
- the post-run local sync of `mlb_book_upload.csv` fails,
- or post-run local sync of prod12 status histories fails (default behavior).

Local sync target defaults to:
- `backend/mlb/data/processed/mlb_book_upload.csv`
- `artifacts/mlb_pipeline_history.jsonl`
- `artifacts/mlb_prod12_phase2_history.jsonl`

Override target path with either:
- arg 4: `bin/mlb_prod12_remote_trigger_and_wait.sh 2400 10 120 <out_csv>`
- env var: `MLB_BOOK_UPLOAD_LOCAL_OUT_CSV=<out_csv>`

Optional history-sync controls:
- disable history sync entirely: `MLB_REMOTE_SYNC_STATUS_HISTORY=0`
- keep history sync but do not fail on history-sync errors: `MLB_REMOTE_SYNC_STATUS_HISTORY_REQUIRED=0`
- override history output paths:
  - `MLB_PIPELINE_HISTORY_LOCAL_OUT=<path>`
  - `MLB_PROD12_PHASE2_HISTORY_LOCAL_OUT=<path>`

Day-to-day local upload build (primary workflow):

```bash
MLB_POLICY_PLAN_ENABLED=0 \
make mlb-book-upload MLB_DATE="$(TZ=America/New_York date +%F)"
```

Policy-on variant (optional legacy behavior):

```bash
make mlb-book-upload MLB_DATE="$(TZ=America/New_York date +%F)"
```

Remote sync-only fallback (use only when you intentionally want to pull the remote artifact):

```bash
MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=1 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED=1 \
PROPPADIA_BACKEND_URL="$PROPPADIA_BACKEND_URL" \
OPS_API_TOKEN="$OPS_API_TOKEN" \
make mlb-book-upload MLB_DATE="$(TZ=America/New_York date +%F)"
```

Adaptive "best of bunch" trim (optional legacy path; skip if you are filtering directly in the tool):

```bash
make mlb-book-upload-top-recommended
```

When using `mlb-book-upload-top-recommended`, defaults are:
- trims current `backend/mlb/data/processed/mlb_book_upload.csv` to adaptive top-40
- uses recent `artifacts/mlb_postgrade_by_prop_daily_tracker.csv` (lookback 5 days)
- scores with rolling windows `7,14` by default when available
- early season fallback is automatic: if full 7d/14d history is not present yet, the selector degrades to available history and continues
- balanced lane status is emitted per prop in recommendation JSON:
  - `promote`: graded `7d` and `14d` ROI both `> 0` with min rows `7d>=15`, `14d>=30`
  - `bench`: graded `7d` and `14d` ROI both `< 0` with min rows `7d>=15`, `14d>=30`
  - otherwise `watch` (including insufficient sample history)
- enforces side-balance nudge (`min_overs=4`) when overs are available

Outputs:
- `backend/mlb/data/processed/mlb_book_upload_top40_recommended.csv`
- `tmp/analysis/mlb_book_upload_filter_recommendation.json`

Optional tuning example:

```bash
make mlb-book-upload-top-recommended \
  MLB_BOOK_UPLOAD_FILTER_TARGET_ROWS=40 \
  MLB_BOOK_UPLOAD_FILTER_LOOKBACK_DAYS=7 \
  MLB_BOOK_UPLOAD_FILTER_WINDOWS_DAYS=7,14 \
  MLB_BOOK_UPLOAD_FILTER_MIN_MODEL_WIN_RATE_PCT=53 \
  MLB_BOOK_UPLOAD_FILTER_MIN_OVERS=6
```

Remote sync-only behavior:
- default remote kind is `book_upload`, so this writes the local upload CSV directly and exits.
- when `kind=book_upload`, companion artifacts are also synced locally by default:
  - `backend/mlb/data/processed/mlb_slate_output.csv`
  - `backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv`
  - `backend/mlb/exports/odds_history/YYYY-MM-DD/manifest.json`
- disable companion sync by setting `MLB_BOOK_UPLOAD_REMOTE_FETCH_COMPANIONS=0`.
- set `MLB_BOOK_UPLOAD_REMOTE_FETCH_KIND=slate_output` to fetch remote slate first, then build upload CSV locally.

## Step 7 (After Graded Wagers Are Posted)

Use this one-step command after graded wagers are posted and next-day cron has settled outcomes:

```bash
make mlb-post-grade-next-day MLB_RECONCILE_BOOKMAKER=betonlineag
```

It:
- auto-picks your newest `~/Downloads/8rainstation_daily_*.csv`,
- splits it into `tmp/graded/*_mlb_player_props.csv`,
- infers the grader date,
- runs reconcile + model-vs-fade + all-available + graded-wager tracker updates for that date.

Equivalent direct target:

```bash
make mlb-post-grade-step7 MLB_RECONCILE_BOOKMAKER=betonlineag
```

If needed, pin a specific grader file:

```bash
make mlb-post-grade-step7 \
  MLB_GRADER_IN_CSV="$HOME/Downloads/8rainstation_daily_YYYY_MM_DD.csv" \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Important:
- `make mlb-post-grade-all-available-check ...` only rebuilds reconcile + all-available report.
- It does **not** split/read the current grader CSV, so it won’t refresh placed graded-wager metrics by itself.

Post-grade model-vs-fade check (optional standalone):

```bash
make mlb-post-grade-fade-check \
  MLB_RECONCILE_FROM_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Notes:
- reconcile now defaults to `odds_latest_compatible.json` via `MLB_RECONCILE_ODDS_FILENAME`.
- override only when needed, for example: `MLB_RECONCILE_ODDS_FILENAME=odds_mlb_playerprops.json`.
- reconcile now auto-falls back between `odds_latest_compatible.json` and `odds_mlb_playerprops.json` when one filename is missing for a day; fallback dates are recorded in the summary JSON.

Outputs:
- `tmp/analysis/mlb_model_vs_fade_summary.json`
- `tmp/analysis/mlb_model_vs_fade_by_prop.csv`

This routine rebuilds reconcile rows for the window, then compares:
- model-picked side ROI (`pnl_model_pick_1u`)
- opposite-side fade ROI (the opposite side at the same row)

Post-grade all-available resolved report (recommended daily):

```bash
make mlb-post-grade-all-available-check \
  MLB_RECONCILE_FROM_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Outputs:
- `tmp/analysis/mlb_all_available_summary.json`
- `tmp/analysis/mlb_all_available_by_prop.csv`

This routine rebuilds reconcile rows for the window, then reports:
- all available resolved rows
- two-sided resolved rows
- model win rate across resolved rows
- per-prop over/under hit rates and model win rate

Post-grade daily tracker table + charts (recommended daily):

If your grader export is still a combined file, split it first:

```bash
GRADER_CSV="$(ls -t ~/Downloads/8rainstation_daily_*.csv | head -n 1)"
[ -n "$GRADER_CSV" ] || { echo "No grader CSV found in ~/Downloads"; exit 1; }
.venv/bin/python backend/scripts/split_grader_csv_by_sport.py --in-csv "$GRADER_CSV"
```

```bash
make mlb-post-grade-report-and-track-latest \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Optional strict mode for placed-wager ingestion (fail if no split MLB grader file exists under `tmp/graded`):

```bash
make mlb-post-grade-report-and-track-latest \
  MLB_RECONCILE_BOOKMAKER=betonlineag \
  MLB_GRADED_REPORT_REQUIRED=1
```

Outputs:
- `artifacts/mlb_postgrade_daily_tracker.csv`
- `artifacts/mlb_postgrade_by_prop_daily_tracker.csv`
- `artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json`
- `artifacts/analysis/mlb/mlb_postgrade_alerts_history.jsonl`
- `artifacts/analysis/mlb/mlb_postgrade_dashboard.png`
- `artifacts/analysis/mlb/mlb_postgrade_roi.png`
- `artifacts/analysis/mlb/mlb_postgrade_winrate.png`
- `artifacts/analysis/mlb/mlb_postgrade_volume.png`
- `tmp/analysis/mlb_graded_wagers_summary.json`
- `tmp/analysis/mlb_graded_wagers_by_prop.csv`
- `tmp/analysis/mlb_graded_wagers_rows.csv`

Notes:
- post-grade reconcile now requires outcomes by default (fails fast if outcomes are unavailable or zero for the window).
- the post-grade tracker now merges three lenses in one place:
  - placed graded wagers (from latest `tmp/graded/8rainstation_daily_*_mlb_player_props.csv`)
  - model-vs-fade (reconcile rows)
  - all-available resolved slate metrics (reconcile rows)
- tracker now enforces graded-date alignment by default: if graded summary `report_date` does not match tracker `report_date`, the run fails to prevent stale graded metrics from being written.
- tracker upserts one row per `report_date` (re-runs replace that date, no duplicate rows).
- charts require `matplotlib` in `.venv` (install once: `.venv/bin/pip install matplotlib`).
- automatic alerts now include:
  - fade beating model on meaningful paired-bet sample
  - model ROI breach threshold
  - overall and per-prop short-window win-rate drops
- strict mode (optional): fail command on critical alerts

```bash
make mlb-post-grade-tracker MLB_POSTGRADE_ALERTS_STRICT=1
```

- optional override (not recommended): allow tracker write even when graded summary date mismatches tracker date

```bash
make mlb-post-grade-tracker MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH=1
```

- to rebuild only the placed graded-wager summary from a specific split file:

```bash
make mlb-graded-wagers-report \
  MLB_GRADED_IN_CSV="tmp/graded/8rainstation_daily_YYYY-MM-DD_mlb_player_props.csv"
```

- ET convenience alias (single-date post-grade run):

```bash
make mlb-post-grade-report-and-track-et
```

- latest-archive convenience alias (recommended to avoid date rollover mismatches):

```bash
make mlb-post-grade-report-and-track-latest
```

- to append only tracker row/charts (without rebuilding reports):

```bash
make mlb-post-grade-tracker
```

Cross-sport sanity check (NHL + MLB summaries):

```bash
make cross-sport-model-vs-fade-strict
```

Output:
- `tmp/analysis/cross_sport_model_vs_fade_summary.json`

One-command post-grade routine (rebuild both sport summaries, then strict cross-sport gate):

```bash
make cross-sport-post-grade-fade-check \
  MLB_RECONCILE_FROM_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_TO_DATE="$(TZ=America/New_York date +%F)"
```

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

Run once per week (thin-trigger path, recommended):

```bash
bin/mlb_prod12_remote_trigger_weekly.sh
```

Direct local weekly cycle (no remote ops trigger) remains available:

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

### Weekly Candidate/Review Runs

Weekly remote trigger runs phase2 candidate/review flow by default.
Retrain/recompute cadence is disabled by default in remote mode.

Trigger:

```bash
bin/mlb_prod12_remote_trigger_weekly.sh
```

Monitor until complete:

```bash
set -a
source backend/.env
set +a
bin/mlb_prod12_remote_status.sh 180 | jq '{status,running,exit_code,run_id,started_at,finished_at}'
```

Review checkpoints after success:
- latest phase2 snapshot strict-pass: `make mlb-prod12-phase2-last-strict`
- current prod12 status strict-pass: `make mlb-prod12-status-strict`
- candidate decision in latest phase2 snapshot (`recommendation`, `overall_lift_pct`, degraded props)

Optional toggles for weekly trigger:
- enable retrain/recompute stage for one run: `MLB_WEEKLY_RETRAIN_CADENCE_ENABLED=1`
- make retrain/recompute stage hard-fail weekly run: `MLB_WEEKLY_RETRAIN_CADENCE_REQUIRED=1`

## Model Bundle Publish

When model artifacts are refreshed, publish the bundle with both keys:
- versioned key: `mlb/prod12/mlb_latest_<timestamp>.tgz`
- stable key: `mlb/prod12/latest.tgz`

Command:

```bash
make mlb-prod12-model-bundle-publish
```

This keeps backend `MLB_MODELS_OBJECT_PATH=mlb/prod12/latest.tgz` stable so weekly jobs do not need env updates.

## Retrain/Recompute Cadence

Suggested cadence:
- daily: keep running normal prod12 daily automation only
- weekly: run retrain/recompute locally, then publish bundle if promoted

Migration mode:
- use market/reconcile rows for quality + candidate evaluation (no `model_training_props/mlb_api` dependency)

Recommended weekly sequence:

```bash
make mlb-retrain-prereq-check
make mlb-reconcile-rows \
  MLB_RECONCILE_FROM_DATE="2025-03-01" \
  MLB_RECONCILE_TO_DATE="$(date -u +%F)" \
  MLB_RECONCILE_BOOKMAKER= \
  MLB_RECONCILE_ODDS_FILENAME="odds_latest_compatible.json" \
  MLB_RECONCILE_ROWS_OUT_CSV="tmp/mlb_base_vs_market_rows_anybook.csv"
make mlb-retrain-broad-reconcile \
  MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows_anybook.csv" \
  MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0
make mlb-prediction-quality-prod12 \
  MLB_QUALITY_SOURCE_TABLE="reconcile_rows" \
  MLB_QUALITY_ROWS_CSV="tmp/mlb_base_vs_market_rows_anybook.csv" \
  MLB_QUALITY_PROP_SOURCES=""
make mlb-candidate-eval-prod12 \
  MLB_CANDIDATE_SOURCE_TABLE="reconcile_rows" \
  MLB_CANDIDATE_ROWS_CSV="tmp/mlb_base_vs_market_rows_anybook.csv"
```

`mlb-retrain-broad-reconcile` now runs the reconcile-based quality + candidate checks automatically at the end.

Current caveat:
- `runs_rbis` reconcile rows require snapshots that include one of the alias keys (`batter_runs_rbis`, `batter_runs_rbi`, `batter_r+rbi`).
- Older archived snapshots may still have zero `runs_rbis` coverage; in strict reconcile mode (`MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0`), that prop will be skipped for those windows.
- Reconcile builder now supports synthetic backfill from `mlb.model_training_props` for missing props (default includes `runs_rbis`):
  - `--derive-props-from-mtp runs_rbis`
  - Synthetic rows use `market_key=derived:runs_rbis` and no price columns.
  - Trainer allows this lane by default via `MLB_TRAIN_RECONCILE_ALLOW_MISSING_PRICE_PROPS=runs_rbis`.
- Broad/hybrid recompute gates treat `runs_rbis` as non-blocking by default (`MLB_RECOMPUTE_NON_BLOCKING_PROPS=runs_rbis`), so missing market support does not hold the entire lane.

Optional: separate candidate scope vs required stability props for prod12 gate:

```bash
make mlb-candidate-eval-prod12 \
  MLB_CANDIDATE_SOURCE_TABLE="reconcile_rows" \
  MLB_CANDIDATE_ROWS_CSV="tmp/mlb_base_vs_market_rows_anybook.csv" \
  MLB_PROD12_CANDIDATE_PROP_TYPES="$(MLB_PROD12_PROP_TYPES)" \
  MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed"
```

Default prod12 weekly tracking now reads reconcile rows:
- `MLB_PROD12_CANDIDATE_SOURCE_TABLE=reconcile_rows`
- `MLB_PROD12_CANDIDATE_ROWS_CSV=tmp/mlb_base_vs_market_rows_anybook.csv`

If candidate recommendation is `promote`, then publish:

```bash
make mlb-prod12-model-bundle-publish
```

Post-publish validation (same session):

```bash
bin/mlb_prod12_remote_trigger_weekly.sh
set -a; source backend/.env; set +a
bin/mlb_prod12_remote_status.sh 180 | jq '{status,running,exit_code,run_id,started_at,finished_at}'
make mlb-prod12-phase2-last-strict
make mlb-prod12-status-strict
```

Notes:
- do not auto-publish on every recompute; keep publish gated by candidate eval and strict weekly checks

## Local Scheduler (macOS launchd)

Use this when you want retrain/recompute cadence to run on your machine (not Render).

### Daily Local Capture Job (Refresh + Build)

This LaunchAgent runs the local daily chain end-to-end:
- roster refresh
- stat-derived refresh
- `mlb-predictions-wide`
- `mlb-slate-output`
- `mlb-book-upload` (forced local build; remote fetch flags are set to `0`)
- `mlb-prod12-track-daily` + `mlb-prod12-ops-log` (local daily history snapshots; best effort)

Create/update runner script:

```bash
mkdir -p "$HOME/bin" "$HOME/Projects/proppadia/artifacts/ops" "$HOME/Library/LaunchAgents"

cat > "$HOME/bin/proppadia_mlb_refresh_daily.sh" <<'EOF'
#!/bin/zsh
set -euo pipefail

REPO="$HOME/Projects/proppadia"
cd "$REPO"

set -a
source backend/.env
set +a

MLB_DATE_ET="$(TZ=America/New_York date +%F)"
MLB_LOCAL_DAILY_TRACKING_ENABLED="${MLB_LOCAL_DAILY_TRACKING_ENABLED:-1}"

echo "[$(date -u +%FT%TZ)] START local daily MLB refresh+capture (MLB_DATE_ET=${MLB_DATE_ET})"

MLB_ROSTER_DATE="$MLB_DATE_ET" \
make mlb-roster-refresh-all

MLB_STAT_DAYS_AGO=2 \
MLB_STAT_SKIP_EXISTING_DATES=1 \
MLB_STAT_DERIVED_DAYS=7 \
MLB_STAT_DERIVED_MIN=0 \
MLB_SEASON_REQUIRE_REGULAR=1 \
make mlb-stat-derived-refresh

make mlb-predictions-wide MLB_DATE="$MLB_DATE_ET"
make mlb-slate-output MLB_DATE="$MLB_DATE_ET"

MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=0 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED=0 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_ONLY=0 \
make mlb-book-upload MLB_DATE="$MLB_DATE_ET"

# 3) Append local prod12 daily history snapshots (best effort).
if [[ "${MLB_LOCAL_DAILY_TRACKING_ENABLED}" == "1" ]]; then
  set +e
  MLB_DATE="$MLB_DATE_ET" make mlb-prod12-track-daily
  track_rc=$?
  make mlb-prod12-ops-log
  ops_rc=$?
  set -e

  if [[ "$track_rc" -ne 0 ]]; then
    echo "[$(date -u +%FT%TZ)] WARN mlb-prod12-track-daily failed rc=${track_rc}" >&2
  fi
  if [[ "$ops_rc" -ne 0 ]]; then
    echo "[$(date -u +%FT%TZ)] WARN mlb-prod12-ops-log failed rc=${ops_rc}" >&2
  fi
else
  echo "[$(date -u +%FT%TZ)] INFO local prod12 history tracking disabled (MLB_LOCAL_DAILY_TRACKING_ENABLED=${MLB_LOCAL_DAILY_TRACKING_ENABLED})"
fi

echo "[$(date -u +%FT%TZ)] DONE local daily MLB refresh+capture (MLB_DATE_ET=${MLB_DATE_ET})"
EOF

chmod +x "$HOME/bin/proppadia_mlb_refresh_daily.sh"
```

History outputs written locally:
- `artifacts/mlb_pipeline_history.jsonl`
- `artifacts/mlb_prod12_ops_history.jsonl`

Create daily LaunchAgent (example: 5:20 AM local):

```bash
cat > "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.proppadia.mlb.refresh.daily</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HOME/bin/proppadia_mlb_refresh_daily.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$HOME/Projects/proppadia</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>5</integer>
    <key>Minute</key><integer>20</integer>
  </dict>
  <key>StandardOutPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.out.log</string>
  <key>StandardErrorPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.err.log</string>
  <key>RunAtLoad</key><false/>
</dict>
</plist>
EOF

touch "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.out.log"
touch "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.err.log"
plutil -lint "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist"
```

Load/reload:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist" 2>/dev/null || true
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist"
```

Trigger once to validate:

```bash
launchctl kickstart gui/$(id -u)/com.proppadia.mlb.refresh.daily
tail -n 120 "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.out.log"
tail -n 120 "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.err.log"
```

Check state:

```bash
launchctl print gui/$(id -u)/com.proppadia.mlb.refresh.daily | rg "state = |runs = |last exit code"
```

### Weekly Local Retrain Job

1. Create a local runner script:

```bash
mkdir -p "$HOME/bin" "$HOME/Projects/proppadia/artifacts/ops" "$HOME/Library/LaunchAgents"

cat > "$HOME/bin/proppadia_mlb_retrain_weekly.sh" <<'EOF'
#!/bin/zsh
set -euo pipefail
cd "$HOME/Projects/proppadia"

set -a
source backend/.env
set +a

echo "[$(date -u +%FT%TZ)] START weekly retrain cadence"
make mlb-retrain-prereq-check
make mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="2025-03-01" MLB_RECONCILE_TO_DATE="$(date -u +%F)" MLB_RECONCILE_BOOKMAKER= MLB_RECONCILE_ODDS_FILENAME="odds_latest_compatible.json" MLB_RECONCILE_ROWS_OUT_CSV="tmp/mlb_base_vs_market_rows_anybook.csv"
make mlb-retrain-broad-reconcile MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows_anybook.csv" MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0 MLB_RETRAIN_QUALITY_MIN_TOTAL=600 MLB_CANDIDATE_MIN_TOTAL=1000 MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed" MLB_PROD12_MAX_PROP_DROP_PCT=12
make mlb-prod12-model-bundle-publish
echo "[$(date -u +%FT%TZ)] DONE weekly retrain cadence"
EOF

chmod +x "$HOME/bin/proppadia_mlb_retrain_weekly.sh"

# Optional quick script sanity check (runs once immediately in current shell):
# "$HOME/bin/proppadia_mlb_retrain_weekly.sh"
```

Notes:
- Because the script runs with `set -e`, publish only runs if prior retrain/eval steps pass.
- Ensure publish credentials are present in `backend/.env` (`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` or `SUPABASE_SECRET_KEY`).
- `MLB_RETRAIN_QUALITY_MIN_TOTAL=600` avoids early-season false-fail on low resolved row counts; raise it back toward `1000` once coverage is consistently higher.
- Early-season candidate gate override keeps weekly cadence moving with current reconcile coverage:
  - `MLB_CANDIDATE_MIN_TOTAL=1000`
  - `MLB_PROD12_CANDIDATE_REQUIRED_PROPS` excludes `strikeouts_batting` until that market resumes in reconcile rows.
  - `MLB_PROD12_MAX_PROP_DROP_PCT=12` (to avoid false holds on low-volume lanes like `walks`).

2. Create a LaunchAgent plist (example: Monday 6:30 AM local time):

```bash
cat > "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.proppadia.mlb.retrain.weekly</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HOME/bin/proppadia_mlb_retrain_weekly.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$HOME/Projects/proppadia</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Weekday</key><integer>1</integer>
    <key>Hour</key><integer>6</integer>
    <key>Minute</key><integer>30</integer>
  </dict>
  <key>StandardOutPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.out.log</string>
  <key>StandardErrorPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.err.log</string>
  <key>RunAtLoad</key><false/>
</dict>
</plist>
EOF

touch "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.out.log"
touch "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.err.log"
plutil -lint "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
```

3. Load or reload the job:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist" 2>/dev/null || true
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
```

4. Trigger once now to verify (without killing a running job):

```bash
launchctl kickstart gui/$(id -u)/com.proppadia.mlb.retrain.weekly
tail -n 80 "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.out.log"
tail -n 80 "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.err.log"
```

Important:
- `launchctl kickstart -k ...` force-restarts the job and sends `SIGTERM` to the current process.
- If the weekly run is mid-step (for example `make mlb-reconcile-rows`), logs will show `Terminated: 15`.

5. Check status anytime:

```bash
launchctl print gui/$(id -u)/com.proppadia.mlb.retrain.weekly | head -n 80
```

6. Disable/remove later if needed:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
rm -f "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
```

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

## OddsAPI External Archive (Keep Full History)

If you want to keep all OddsAPI snapshots without growing local disk usage, offload `backend/mlb/exports/odds_history` to an external drive and then prune only local copies that are confirmed archived.

Set your archive root (example using mounted drive `ACASIS 1`):

```bash
export MLB_ODDS_HISTORY_ARCHIVE_ROOT="/Volumes/ACASIS 1/OddsAPI/mlb"
```

Audit local vs archive:

```bash
make mlb-odds-history-offload-status \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT"
```

Sync local odds history to external archive:

```bash
make mlb-odds-history-offload-sync \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT"
```

Prune local only for dates older than retention when archive copy exists (safe mode):

```bash
make mlb-odds-history-offload-prune-local \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT" \
  MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS=180
```

One-command cycle:

```bash
make mlb-odds-history-offload-cycle \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT" \
  MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS=180
```

Optional pre-prune local compaction (removes raw intermediates where `odds_latest_compatible.json` already exists):

```bash
make mlb-odds-history-prune-intermediate
```
