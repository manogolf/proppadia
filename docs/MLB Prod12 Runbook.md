# MLB Prod12 Runbook

This runbook defines the operating plan for the active MLB prediction lane with 12 props.

## Active Lane (Prod12)

`hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis`

Watchlist-only props (not in active lane): `outs_recorded,home_runs`

## Daily Loop (Required)

Run once per day:

```bash
make mlb-pipeline-check-prod12 \
  MLB_BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=YYYY-MM-DD \
  MLB_PREDICT_SAMPLE=10 \
  MLB_PREDICT_MIN_SUCCESS=3 \
  MLB_QUALITY_GAMES_BACK=30 \
  MLB_QUALITY_MIN_TOTAL=1000 \
  MLB_QUALITY_MIN_ACCURACY=48 \
  MLB_PROP_COVERAGE_GAMES_BACK=30 \
  MLB_CORE_MIN_GRADED=20
```

Then append history:

```bash
make mlb-pipeline-log \
  MLB_BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=YYYY-MM-DD \
  MLB_PREDICT_SAMPLE=10 \
  MLB_PREDICT_MIN_SUCCESS=3 \
  MLB_PREDICT_PROP_TYPES="$(MLB_PROD12_PROP_TYPES)" \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  MLB_QUALITY_MIN_TOTAL=1000 \
  MLB_QUALITY_MIN_ACCURACY=48 \
  MLB_QUALITY_PROP_SOURCES=mlb_api \
  MLB_PROP_COVERAGE_WINDOW_MODE=games \
  MLB_PROP_COVERAGE_GAMES_BACK=30 \
  MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROD12_PROP_TYPES)" \
  MLB_PROP_COVERAGE_MIN_GRADED=20
```

Run balance guard:

```bash
make mlb-balance-guard \
  MLB_BALANCE_GUARD_PROP_TYPE=runs_scored \
  MLB_BALANCE_GUARD_GAMES_BACK=30 \
  MLB_BALANCE_GUARD_MIN_TOTAL=1000 \
  MLB_BALANCE_GUARD_MIN_ACCURACY=48 \
  MLB_BALANCE_GUARD_MIN_OVER_PCT=10
```

## Weekly Retrain Loop (Required)

Run once per week:

```bash
for p in hits total_bases strikeouts_batting earned_runs doubles hits_allowed strikeouts_pitching walks hits_runs_rbis runs_scored walks_allowed runs_rbis; do
  .venv/bin/python backend/mlb/model_trainer.py --prop "$p" --days-back 1095 --limit 150000
done
```

Then re-run:

```bash
make mlb-prediction-quality-prod12 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000
make mlb-pipeline-check-prod12 MLB_BASE_URL=https://baseball-streaks-sq44.onrender.com MLB_DATE=YYYY-MM-DD MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_CORE_MIN_GRADED=20
```

## Model Artifact Policy

- Primary runtime location: Render persistent disk (`/var/data/proppadia/models`).
- Backup location: Supabase storage.
- Keep versioned releases under `/var/data/proppadia/models/releases/<timestamp>/latest`.
- Activate by symlink swap to `/var/data/proppadia/models/latest`.
- Keep previous release for immediate rollback.

## Readiness Gates

- `mlb-pipeline-check-prod12` status must be `pass`.
- `degraded_prop_lanes` must be empty.
- `mlb-balance-guard` must pass for `runs_scored`.
- Latest pipeline history must contain a same-day pass entry.

## Incident Rules

- If daily loop is missed: treat as incident and run catch-up immediately.
- If weekly retrain is missed: treat as incident and run full retrain within 24 hours.
- If any gate fails: hold rollout changes and remediate before next promotion.

## Phase 1 Replay/Reconcile Retention

Daily slate artifacts are now archived under:

- `backend/mlb/exports/odds_history/YYYY-MM-DD/`

When running the slate build path:

```bash
make mlb-predictions-wide MLB_DATE=YYYY-MM-DD
make mlb-slate-output MLB_DATE=YYYY-MM-DD
make mlb-book-upload MLB_DATE=YYYY-MM-DD
```

`mlb-book-upload` now auto-runs `mlb-slate-archive`, preserving:

- `mlb_predictions_wide_calibrated.csv`
- `mlb_slate_output.csv`
- `mlb_book_upload.csv`
- `odds_mlb_playerprops.json` (exact OddsAPI snapshot used by predictions-wide)

Prod12 cron default behavior (to prevent retention gaps):

- `MLB_DAILY_WIDE_PREDICTIONS_ENABLED=1`
- `MLB_DAILY_WIDE_PREDICTIONS_REQUIRED=1`
- `MLB_DAILY_SLATE_ARTIFACTS_ENABLED=1`
- `MLB_DAILY_SLATE_ARTIFACTS_REQUIRED=1`

Daily cron now verifies `backend/mlb/exports/odds_history/YYYY-MM-DD/manifest.json`
exists after upload. If missing, the run fails (required mode).

Build row-level reconcile rows from archived slates:

```bash
make mlb-reconcile-rows \
  MLB_RECONCILE_FROM_DATE=YYYY-MM-DD \
  MLB_RECONCILE_TO_DATE=YYYY-MM-DD \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Outputs:

- `tmp/mlb_base_vs_market_rows.csv`
- `tmp/mlb_base_vs_market_summary.json`

Historical MLB regular-season odds backfill (recommended before lowering OddsAPI plan):

```bash
make mlb-odds-backfill-history \
  MLB_ODDS_BACKFILL_SEASON=2025 \
  MLB_ODDS_BACKFILL_TO_DATE=2025-09-28
```

Dry-run estimate first (no API spend):

```bash
make mlb-odds-backfill-history \
  MLB_ODDS_BACKFILL_SEASON=2025 \
  MLB_ODDS_BACKFILL_TO_DATE=2025-09-28 \
  MLB_ODDS_BACKFILL_DRY_RUN=1
```
