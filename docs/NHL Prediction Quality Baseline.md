# NHL Prediction Quality Baseline

## Purpose

Provide a repeatable, fixed-window backtest summary for NHL prediction direction quality.

This is a read-only report against `mlb.player_props` rows where `prop_source` begins with `nhl_`.

## Command

```bash
make nhl-prediction-quality-auto \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31 \
  NHL_QUALITY_ACTIVE_MIN_TOTAL=1
```

## Output

JSON payload containing:

- `overall`: graded total/correct/accuracy
- `by_prop`: breakdown by NHL prop type
- `by_source`: breakdown by `prop_source`
- `caveats`: sparse/offseason interpretation notes

## Notes

- Only graded outcomes (`win`/`loss`) count toward model accuracy.
- `push`/`dnp` are excluded from correctness denominator.
- Use explicit date windows for reproducibility between runs.
- Auto mode keeps offseason windows non-blocking (`effective min_total=0` when no graded NHL rows exist).

## SOG Segmented Calibration Baseline

Purpose:
- Capture a fixed-window, holdout-based baseline specifically for NHL SOG segmented calibration.

Command:

```bash
make nhl-sog-calibration-baseline \
  NHL_SOG_BASELINE_FROM_DATE=2025-10-01 \
  NHL_SOG_BASELINE_TO_DATE=2026-02-04
```

Default output:
- `artifacts/season_baselines/nhl_sog_segmented_calibration_baseline.json`

Contains:
- `holdout_by_method_line`
- `holdout_deltas_vs_raw`
- `counts` (train/holdout rows and dates)
- calibration fit metadata by line/segment

## SOG Calibration Monitor History

Purpose:
- Append a compact pass/fail snapshot to JSONL history using the same holdout experiment.

Command:

```bash
make nhl-sog-calibration-log
make nhl-sog-calibration-last NHL_SOG_MONITOR_HISTORY_LIMIT=5
make nhl-sog-calibration-history-clean NHL_SOG_MONITOR_HISTORY_CLEAN_BACKUP=1
```

Default history file:
- `artifacts/nhl_sog_calibration_history.jsonl`

Pass/fail default:
- each required line must have `delta_brier_vs_raw <= 0.0`
- and `delta_logloss_vs_raw <= 0.0` when logloss exists

History cleanup:
- `nhl-sog-calibration-history-clean` removes transient DNS/network failure entries from history.
- with `NHL_SOG_MONITOR_HISTORY_CLEAN_BACKUP=1`, a timestamped backup is written before rewrite.
