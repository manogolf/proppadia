# NHL Prediction Quality Baseline

## Purpose

Provide a repeatable, fixed-window backtest summary for NHL prediction direction quality.

This is a read-only report against `public.player_props` rows where `prop_source` begins with `nhl_`.

## Command

```bash
make nhl-prediction-quality \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31 \
  NHL_QUALITY_MIN_TOTAL=1
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
