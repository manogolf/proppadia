# MLB Retrain Prerequisites Checklist

Purpose: gate MLB retrain decisions on one repeatable, operator-facing payload.

Command:

```bash
make mlb-retrain-prereq-check
```

The bundle validates four prerequisites:

1. Data freshness: recent `mlb_api` row volume in `model_training_props`.
2. Prop coverage: required prop lanes meet minimum `training_source_count`.
3. Grading completeness: minimum graded sample size in `model_training_props`.
4. Baseline comparison availability: latest MLB baseline artifact exists (and optional age check).

Default profile (from `Makefile`):

- `MLB_RETRAIN_FRESHNESS_DAYS=7`
- `MLB_RETRAIN_FRESHNESS_MIN_ROWS=1`
- `MLB_RETRAIN_COVERAGE_WINDOW_MODE=games`
- `MLB_RETRAIN_COVERAGE_GAMES_BACK=30`
- `MLB_RETRAIN_REQUIRED_PROPS=$(MLB_CORE_PROP_TYPES)`
- `MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP=$(MLB_CORE_MIN_GRADED)`
- `MLB_RETRAIN_TRAINING_PROP_SOURCES=mlb_api,user_added`
- `MLB_RETRAIN_GRADING_WINDOW_MODE=games`
- `MLB_RETRAIN_GRADING_GAMES_BACK=30`
- `MLB_RETRAIN_GRADING_PROP_TYPES=$(MLB_CORE_PROP_TYPES)`
- `MLB_RETRAIN_GRADING_MIN_TOTAL=1000`
- `MLB_RETRAIN_BASELINE_MAX_AGE_HOURS=0` (disabled unless set > 0)

Recommended stricter run before retrain:

```bash
make mlb-retrain-prereq-check \
  MLB_RETRAIN_FRESHNESS_DAYS=14 \
  MLB_RETRAIN_FRESHNESS_MIN_ROWS=500 \
  MLB_RETRAIN_COVERAGE_GAMES_BACK=60 \
  MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP=50 \
  MLB_RETRAIN_GRADING_GAMES_BACK=60 \
  MLB_RETRAIN_GRADING_MIN_TOTAL=3000 \
  MLB_RETRAIN_BASELINE_MAX_AGE_HOURS=168
```
