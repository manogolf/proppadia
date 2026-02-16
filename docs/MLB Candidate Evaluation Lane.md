# MLB Candidate Evaluation Lane

Purpose: compare candidate model quality against locked baseline artifacts and emit one promotion recommendation payload.

Command:

```bash
make mlb-candidate-eval
```

What it does:

1. Loads baseline from `MLB_CANDIDATE_BASELINE_PATH` (or latest `artifacts/season_baselines/mlb_quality_*.json`).
2. Computes candidate quality from `model_training_props` using the same holdout window/profile.
3. Applies promotion rule checks:
   - candidate sample size meets minimum
   - overall accuracy lift meets minimum
   - required prop lanes do not degrade beyond allowed drop
4. Emits JSON with `recommendation=promote|hold`, `failures`, and per-check details.

Default thresholds:

- `MLB_CANDIDATE_MIN_TOTAL=-1` (uses baseline overall total)
- `MLB_CANDIDATE_MIN_LIFT_PCT=0.25`
- `MLB_CANDIDATE_MAX_PROP_DROP_PCT=0.5`
- `MLB_CANDIDATE_REQUIRED_PROPS=$(MLB_CORE_PROP_TYPES)`

Suggested stricter run:

```bash
make mlb-candidate-eval \
  MLB_CANDIDATE_BASELINE_PATH=artifacts/season_baselines/mlb_quality_games_30_120.json \
  MLB_CANDIDATE_PROP_TYPES=hits,total_bases,hits_runs_rbis,runs_rbis,rbis,runs_scored,strikeouts_batting,walks,singles,doubles,strikeouts_pitching,outs_recorded \
  MLB_CANDIDATE_REQUIRED_PROPS=hits,total_bases,hits_runs_rbis,runs_rbis,rbis,runs_scored,strikeouts_batting,walks,singles,doubles,strikeouts_pitching,outs_recorded \
  MLB_CANDIDATE_MIN_TOTAL=3000 \
  MLB_CANDIDATE_MIN_LIFT_PCT=0.50 \
  MLB_CANDIDATE_MAX_PROP_DROP_PCT=0.25
```
