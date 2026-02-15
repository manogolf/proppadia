# MLB Prediction Flow Audit

## Scope

End-to-end MLB prediction write path:

1. `POST /api/prepareProp`
2. `POST /api/predict`
3. `POST /api/props/add`
4. grade/reconcile writes reflected in `player_props.outcome` / `player_props.status`

## Source Of Truth

- Prediction/write table: `public.player_props`
- Training/stat-derived table: `public.model_training_props`
- Core flow keys: `player_id`, `game_id`, `prop_type`
- User scoping key (when present): `user_id`

## Joins Used For Audit

- Prediction identity join:
  - `player_props(player_id, game_id, prop_type)` as canonical write identity
- Training identity join:
  - `model_training_props(player_id, game_id, prop_type, prop_source='stat_derived')`

## What The Audit Checks

Command:

```bash
make mlb-prediction-flow-audit
```

Underlying script:

```bash
.venv/bin/python backend/scripts/audit_mlb_prediction_flow.py
```

Checks:

- User-added rows missing/invalid `game_id`
- User-added rows missing/invalid `game_date`
- User-added rows with suspicious `created_at` vs `game_date` drift (default `>1` day)
- Resolved rows with invalid `outcome` state
- Duplicate-key groups in:
  - `player_props` (user-added identity)
  - `model_training_props` (`stat_derived` identity)

## Late-Season Failure Mode Coverage

Added guards to block stale/misaligned commit payloads in `add_prop_from_commit`:

- `game_date` is required
- `game_date` must be `YYYY-MM-DD`
- if `for_date` is present, it must match `game_date`

Repro tests added in `backend/tests/test_mlb_prop_workflow.py`:

- `test_add_prop_requires_game_date`
- `test_add_prop_rejects_bad_game_date`
- `test_add_prop_rejects_game_date_context_mismatch`

Plus script-level audit tests:

- `backend/tests/test_shared_mlb_prediction_flow_audit.py`
