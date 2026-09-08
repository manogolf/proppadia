# NHL season 2026 three-lane day-one runbook

## Scope and hard boundaries

- Moneyline: frozen-control prediction/market shadow only; preseason non-evaluative.
- SOG: certified P/M/C lineage only, always pass `--no-upload-shaped-output`; execution is always zero.
- Points: P/M only. `C/U/E=0` under `RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`. Never pass a substitute policy.
- Goalie Saves: disabled and `NOT_READY_FOR_PRESEASON_BURN_IN`; run no Saves command.

## 07:30 local LaunchAgent

`com.proppadia.nhl.morning-orchestration` runs `.venv/bin/python backend/nhl/scripts/run_nhl_morning_orchestration.py --slate-date today --env-file backend/.env`. It performs database/environment preflight, official schedule acquisition, slate health, canonical spine export, stable daily history/roster prerequisites, and per-lane readiness. It does not capture markets, score, create candidates, upload, execute, or grade.

```sh
launchctl print gui/$(id -u)/com.proppadia.nhl.morning-orchestration | rg 'state =|runs =|last exit code'
tail -n 1 artifacts/operational/nhl/morning/launchagent.stdout.log
```

Required: `SUPABASE_DB_URL` or `DATABASE_URL` in `backend/.env`; `ODDS_API_KEY` for the later three market fetches; a manifest-complete canonical spine and Points input snapshot; the certified SOG parity JSON; and an explicit authorized SOG walk-forward policy JSON. Never use `*_latest`, `*_today`, or a mutable legacy odds archive for a critical decision.

## MIDDAY

Set create-only paths and RFC3339 UTC timestamps first:

```sh
export SLATE_DATE=YYYY-MM-DD RUN_TS=YYYY-MM-DDTHH:MM:SSZ
export GAME_SPINE=/absolute/run-bound/canonical_game_spine.csv
export TEAM_HISTORY=/absolute/run-bound/team_history.csv
export SOG_PLAYERS=/absolute/run-bound/sog_player_spine.csv
export SOG_INPUTS=/absolute/run-bound/sog_prediction_inputs.csv
export POINTS_FEATURES=/absolute/run-bound/points_legacy_feature_export.csv
export MORNING_MANIFEST=/absolute/run-bound/SHA256SUMS
export RAW_ROOT=/absolute/create-only/raw MARKET_ROOT=/absolute/create-only/markets SHADOW_ROOT=/absolute/create-only/shadow

.venv/bin/python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/moneyline_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.mainline_shadow.cli run --schedule-csv "$GAME_SPINE" --history-csv "$TEAM_HISTORY" --odds-json "$RAW_ROOT/moneyline_${RUN_TS}.json" --output-root "$SHADOW_ROOT/moneyline" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY

.venv/bin/python -m backend.nhl.sog_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/sog_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.sog_quote_capture.cli run --payload-json "$RAW_ROOT/sog_${RUN_TS}.json" --games-csv "$GAME_SPINE" --players-csv "$SOG_PLAYERS" --output-root "$MARKET_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.sog_shadow.cli build-policy-config --policy-json /absolute/authorized/frozen_sog_walkforward_policy.json --output "$RAW_ROOT/sog_effective_policy_${RUN_TS}.json"
.venv/bin/python -m backend.nhl.sog_shadow.cli run --game-spine-csv "$GAME_SPINE" --player-inputs-csv "$SOG_INPUTS" --quote-run-dir /absolute/immutable/sog_quote_run --effective-policy-json "$RAW_ROOT/sog_effective_policy_${RUN_TS}.json" --parity-json artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json --output-root "$SHADOW_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY --no-upload-shaped-output

.venv/bin/python backend/nhl/scripts/create_points_shadow_input_snapshot.py --slate-date "$SLATE_DATE" --game-spine-csv "$GAME_SPINE" --game-spine-manifest "$MORNING_MANIFEST" --features-csv "$POINTS_FEATURES" --output-root "$SHADOW_ROOT/points_inputs"
.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/points_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json "$RAW_ROOT/points_${RUN_TS}.json" --games-csv /absolute/points_snapshot/canonical_game_spine.csv --players-csv /absolute/points_snapshot/points_player_inputs.csv --parent-manifest /absolute/points_snapshot/SHA256SUMS --output-root "$MARKET_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv /absolute/points_snapshot/canonical_game_spine.csv --game-spine-manifest /absolute/points_snapshot/SHA256SUMS --player-inputs-csv /absolute/points_snapshot/points_player_inputs.csv --player-inputs-manifest /absolute/points_snapshot/SHA256SUMS --quote-run-dir /absolute/immutable/points_quote_run --output-root "$SHADOW_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase MIDDAY --slate-date "$SLATE_DATE" --run-id /exact/midday_run_id --input-json /absolute/run-bound/midday_sentinel_input.json
```

## FINAL_PREGAME

Repeat the fetch/capture/run commands with a new create-only `RUN_TS` and `--run-type FINAL_PREGAME`. Do not reuse MIDDAY raw paths, quote directories, run IDs, or effective-config output paths. Confirm every provider/source/capture timestamp is strictly before scheduled start. Then run the sentinel:

```sh
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase FINAL_PREGAME --slate-date "$SLATE_DATE" --run-id /exact/final_run_id --input-json /absolute/run-bound/final_sentinel_input.json
```

Expected success artifacts are `SHA256SUMS`, run metadata, raw/book-level quotes, timing/binding audits, predictions, market views, population ledgers, and lane health/sentinel files. Points additionally requires `RUN_COMPLETE.json`, ladder diagnostics, P=all frozen rows, M=eligible ladders with quotes, and C/U/E=0. SOG must archive the effective policy and matching hash and must have no upload-shaped file in this operating mode.

`GREEN` means no blockers or warnings. `YELLOW` means bounded coverage/evidence warnings; inspect every reason and proceed only within existing scope. `RED` prohibits candidate processing or further downstream action for stale/missing/hash-mismatched parents, partial slate, identity/orientation failure, post-start contamination, misgrading, wrong season/type, unsafe mutable input, or database/runtime failure. A manifest-complete valid empty slate is a successful stop with no scoring/capture.

An incomplete run lacks `SHA256SUMS` (and Points also lacks `RUN_COMPLETE.json` or remains under `.incomplete`). Never fill, delete, rename over, or rerun the same identity. Preserve it for diagnosis and retry with a new timestamp/run identity and fresh create-only paths after the cause is fixed.

Grading uses the lane `grade` CLI after official completion and writes a separate create-only tree:

```sh
export GRADE_TS=YYYY-MM-DDTHH:MM:SSZ GRADE_ROOT=/absolute/create-only/grades
.venv/bin/python -m backend.nhl.mainline_shadow.cli grade --run-dir /absolute/immutable/moneyline_final_run --outcomes-csv /absolute/certified/moneyline_outcomes.csv --grade-root "$GRADE_ROOT/moneyline" --grading-timestamp-utc "$GRADE_TS"
.venv/bin/python -m backend.nhl.sog_shadow.cli grade --run-dir /absolute/immutable/sog_final_run --outcomes-csv /absolute/certified/sog_outcomes.csv --grade-root "$GRADE_ROOT/sog" --grading-timestamp-utc "$GRADE_TS"
.venv/bin/python -m backend.nhl.points_shadow.cli grade --run-dir /absolute/immutable/points_final_run --outcomes-csv /absolute/certified/points_outcomes.csv --grade-root "$GRADE_ROOT/points" --grading-timestamp-utc "$GRADE_TS"
```

Preseason remains non-evaluative; nonparticipants remain ungraded. Upload-shaped data never implies execution, and this rehearsal emits no upload. Points has no upload/execution CLI. Goalie Saves stays disabled.
