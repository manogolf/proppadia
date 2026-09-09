# NHL season 2026 four-lane day-one runbook

## Boundaries

- Moneyline: authorized preseason prediction/market shadow.
- SOG: authorized scope with an explicit hash-valid effective policy; use `--no-upload-shaped-output`.
- Points: P/M shadow only. Materially incoherent ladders remain in P and cannot enter M; C/U/E are disabled.
- Goalie Saves: P/M and append-only shadow grading only. Every probability is `P(goalie saves exceed proposition line | named goalie starts)`, with `start_prob=1.0` as a constant conditional flag—not an estimated start probability. Policy C requires the unique strictly top-supported goalie per game-team with at least two distinct sportsbooks. Label only `MARKET_LISTED_STARTER_UNCONFIRMED`; never projected/probable/confirmed. C/U/E are unauthorized.
- `SHADOW_PREDICTION_EXPORT` is diagnostic output, never a candidate upload or execution.

## 07:30 morning boundary

The unchanged LaunchAgent runs `.venv/bin/python backend/nhl/scripts/run_nhl_morning_orchestration.py --slate-date today --env-file backend/.env`. Require a manifest-complete canonical spine and all four prerequisite readiness flags. It performs no market capture or scoring. A manifest-complete `VALID_EMPTY_SLATE` is a successful stop: run no lane capture or scoring.

## Common variables

```sh
export SLATE_DATE=YYYY-MM-DD RUN_TS=YYYY-MM-DDTHH:MM:SSZ
export GAME_SPINE=/absolute/run-bound/canonical_game_spine.csv TEAM_HISTORY=/absolute/run-bound/team_history.csv
export MORNING_MANIFEST=/absolute/run-bound/SHA256SUMS RAW_ROOT=/absolute/create-only/raw
export MARKET_ROOT=/absolute/create-only/markets SHADOW_ROOT=/absolute/create-only/shadow
export SOG_PLAYERS=/absolute/run-bound/sog_player_spine.csv SOG_INPUTS=/absolute/run-bound/sog_prediction_inputs.csv
export POINTS_SNAPSHOT=/absolute/run-bound/points_snapshot SAVES_SNAPSHOT=/absolute/run-bound/saves_snapshot
```

## Exact MIDDAY commands

```sh
.venv/bin/python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/moneyline_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.mainline_shadow.cli run --schedule-csv "$GAME_SPINE" --history-csv "$TEAM_HISTORY" --odds-json "$RAW_ROOT/moneyline_${RUN_TS}.json" --output-root "$SHADOW_ROOT/moneyline" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.sog_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/sog_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.sog_quote_capture.cli run --payload-json "$RAW_ROOT/sog_${RUN_TS}.json" --games-csv "$GAME_SPINE" --players-csv "$SOG_PLAYERS" --output-root "$MARKET_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.sog_shadow.cli build-policy-config --policy-json /absolute/authorized/frozen_sog_walkforward_policy.json --output "$RAW_ROOT/sog_policy_${RUN_TS}.json"
.venv/bin/python -m backend.nhl.sog_shadow.cli run --game-spine-csv "$GAME_SPINE" --player-inputs-csv "$SOG_INPUTS" --quote-run-dir /absolute/immutable/sog_quote_run --effective-policy-json "$RAW_ROOT/sog_policy_${RUN_TS}.json" --parity-json artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json --output-root "$SHADOW_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY --no-upload-shaped-output
.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/points_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json "$RAW_ROOT/points_${RUN_TS}.json" --games-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --players-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --parent-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --player-inputs-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --player-inputs-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/points_quote_run --output-root "$SHADOW_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.saves_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/saves_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.saves_quote_capture.cli archive --payload-json "$RAW_ROOT/saves_${RUN_TS}.json" --games-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --goalies-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --parent-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.saves_shadow.cli run --game-spine-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --goalie-inputs-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --goalie-inputs-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/saves_quote_run --output-root "$SHADOW_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase MIDDAY --slate-date "$SLATE_DATE" --run-id /exact/four_lane_midday_run_id --input-json /absolute/run-bound/midday_sentinel_input.json
```

## Exact FINAL_PREGAME commands

Set a new `RUN_TS` and use new create-only paths. Never reuse MIDDAY identities.

```sh
.venv/bin/python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/moneyline_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.mainline_shadow.cli run --schedule-csv "$GAME_SPINE" --history-csv "$TEAM_HISTORY" --odds-json "$RAW_ROOT/moneyline_${RUN_TS}.json" --output-root "$SHADOW_ROOT/moneyline" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.sog_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/sog_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.sog_quote_capture.cli run --payload-json "$RAW_ROOT/sog_${RUN_TS}.json" --games-csv "$GAME_SPINE" --players-csv "$SOG_PLAYERS" --output-root "$MARKET_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.sog_shadow.cli build-policy-config --policy-json /absolute/authorized/frozen_sog_walkforward_policy.json --output "$RAW_ROOT/sog_policy_${RUN_TS}.json"
.venv/bin/python -m backend.nhl.sog_shadow.cli run --game-spine-csv "$GAME_SPINE" --player-inputs-csv "$SOG_INPUTS" --quote-run-dir /absolute/immutable/sog_quote_run --effective-policy-json "$RAW_ROOT/sog_policy_${RUN_TS}.json" --parity-json artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json --output-root "$SHADOW_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME --no-upload-shaped-output
.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/points_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json "$RAW_ROOT/points_${RUN_TS}.json" --games-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --players-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --parent-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --player-inputs-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --player-inputs-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/points_quote_run --output-root "$SHADOW_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.saves_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/saves_${RUN_TS}.json" --regions us,us2
.venv/bin/python -m backend.nhl.saves_quote_capture.cli archive --payload-json "$RAW_ROOT/saves_${RUN_TS}.json" --games-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --goalies-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --parent-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.saves_shadow.cli run --game-spine-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --goalie-inputs-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --goalie-inputs-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/saves_quote_run --output-root "$SHADOW_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase FINAL_PREGAME --slate-date "$SLATE_DATE" --run-id /exact/four_lane_final_run_id --input-json /absolute/run-bound/final_sentinel_input.json
```

Confirm every qualifying provider/source/capture timestamp precedes scheduled start.

## Sentinel, recovery, and grading

`GREEN` has no warnings; `YELLOW` requires review but bounded/no market coverage may still be a healthy P run; `RED` blocks downstream work. Saves additionally blocks parent/population drift, pre-score filtering, non-1 start flags, actual-starter leakage, ambiguous/multibook/timing violations, nonstarter misgrading, mutable inputs, and wrong season/slate/type.

Incomplete runs are forensic evidence. Never fill, delete, or rename over them. Fix the cause, use a new timestamp/run identity, and create fresh paths. Grade only after official completion into separate create-only trees. Actual goalie starter/participation is grading-only; nonstarters never enter the conditional evaluation denominator. Preseason gets no regular-season numeric target and feeds no regular-season history.
