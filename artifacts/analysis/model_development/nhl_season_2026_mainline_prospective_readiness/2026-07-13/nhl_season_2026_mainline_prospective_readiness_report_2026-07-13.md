# NHL Season 2026 Mainline Prospective Readiness

## Decision

Season 2026 mainline observation is `READY_WITH_BOUNDED_LIMITS` for a separately authorized implementation in `SHADOW_OBSERVATION_ONLY`. Game identity is ready. Champion parameters and parity evidence are frozen, but a governed prospective scorer wrapper is not implemented. The Odds API transport can request `h2h`, but the current NHL path defaults to player props, overwrites latest files, and does not preserve an immutable mainline quote history.

Goalie roster identity exists without a certified projected/confirmed-starter source. Active roster plumbing exists, but injuries, scratches, line combinations, and immutable timing are not connected. These remain optional/Phase 2 and do not block the MVP.

## MVP

The MVP requires official game identity, six strict-prior champion inputs, frozen probability, timestamped two-sided full-game moneyline quotes, create-only run archives, health manifests, and append-only outcome grading. Runs are limited to `MIDDAY` and `FINAL_PREGAME`; every later state creates a new run.

Market normalization is proportional two-way de-vig. Raw prices, implied probabilities, overround, and normalized probabilities remain separate from champion probabilities. Execution is never inferred.

## Workflow and governance

Implementation should adapt the existing schedule importer and Odds API transport behind new mainline-only wrappers, then add feature preparation, frozen scoring, packaging, validation, and grading commands shown in the workflow map. No command is enabled here. Corrupt identity, model/hash mismatch, feature leakage, probability invalidity, post-start attachment, or overwrite attempts fail closed. Missing optional goalie/lineup context does not fail the slate.

The SOG lane remains independent at player-prop grain. Only transport, schedule identity, timestamps, and hashing conventions may be shared.

The single next bounded task is **NHL season 2026 mainline prospective shadow-capture implementation**. Wagering, recommendations, promotion, automated execution, unattended jobs, and frontend changes remain unauthorized.

## Required decisions

- `NHL_SEASON_2026_MAINLINE_GAME_IDENTITY_READINESS` = `READY`
- `NHL_SEASON_2026_MAINLINE_CHAMPION_REPLAY_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_PRICE_CAPTURE_READINESS` = `BLOCKED_BY_IMMUTABLE_H2H_CAPTURE_IMPLEMENTATION`
- `NHL_SEASON_2026_MAINLINE_PRICE_TIMESTAMP_READINESS` = `BLOCKED_BY_MAINLINE_TIMESTAMP_PERSISTENCE`
- `NHL_SEASON_2026_MAINLINE_GOALIE_CAPTURE_READINESS` = `BLOCKED_BY_PREGAME_GOALIE_SOURCE_INTEGRATION`
- `NHL_SEASON_2026_MAINLINE_LINEUP_CAPTURE_READINESS` = `BLOCKED_BY_TIMESTAMPED_LINEUP_INJURY_SOURCE`
- `NHL_SEASON_2026_MAINLINE_OUTCOME_GRADING_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_MVP_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_HEALTH_GATE_READINESS` = `READY`
- `NHL_SEASON_2026_MAINLINE_SHADOW_OBSERVATION_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_WAGER_RECOMMENDATION_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_OPERATIONAL_RESTART_READINESS` = `SHADOW_ONLY`
