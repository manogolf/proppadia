# NHL Season 2026 Game-Type Propagation and Preseason Isolation

## Result

Official `gameType` now propagates as `game_type_code` and `game_type_label` through the mainline game spine, strict-prior features, frozen predictions, provider binding, H2H quotes, market comparisons, typed population membership, run metadata, health gates, and immutable grading. The mapping is `1=PRESEASON`, `2=REGULAR_SEASON`, and `3=POSTSEASON`; unsupported or absent values remain `UNKNOWN_GAME_TYPE` and fail closed from scoring/evaluation qualification. Game type is never inferred from date.

Preseason games may be scored and may retain valid H2H observations solely as `PRESEASON_NON_EVALUATION` and `PRESEASON_PLUMBING_MARKET_OBSERVATION`. The reusable regular-season eligibility predicate hard-filters `game_type_code == 2`; explicit exclusions cover preseason, postseason, and unknown types. Grading preserves the pregame evaluation status, so a known preseason result remains non-evaluation.

Regular-season feature targets use completed type-2 history only. A deterministic first-regular-game fixture with an earlier 5-1, 40-20 preseason result retained zero prior games and `SEASON_OPEN_NO_HISTORY`; preseason goals, shots, form, prior-10, and rest therefore cannot contaminate the frozen regular-season state. This matches the frozen historical population, which contained no preseason rows, without changing coefficients or probabilities.

The exact 2,798-row champion parity replay passed with maximum probability delta `1.17e-15` and zero side mismatches. SOG sources, paths, candidate logic, prop normalization, and archives were untouched. No live run was performed.

The system is `READY_WITH_BOUNDED_LIMITS` for exactly one later human-initiated real preseason shadow validation when official games and H2H markets exist.

## Decisions

- `NHL_SEASON_2026_GAME_TYPE_SOURCE_MAPPING` = `READY`
- `NHL_SEASON_2026_GAME_TYPE_PROPAGATION` = `READY`
- `NHL_SEASON_2026_PRESEASON_SCORING_PATH` = `READY`
- `NHL_SEASON_2026_PRESEASON_EVALUATION_ISOLATION` = `READY`
- `NHL_SEASON_2026_REGULAR_SEASON_HISTORY_ISOLATION` = `READY`
- `NHL_SEASON_2026_UNKNOWN_GAME_TYPE_FAIL_CLOSED` = `READY`
- `NHL_SEASON_2026_GAME_TYPE_GRADING_PROPAGATION` = `READY`
- `NHL_SEASON_2026_GAME_TYPE_HEALTH_GATES` = `READY`
- `NHL_SEASON_2026_CHAMPION_PARITY_AFTER_GAME_TYPE_CHANGE` = `READY`
- `NHL_SEASON_2026_SOG_ISOLATION_AFTER_GAME_TYPE_CHANGE` = `READY`
- `NHL_SEASON_2026_FIRST_PRESEASON_SHADOW_RUN_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_REGULAR_SEASON_OPENING_READINESS` = `BLOCKED_BY_REAL_PRESEASON_VALIDATION`
- `NHL_SEASON_2026_WAGER_RECOMMENDATION_READINESS` = `NOT_READY`
