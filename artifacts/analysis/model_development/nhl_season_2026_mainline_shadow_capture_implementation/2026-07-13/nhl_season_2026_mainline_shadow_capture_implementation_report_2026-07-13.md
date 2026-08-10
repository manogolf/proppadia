# NHL Season 2026 Mainline Shadow-Capture Implementation

## Result

Manual shadow capture is `READY_WITH_BOUNDED_LIMITS`. The frozen parameter-only scorer replayed all 2,798 champion rows with maximum probability delta `1.17e-15` and zero side mismatches. No fit or recalibration exists in the implementation.

A file-based strict-prior builder, explicit-only `h2h` fetch command, book-level quote qualifier/de-vig normalizer, create-only run archive, health ledger, Population A/B separation, and append-only grading hook are implemented under `backend/nhl/mainline_shadow`. The shared SOG CLI and prop archives were not modified.

Because no suitable season 2026 regular-season slate exists on 2026-08-09, the end-to-end validation used a clearly labeled season 2023 historical orchestration fixture. It used preserved game identities/outcomes and synthetic shot counts solely to exercise feature chronology. Both fixture games entered Populations A and B; C, D, and E remained empty. The run passed, reproduced byte-for-byte in isolation, blocked an overwrite, retained all quote-status diagnostics, and graded into a separate immutable tree. This is not a prospective observation result.

## Manual operation

A human first captures raw `h2h` with `python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output <create-only-raw-path>`, then invokes the run command documented in the final handoff. No scheduler or job was enabled.

Exactly one later task is unlocked: one actual season 2026 prospective mainline shadow observation when a real slate is available. `SHADOW_OBSERVATION_ONLY`; no wagering, recommendations, execution, promotion, or automation.

## Required decisions

- `NHL_SEASON_2026_MAINLINE_CHAMPION_SCORER_IMPLEMENTED` = `READY`
- `NHL_SEASON_2026_MAINLINE_CHAMPION_HISTORICAL_PARITY` = `READY`
- `NHL_SEASON_2026_MAINLINE_STRICT_PRIOR_FEATURE_BUILDER_IMPLEMENTED` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_H2H_CAPTURE_IMPLEMENTED` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_PRICE_TIMESTAMP_PERSISTENCE_IMPLEMENTED` = `READY`
- `NHL_SEASON_2026_MAINLINE_IMMUTABLE_RUN_ARCHIVE_IMPLEMENTED` = `READY`
- `NHL_SEASON_2026_MAINLINE_HEALTH_GATES_IMPLEMENTED` = `READY`
- `NHL_SEASON_2026_MAINLINE_OUTCOME_GRADING_HOOK_IMPLEMENTED` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_SOG_ISOLATION_VERIFIED` = `READY`
- `NHL_SEASON_2026_MAINLINE_MANUAL_SHADOW_RUN_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_MAINLINE_AUTOMATED_SHADOW_RUN_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_WAGER_RECOMMENDATION_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_OPERATIONAL_RESTART_READINESS` = `MANUAL_SHADOW_ONLY`
