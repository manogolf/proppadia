# NHL Season 2026 Mainline Shadow-Capture Implementation

## Result

Manual shadow capture is `READY_WITH_BOUNDED_LIMITS`. The frozen parameter-only scorer replayed all 2,798 champion rows with maximum probability delta `1.17e-15` and zero side mismatches. No fit or recalibration exists in the implementation.

A file-based strict-prior builder, explicit-only `h2h` fetch command, book-level quote qualifier/de-vig normalizer, create-only run archive, health ledger, Population A/B separation, and append-only grading hook are implemented under `backend/nhl/mainline_shadow`. The shared SOG CLI and prop archives were not modified.

Because no suitable season 2026 regular-season slate exists on 2026-08-09, the end-to-end validation used a clearly labeled season 2023 historical orchestration fixture. It used preserved game identities/outcomes and synthetic shot counts solely to exercise feature chronology. Both fixture games entered Populations A and B; C, D, and E remained empty. The run passed, reproduced byte-for-byte in isolation, blocked an overwrite, retained all quote-status diagnostics, and graded into a separate immutable tree. This is not a prospective observation result.

## Manual operation

A human first captures raw `h2h` with `python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output <create-only-raw-path>`, then invokes the run command documented in the final handoff. No scheduler or job was enabled.

Exactly one later task is unlocked: one actual season 2026 prospective mainline shadow observation when a real slate is available. `SHADOW_OBSERVATION_ONLY`; no wagering, recommendations, execution, promotion, or automation.
