# NHL Season 2026 Preseason Operational Checklist

Mode: `PLUMBING_VALIDATION_ONLY`; every probability: `PRESEASON_NON_EVALUATION`.

## Before run

- [ ] Official schedule import succeeds and raw response is immutable.
- [ ] `canonical_season=2026`, `game_type=PRESEASON`, game ID, teams, and start are retained.
- [ ] Champion parameter and 2,798-row parity hashes pass.
- [ ] H2H endpoint responds; an empty-book response is handled explicitly.
- [ ] Create-only run path does not exist.

## MIDDAY

- [ ] Strict-prior builder and frozen scoring execute; opening-state labels are retained.
- [ ] Raw odds, provider source timestamps, capture timestamp, and Population A/B counts are preserved.
- [ ] Health ledger passes or fails closed.

## FINAL_PREGAME

- [ ] Same games bind to a distinct run ID; new quotes create a new snapshot.
- [ ] MIDDAY hashes remain unchanged; missing books are allowed.
- [ ] Post-start quotes/context are retained diagnostically but rejected from pregame qualification.

## Postgame

- [ ] Official outcome is appended in a separate grade tree.
- [ ] Both pregame manifests remain unchanged.
- [ ] No preseason row enters champion performance/calibration/ROI evaluation.
