# No action until authoritative postseason evidence exists

- [ ] Keep the V1 proposal and source manifest byte-identical at their current paths.
- [ ] Keep V1 as the only active authority; do not change a pin or process cache.
- [ ] Continue only already-authorized ordinary retention; do not add, replay, or widen a request.
- [ ] Do not infer a postseason date, round, or phase from the calendar or MLB expectations.
- [ ] Wait for immutable retained StatsAPI bytes containing a previously absent exact `gamePk` and raw authoritative `gameType`.
- [ ] Treat an empty valid scan as `NO_NEW_AUTHORITY_EVIDENCE`, not as an error and not as activation evidence.
- [ ] Treat missing, malformed, unknown, stale, duplicate, or conflicting evidence as a blocking failure.
- [ ] Do not build a candidate merely from provider events, prices, predictions, teams, matchups, or outcomes.
- [ ] Do not edit the one-time source-completion package or any completed phase-gating contract.
- [ ] Do not activate a candidate until the versioned loader/builder and close decoupling are implemented and reviewed.
- [ ] Do not let a failed newer configured snapshot fall back to V1.
- [ ] Do not extend agreement V4 beyond 2026-09-27 without separate acquisition authorization.
- [ ] After authorized activation, validate lanes only in their next naturally eligible ordinary windows.
- [ ] Do not manually rerun or duplicate acquisition to manufacture cross-lane operational proof.

