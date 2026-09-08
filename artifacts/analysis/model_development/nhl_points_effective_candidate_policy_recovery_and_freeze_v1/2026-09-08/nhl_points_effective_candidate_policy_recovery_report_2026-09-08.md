# NHL Points effective candidate-policy recovery and freeze V1

Task: `NHL_POINTS_EFFECTIVE_CANDIDATE_POLICY_RECOVERY_AND_FREEZE_V1`  
Assessment date: `2026-09-08`  
Repository HEAD: `323ffd803add79dac823e9e27414ab2442433bc6`

## Final determination

`NHL_POINTS_EFFECTIVE_POLICY_DECISION = NOT_RECOVERABLE_SHADOW_ONLY`

No genuinely authoritative Points candidate/upload policy survives. The authoritative live Points chain ended at a mutable prediction-plus-market display CSV. Candidate and upload tooling, commands, runbooks, configs, and archived outputs are SOG-only. The current React Points max-edge cards and old static Top-20 CSV export are operator-visible research surfaces, but they materially conflict and neither records candidate, upload, or execution lineage.

The mandatory ladder gate remains rule 10. The existing shadow path remains fail-closed at rule 20 with `RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`. No policy was frozen, hash-pinned, or integrated; no production code or workflow changed.

## Historical evidence and replay

- P: 59,133 archived line rows / 19,711 complete player-game ladders across 48 canonical files (2026-02-27 through 2026-04-16, missing 2026-04-10); duplicate line identities: 0.
- Mandatory gate: 13,836 material/unevaluable ladders excluded before market (41,508 line rows); 5,875 ladders / 17,625 lines remain eligible.
- Legacy M proxy: 642 market-attached line rows / 343 player-games. This is not immutable/book-timestamp-certified M.
- Governed C/U/E/G: 0/0/0/0 recoverable rows. Prediction outcomes exist separately but cannot establish candidate or execution history.
- React research replay, historically applicable 41 dates: 410 displayed rows without the new gate versus 260 with the gate.
- Static research export replay, 48 dates: 960 rows without the gate versus 955 with the gate; 721 gated rows are unpriced.
- Candidate/upload parity: not testable; no archived targets exist.

## Recovered behavior versus policy authority

Directly recoverable display behavior includes Over-only Points outcomes, median American price across every encountered book, raw implied market probability, and the three model lines 0.5/1.5/2.5. None establishes sportsbook selection, no-vig normalization, candidate thresholds, support gates, deterministic identity-level ties, upload provenance, or execution state.

The React research surface uses -350/+500 price bounds, one maximum-gap line per player-game, edge then selected probability ordering, and a top-ten cap with no minimum positive edge. The older static surface uses one operator-selected line, a runtime minimum edge (default zero), EV ordering, and mutable Top N (default 20); it can retain unpriced rows. These are not interchangeable and cannot be merged into a recovered policy.

## Preseason disposition

Points should remain prediction/market shadow-only. Existing create-only MIDDAY and FINAL_PREGAME parent binding, book-level quote capture, `PRESEASON_NON_EVALUATION`, append-only manual/grading records, no post-start qualification, and no inference from upload to execution remain unchanged. C/U/E stay disabled.

Readiness:

- P: `READY_FOR_PRESEASON_BURN_IN`
- M: `READY_FOR_PRESEASON_BURN_IN_LIVE_COVERAGE_UNVERIFIED`
- C: `BLOCKED_RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`
- U: `BLOCKED_DISABLED_NO_RECOVERED_POLICY_OR_UPLOAD_SCHEMA`
- E: `BLOCKED_DISABLED_NEVER_INFERRED`
- G: `READY_FOR_APPEND_ONLY_SHADOW_OBSERVATION`; preseason rows remain `PRESEASON_NON_EVALUATION`

## Exact next task

`NHL_POINTS_FIRST_NONEMPTY_PRESEASON_DUAL_SNAPSHOT_BURN_IN_V1`

On the first real nonempty official season-2026 preseason slate, run one manual MIDDAY and one manual FINAL_PREGAME Points P/M observation, verify real provider/player binding and market disappearance/change evidence, and append one official-outcome grading observation. Keep C/U/E disabled. Do not alter the frozen scorer or coherence gate.
