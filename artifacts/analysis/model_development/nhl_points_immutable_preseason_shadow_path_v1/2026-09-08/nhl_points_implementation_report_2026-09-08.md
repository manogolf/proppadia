# NHL Points immutable preseason shadow path V1

Task: `NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1_WITH_EXPLICIT_LADDER_COHERENCE_GATE`

## Result

The Points lane is `READY_FOR_PRESEASON_BURN_IN_WITH_REDUCED_COVERAGE`. The new authoritative path supports create-only canonical input snapshots, independent MIDDAY and FINAL_PREGAME book-level quote archives, exact frozen scoring, raw ladder diagnostics, ladder-gated market qualification, append-only grading, and Points-specific live-failure evidence. It never writes the legacy latest/today files or production prediction table.

The containment gate is not a repair and establishes no predictive-quality claim. Every raw probability remains unchanged in P. A player-game with maximum raw adjacent crossing at least 0.01 is absent from M/C/U/E; an incomplete or identity-inconsistent ladder fails closed. Crossings below 0.01 remain in P and may enter M with an explicit warning.

## Frozen scorer

The untouched retained scorer and three independent line-specific logistic pipelines reproduced the retained 310-player / 930-prediction fixture byte-identically. Exact copies of that input and output are sealed under the new Points namespace, so legacy latest-file mutation cannot invalidate the control. The path pins scorer code, feature SQL, joblib, fitted scaler arrays, fitted coefficients/intercepts/classes, feature order, and pipeline configuration. No fit, update, calibration, sorting, clipping, averaging, or reconciliation is present.

The accepted diagnostic parity also reproduced 222/310 raw non-monotonic ladders and 213/310 material blocks. The fixture population included one coherent, one minor-warning, and one material ladder: P=9, M=6, C=0, U=0, E=0, G=0. The blocked ladder remained in P and diagnostics and did not enter M.

## Candidate boundary

The surviving frontend Points surface is a research ranking heuristic, not a governed candidate policy. Its code and certification-time hash are recorded, but it is not promoted to policy authority. The path therefore emits the ordered ladder and policy rule ledger and then records `RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`. Supplying an uncertified policy file is rejected. Candidates, upload artifacts, and executions remain empty.

## Immutability and operations

Run directories are claimed as `.incomplete`, receive `RUN_COMPLETE.json` and `SHA256SUMS`, then rename atomically to their final identity. Duplicate final or incomplete identities are rejected. Separate Points locks and namespaces are used. Parent manifests, run/slate/phase, canonical season, game type, orientation, pregame timestamps, scorer state, and quote timestamps are fail-closed checks.

Morning orchestration gained only `points_prerequisite_readiness`, `POINTS_MORNING_PREREQUISITES_READY`, and stage `10_points_prerequisites`; its schedule, existing Mainline/SOG stages, and market boundary are unchanged. Valid-empty and nonempty fixtures both passed. The new input snapshot builder binds the legacy strict-prior feature export to the manifested morning spine and records the mutable source hash before any scoring decision.

## Verification and disposition

All 24 certification checks passed with zero critical failures. Eight adversarial vectors produced no critical/high defect. The only bounded state is expected reduced coverage: no governed candidate policy and no live preseason provider/outcome-source observation yet.

Classifications:

- scorer reproduction: `READY_EXACT_BYTE_PARITY`
- prediction shadow: `READY_FOR_PRESEASON_BURN_IN`
- quote capture: `READY_FOR_PRESEASON_BURN_IN_LIVE_COVERAGE_UNVERIFIED`
- coherence gate: `READY_FAIL_CLOSED_CONTAINMENT`
- candidate: `NOT_READY_FAIL_CLOSED_MISSING_EFFECTIVE_POLICY`
- upload: `NOT_READY_DISABLED`
- grading: `READY_FOR_APPEND_ONLY_SHADOW_OBSERVATION_SOURCE_INPUT_MANUAL`
- sentinel: `READY_POINTS_SPECIFIC`
- overall: `READY_FOR_PRESEASON_BURN_IN_WITH_REDUCED_COVERAGE`
- Goalie Saves: `NOT_READY_FOR_PRESEASON_BURN_IN` (unchanged)

## Exact next bounded task

`NHL_POINTS_FIRST_NONEMPTY_PRESEASON_DUAL_SNAPSHOT_BURN_IN_V1`: on the first nonempty official preseason slate, execute one manual MIDDAY and one manual FINAL_PREGAME Points P/M observation, verify real provider/player binding and market disappearance/change evidence, and append one official-outcome grading observation. Keep C/U/E disabled and do not alter the frozen scorer or coherence gate.
