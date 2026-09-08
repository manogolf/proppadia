# NHL Points and Goalie Saves Dependency-Ordered Remediation Sequence

This plan is evidence-derived and does not authorize any implementation, purchase, credential addition, workflow activation, or model change.

## 1. Required before September 19

1. Implement `NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1` as a new, non-production namespace. Freeze the three active LR artifact hashes and feature order without fitting. Preserve official game/player/game-type identity, exact strict-prior input bytes, raw and declared-monotone probabilities, and create-only run metadata.
2. Add Points-only book-level, two-sided Odds API normalization with provider event binding, book, line, side, price, source timestamp, capture timestamp, and status. Support explicit `MIDDAY` and `FINAL_PREGAME` run types with pre-start gates. Do not reuse the SOG module by assumption.
3. Add a Points observation-only effective policy declaration, empty execution ledger, upload-shaped-but-nonexecuting output, explicit scratch/nonparticipant exclusions, preseason non-evaluation labels, and lane-specific sentinel input. The first implementation test must use fixtures only.
4. Keep Goalie Saves disabled. Obtain a human decision on a projected/confirmed starting-goalie source. Before any integration, prove source-state timestamp semantics, access authorization, deterministic game/team/player crosswalk feasibility, and append-only projected-to-confirmed transitions. Do not buy or configure anything implicitly.
5. Freeze the currently active Saves artifact pair by hash in an assessment-only release declaration and quarantine conflicting copies conceptually; no coefficient/calibration/line changes are authorized by this assessment.

## 2. Safe to exercise during preseason

1. Run Points as `SHADOW_OBSERVATION_ONLY` at MIDDAY and FINAL_PREGAME on official preseason games. Validate population denominators, roster/scratch reasons, strict-prior hashes, quote coverage, line monotonic diagnostics, duplicate-free identities, and unchanged earlier snapshots. Do not interpret preseason results as regular-season model evidence.
2. Exercise Points append-only outcome capture with nonparticipants, postponements, missing logs, and corrections remaining ungraded or explicitly superseded. No UI recommendation or execution activation.
3. For Goalie Saves, only if the prerequisite source proof passes, exercise read-only source capture and identity/state-transition plumbing. Until then, only raw market acquisition can be tested independently; do not create goalie predictions or candidates.
4. Validate generic sentinel behavior using lane-complete fixture inputs, then invoke it from each new shadow run. The existing morning sentinel alone is not evidence of lane health.

## 3. Deferrable until before September 29

1. Connect the proven Points prerequisite checks to morning health without changing its current Mainline/SOG gates until fixture and preseason runs pass.
2. Add append-only Points grade/correction artifacts and an explicit operator handoff. Keep candidate, upload, and execution states separate.
3. If and only if the goalie source is certified, build the bounded Saves immutable runner, exact projected/confirmed eligibility rule, source freshness sentinel, book-level quotes, game-type isolation, and outcome-only actual starter record.
4. Reconcile Saves line exposure: unsupported lines must remain diagnostic/non-actionable pending separate evidence; align API/UI output with the declared burn-in contract.
5. Require a nonempty prospective burn-in record and hostile failure fixtures before either lane can be considered for regular-season operational enablement.

## 4. Research-only and not operationally necessary

1. Refit or retrain either model.
2. Develop Points or Saves challengers, add new features, or optimize candidate thresholds.
3. Evaluate ROI, wagering performance, promotion, or production recommendation policy.
4. Recalibrate Points or Saves probabilities. The Points wrapper may preserve both raw and mechanically monotone diagnostic probabilities, but any learned recalibration is separate research.
5. Study a broader goalie-source vendor set after a practical timestamp-certifiable source is secured; this does not unblock current operations.

## Exact next bounded implementation task

`NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1` is the next supported task. It is independent of the blocked commercial goalie-source decision, preserves the current Points model unchanged, and closes the minimum integrity gaps needed for safe, non-actionable preseason observation.
