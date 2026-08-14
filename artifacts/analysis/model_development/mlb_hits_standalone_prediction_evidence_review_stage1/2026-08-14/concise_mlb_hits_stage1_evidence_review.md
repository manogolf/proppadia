# MLB Hits standalone prediction evidence review — Stage 1

Frozen population: 7,564 exact synchronized rows, 2026-05-08 through 2026-08-02; immutable source hash `3ac2ea64a2301d6224ab999216cf300807496e4e90aac271d615ce3f63f88088`.

This is descriptive evidence only. No certification decision, model change, recalibration, selector, ROI/EV, or UI recommendation was made.

## Line evidence
- Hits 0.5: n=6750; model Brier 0.247439 vs BetOnline 0.242614; model log loss 0.688501 vs 0.678218; mean absolute separation 6.85%.
- Hits 1.5: n=814; model Brier 0.223080 vs BetOnline 0.209705; model log loss 0.637614 vs 0.610467; mean absolute separation 8.24%.

## Separation
At >=15 pp: n=597; model Brier 0.259613 vs BetOnline 0.236642; Over share 35.3%; Hits 0.5 share 81.9%.

## Evidence limits
- Governed history-depth and starter-context classifications are not embedded in the frozen population, so those analyses fail closed.
- Current prospective Hits evidence is not mature enough to compare with the historical population.

## QUESTIONS_REQUIRING_REVIEW_BEFORE_CERTIFICATION
- Should Hits 0.5 and Hits 1.5 be reviewed as separate prediction authorities?
- Is the observed line/side asymmetry operationally material?
- Does deterioration at >=15 pp separation undermine probability trust despite aggregate parity?
- Is temporal behavior stable enough across the retained May-August window?
- Must prospective resolved evidence mature before any certification decision?
