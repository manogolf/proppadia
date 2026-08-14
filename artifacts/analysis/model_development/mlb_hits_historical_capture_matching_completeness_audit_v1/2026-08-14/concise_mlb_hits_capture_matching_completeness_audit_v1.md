# MLB Hits historical capture and matching completeness audit v1

- Retained game denominator: 1,138; player-game opportunities: 22,579.
- Raw BetOnline Hits observations: 9,253; unique latest pregame propositions: 8,310.
- Model Hits propositions: 27,826; synchronized rows: 7,564 (27.2% of retained model proposition denominator).
- Starting-hitter opportunity coverage is not uniformly retained and was not approximated.
- Primary attrition is missing/unresolved pregame timing, absent BetOnline exact propositions, and outcome/final-freeze availability; paired-price-only losses are separately quantified.
- No additional row is counted as recovered yet, but the raw-to-canonical identity gap justifies a bounded deterministic recovery pass using retained artifacts.
- The synchronized population is materially selected by provider identity/timing/outcome availability; current governed capture is materially more complete.
- Decision: `HITS_HISTORICAL_COVERAGE_INCOMPLETE_RECOVERY_JUSTIFIED`.
