# Concise MLB totals RAW run-environment bias decomposition v1

- Model/hash: `DIRECT_NEGATIVE_BINOMIAL` / `fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac`; artifact SHA-256 `c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe`.
- Residual convention: `actual - RAW`; positive means underforecast.
- Chronology: 2025 frozen validation +0.215047; 2026 sequential early +0.577433; exact 439-game late holdout +0.661055; 126-game prospective +0.558992. `LONGSTANDING_MODEL_BIAS`.
- Actual runs/game: 2025 8.888204; 2026 pre-Aug-6 8.973837; Aug 6–15 all official 8.664179 (-0.309658). No recent scoring surge is present.
- Forecast magnitude: `NONLINEAR`. Team-side forecast residuals are unavailable because RAW emits one direct total.
- Inning context: Aug 6–15 all-official 1–5 / 6–9 / extras runs per game 4.858209 / 3.492537 / 0.313433; `NO_RECENT_EARLY_OR_LATE_SCORING_EXCESS; EXTRA_INNINGS_MODESTLY_HIGHER`.
- Associations: pitching `MIXED`; offense `MIXED`; weather/environment `NOT_TESTABLE`.
- Park/timing: `BROAD_ACROSS_VENUES_WITH_CUMULATIVE_PARK_HISTORY_DEPTH_DRIFT`; `SCORE_MISSING_ROWS_REDUCE_GLOBAL_UNDERFORECAST`.
- Residual distribution: `BROAD_WITH_TAIL_CONTRIBUTION`; 5%-absolute-trimmed mean +0.171851. Underforecast sign disappears in exclusion stress: False.
- Baseline actual-minus-forecast residuals: RAW +0.558992; population -0.358833; team -0.361917. `RAW_MODEL_SPECIFIC`.
- Frozen intercept: `APPROPRIATE_ON_AVERAGE_BUT_HETEROGENEOUS`; `BROAD_MAJORITY_NOT_UNIVERSAL_AND_RESIDUAL_ALIGNED` (`INTERCEPT_IMPROVES_20_OF_30_ADEQUATE_SUBGROUPS`).
- Strongest attribution: cumulative `park_history_depth` drift, `STRONG_SUPPORT`; prospective frozen log-location contribution -0.121145 (factor 0.885906).
- `BASEBALL_CAUSAL_FOLLOWUP = NO_CAUSAL_FOLLOWUP_YET`.
- `TOTALS_BIAS_MODEL_SPECIFIC_STRUCTURAL_MISS`.
- `V1_INTERCEPT_CORRECTS_AVERAGE_BIAS_BUT_MASKS_STRUCTURE`.
- `NEXT_RESEARCH_DIRECTION = CONTINUE_UNCHANGED_PROSPECTIVE_COLLECTION + GLOBAL_RUN_ENVIRONMENT_MODEL_REVIEW + PARK/CONTEXT_REVIEW`. No next task was executed.
