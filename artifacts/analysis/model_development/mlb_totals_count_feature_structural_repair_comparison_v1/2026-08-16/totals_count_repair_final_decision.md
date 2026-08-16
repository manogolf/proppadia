# MLB totals count-feature structural repair comparison v1

`COUNT_STRUCTURAL_REPAIR_STRUCTURALLY_BETTER_BUT_POINT_TRADEOFF_UNRESOLVED`

- A/B loaded frozen; C/D fit exactly once on 4,859 identical development rows. Variant E: `NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM`.
- Stationarity: {"A_CONTROL": "FAIL", "B_PARK_ONLY": "PARTIAL", "C_CONFIDENCE_ONLY": "PASS", "D_LOW_DEPTH": "PASS"}.
- Point effects: {"A_CONTROL": "CONTROL_REFERENCE", "B_PARK_ONLY": "MIXED", "C_CONFIDENCE_ONLY": "MIXED", "D_LOW_DEPTH": "MIXED"}.
- Probability effects: {"A_CONTROL": "CONTROL_REFERENCE", "B_PARK_ONLY": "IMPROVED", "C_CONFIDENCE_ONLY": "IMPROVED", "D_LOW_DEPTH": "IMPROVED"}.
- Bias stability: {"A_CONTROL": "MATERIAL_DRIFT", "B_PARK_ONLY": "SMALL_RESIDUAL_BIAS", "C_CONFIDENCE_ONLY": "SMALL_RESIDUAL_BIAS", "D_LOW_DEPTH": "SMALL_RESIDUAL_BIAS"}.
- Coefficient reassignment risk: {"B_PARK_ONLY": "LOW", "C_CONFIDENCE_ONLY": "LOW", "D_LOW_DEPTH": "LOW"}.
- Related count result: `NO_OTHER_DRIFTING_COUNT_ABSORPTION_DETECTED`.
- Intercept status for strongest structural candidate `C_CONFIDENCE_ONLY`: `LIKELY_UNNECESSARY`.
- Preferred research challenger: `NONE_NO_CLEAR_WINNER`.
- Shadow readiness: `TOTALS_REPAIRED_CHALLENGER_NOT_SHADOW_READY`. No promotion or shadow activation occurred.

Exact next decision: review C versus D tradeoffs and authorize a new task only if one contract is selected; do not start shadow capture yet.
