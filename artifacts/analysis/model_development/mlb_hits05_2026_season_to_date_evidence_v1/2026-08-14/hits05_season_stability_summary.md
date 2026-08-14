# Hits 0.5 season stability summary

`HITS05_2026_SEASON_EVIDENCE_STABLE_WITH_KNOWN_LIMITATIONS`

1. Strict monthly Brier remains in a similar range: `0.243285`–`0.245479`.
2. Log loss is broadly stable by month; detailed values are fixed in `hits05_monthly_metrics.csv`.
3. Calibration is broadly stable but upper-tail overconfidence remains visible.
4. Strict-season confidence ordering is `MONOTONIC` and generally useful, with month/generation variation reported separately.
5. Upper-tail overconfidence is `PERSISTENT_STRUCTURAL`, strongest at >=75% in the historical board; no recalibration is performed.
6. Generation metrics vary, but no generation invalidates the aggregate; early legacy model identity remains unresolved.
7. August 3–13 is `AUGUST_CONTINUITY_CONSISTENT`, not a regime break.
8. Whole recoverable and strict-pregame results tell the same broad story; the strict population is the stronger apples-to-apples evidence.
