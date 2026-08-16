# MLB totals count-feature repair variant contracts

Candidate set predeclared before evaluation:

- **A CONTROL** `DIRECT_NEGATIVE_BINOMIAL_RAW_V1`: frozen 22 direct fields; all raw counts remain direct.
- **B PARK-ONLY** `DIRECT_NEGATIVE_BINOMIAL_PARK_DEPTH_REPAIR_V1`: 21 direct fields; raw `park_history_depth` absent, upstream `n/(n+50)` park shrinkage retained; starter counts remain direct.
- **C CONFIDENCE-ONLY** `DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1`: 19 direct fields; raw park and starter counts absent. The counts remain unchanged in park shrinkage, starter fallback, minimum-history, workload, and confidence state.
- **D LOW-DEPTH** `DIRECT_NEGATIVE_BINOMIAL_LOW_DEPTH_EXPERIENCE_V1`: 23 direct fields; C plus four bounded indicators for home/away `n=0` and `n=1–2`, with `n>=3` as the reference. These are the pre-existing governed fallback boundaries and cannot grow after mature support.
- **E** `VARIANT_E_STATUS = NOT_AUTHORIZED_NO_PREEXISTING_SEMANTIC_TRANSFORM`. The existing park support weight is already consumed by park-factor shrinkage; reusing it in location would double-use confidence. No pre-existing starter transform exists beyond D's states.

No removed raw count or `*_history_depth` alias is present in C/D direct feature order. Identical upstream feature construction, fallback inputs, park factor, starter state, training rows, StandardScaler/Poisson settings, target, dispersion construction, and probability contract are retained.
