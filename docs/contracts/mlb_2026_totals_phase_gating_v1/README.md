# MLB 2026 Totals phase gating V1

RAW Totals and Totals C are genuinely coupled: C derives each row from an immutable RAW prediction/context identity, and the ordinary hook sequences RAW before C. One shared exact-`gamePk` phase interface now gates both lanes without adding phase columns to either append-only ledger.

Retained evidence is unchanged. The observed population is RAW 625 predictions/gamePks (623 regular season, 2 postseason) and C 482 predictions/gamePks (482 regular season), with C an exact RAW subset for all 482 identities and 143 RAW-only identities. No independent retained source/ingestion census establishes an expected total, so population checks are `INCONCLUSIVE`; observed counts are not treated as their own oracle. Existing predictions, outcomes, features, proper-score inputs, market inputs, thresholds, models, selector status, and publication status were not changed.

Regular-season reporting remains the default. Postseason requires explicit `POSTSEASON` evaluation mode and remains shadow-only. Operational readiness is blocked until an actual authoritative postseason game traverses the ordinary RAW and C paths; synthetic coverage proves code behavior only.

The SciPy coefficient discrepancy remains open; this validation did not establish its cause or change any tolerance or production check.

Validation: 41 passed, 0 failed, 0 skipped; overall `INCONCLUSIVE`.
