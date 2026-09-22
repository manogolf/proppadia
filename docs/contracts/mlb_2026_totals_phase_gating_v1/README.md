# MLB 2026 Totals phase gating V1

RAW Totals and Totals C are genuinely coupled: C derives each row from an immutable RAW prediction/context identity, and the ordinary hook sequences RAW before C. One shared exact-`gamePk` phase interface now gates both lanes without adding phase columns to either append-only ledger.

Retained evidence is unchanged. RAW contains 608 predictions / 608 gamePks and C contains 467 / 467; every retained gamePk is authoritatively `REGULAR_SEASON`, C is an exact RAW subset, and the affected-row ledger is empty. Existing predictions, outcomes, features, proper-score inputs, market inputs, thresholds, models, selector status, and publication status were not changed.

Regular-season reporting remains the default. Postseason requires explicit `POSTSEASON` evaluation mode and remains shadow-only. Operational readiness is blocked until an actual authoritative postseason game traverses the ordinary RAW and C paths; synthetic coverage proves code behavior only.

Validation: 37 passed, 0 failed, 0 skipped; overall `PASS`.
