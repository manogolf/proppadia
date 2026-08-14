# MLB Hits predictive parity on recovered population v2

Evidence reevaluation only; no certification, model, recalibration, selector, prospective capture, EV/ROI, or UI change.

- Frozen population: 12,618 rows (7,564 original; 5,054 recovered-only); behavior `RECOVERED_ROWS_PARTIALLY_DIFFER`.
- Pooled comparison: Proppadia Brier 0.243693, log loss 0.680802, ECE 0.020174; BetOnline Brier 0.237994, log loss 0.668901, ECE 0.023470; `BROADLY_COMPARABLE`.
- `HITS_05_OVER`: n=8,476; Brier 0.244628; observed 59.1%; ECE 0.044854; ordering `PARTIAL_ORDERING`; temporal `STABLE`; Stage 2 Brier delta -0.002198; BetOnline-relative `approximately comparable`.
- `HITS_05_UNDER`: n=2,596; Brier 0.251032; observed 47.4%; ECE 0.060234; ordering `MONOTONIC_OR_NEAR_MONOTONIC`; temporal `DETERIORATING`; Stage 2 Brier delta +0.001646; BetOnline-relative `approximately comparable`.
- `HITS_15_OVER`: n=164; Brier 0.270158; observed 29.3%; ECE 0.243492; ordering `INVERTED_OR_UNRELIABLE`; temporal `MILD_DRIFT`; Stage 2 Brier delta +0.002790; BetOnline-relative `materially worse`.
- `HITS_15_UNDER`: n=1,382; Brier 0.221030; observed 70.0%; ECE 0.097513; ordering `FLAT`; temporal `MILD_DRIFT`; Stage 2 Brier delta +0.003941; BetOnline-relative `modestly worse`.

`HISTORICAL_MODEL_IDENTITY = UNRESOLVED`: performance evidence is auditable, but exact historical producer/version binding remains unavailable.

## QUESTIONS_REQUIRING_HUMAN_DELIBERATION_AFTER_RECOVERY
- Did Hits 1.5 Under survive the doubled sample strongly enough to prioritize provenance work?
- Is Hits 0.5 Over extreme-confidence deterioration material enough to preclude formal review?
- Is the persistent Hits 0.5 Under temporal deterioration durable?
- Is 164 rows still too sparse for Hits 1.5 Over conclusions?
- Can exact historical model producer/version binding be recovered before any certification review?
- Which lanes, if any, deserve formal certification review after provenance is resolved?
