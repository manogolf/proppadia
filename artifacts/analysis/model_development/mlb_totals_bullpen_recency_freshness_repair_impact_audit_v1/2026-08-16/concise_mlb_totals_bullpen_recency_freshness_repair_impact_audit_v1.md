# MLB Totals bullpen recency freshness repair and impact audit v1

- `BULLPEN_RECENCY_FRESHNESS_REPAIR_VALIDATED`; root cause `STALE_ARTIFACT_DEFECT`; defect starts 2026-08-07.
- Frozen state: 136 affected RAW rows over 10 dates, including 15 immutable August 16 predictions. C diagnostic rows affected through August 15: 121.
- Burden states (282 side rows): original zero=214, mean=1.390071; corrected zero=0, mean=9.960993; zero-to-nonzero=214.
- RAW affected-completed counterfactual delta: mean forecast=+0.134716, MAE=-0.005309, RMSE=-0.014654, bias=-0.134716, CRPS=-0.010865, Brier=-0.001549, log loss=-0.003606, ECE=-0.011777.
- C corrected-feature impact `SMALL`: MAE=+0.004393, RMSE=+0.006223, bias=-0.087307, CRPS=+0.001380, Brier=-0.000069, log loss=-0.000172, ECE=+0.004309.
- Historical 2025/early-2026/late-holdout strict-prior rows reproduce with zero mismatches: `HISTORICAL_BULLPEN_EVIDENCE=UNAFFECTED`.
- Repair: read-through of already-retained official final feeds plus explicit cutoff/acquisition/hash provenance; stale coverage returns null state and fails context scoring rather than emitting zero.
- C gate `PASS_WITH_WATCH`; shadow decision `TOTALS_COUNT_CONFIDENCE_ONLY_SHADOW_READY_WITH_WATCH_ITEMS`; RAW record `RAW_PROSPECTIVE_RECORD_PARTIALLY_CONTAMINATED_BY_STALE_BULLPEN_STATE`.
- No refit, recalibration, prediction mutation, shadow launch, EV/ROI calculation, or push.
