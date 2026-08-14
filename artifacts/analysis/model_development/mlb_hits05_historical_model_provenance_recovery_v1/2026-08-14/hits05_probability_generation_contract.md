# Hits 0.5 probability-generation contract

The strict-benchmark producer is a **standalone baseball model**, not a market-informed model. The fitted artifact contains logistic-regression and random-forest sklearn pipelines. Each emits `predict_proba`; the producer weights available probabilities by `max(validation AUC - 0.5, 0)` and uses their mean if all weights are zero. It clamps the blend to `[0,1]`, then applies the March 31 deterministic line transform:

`sigmoid(logit(p) + 0.90 * ((history_mean - line) / history_scale))`

`history_mean` is the 0.60/0.30/0.10 weighted mean of d7/d15/d30 Hits when present, falling back to `rolling_result_avg_7`; the Hits base scale is 0.85 with a bounded horizon-spread adjustment. At line 0.5 this becomes historical P(1+ hit). The slate exporter rounds to six decimals, defines `P(Under)=1-P(Over)`, and chooses Over when `P(Over)>=0.5`. The model artifact's decision threshold affects the producer's legacy `predicted_outcome`, but not the slate's final selected-side rule.

No market field occurs in the retained artifact input columns. No fallback probability was observed in the replay. Upload calibration is opt-in; frozen output evidence has blank calibration method and raw/final equality, supporting no downstream calibration. Market data accompanied rows but did not generate model probability.

Replay: 60 rows, 0 bit-exact before export-rounding, 0 within 1.1e-6, max difference 0.077789846, mean difference 0.011165681, side parity 57/60; `PARTIAL_REPLAY`. Differences outside rounding reflect that daily prepared-vector snapshots are not cryptographically bound to the earliest frozen row, so they are not approximated away.
