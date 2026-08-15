# August 14 due-diligence effect

`AUG14_MODESTLY_STRENGTHENED_EVIDENCE`

- August 14 model Brier/log loss: 0.237278 / 0.667000.
- August 14 model-minus-population Brier/log-loss: -0.005339 / -0.011311.
- August 14 model-minus-hitter-shrunk Brier/log-loss: -0.004601 / -0.009937.
- The slate improved the model more than both baselines on both proper scores. The cumulative population-baseline Brier delta changed sign in the model's favor, while the hitter-baseline advantage widened.
- Model Brier/log loss moved from 0.244760 / 0.682670 through Aug 13 to 0.244066 / 0.681216 through Aug 14; fixed-bin ECE moved from 0.031982 to 0.029602.
- Population Brier-delta 95% CI moved from [-0.002957, 0.003779] to [-0.003301, 0.003050].
- Hitter-shrunk Brier-delta 95% CI moved from [-0.005306, 0.001579] to [-0.005252, 0.001014].
- Confidence ordering remains `DIRECTIONALLY_PRESENT`; upper-tail calibration remains `INSUFFICIENT_SAMPLE` with one row at >=75%.
- Clustered uncertainty still governs; this is a modest evidence update, not certification.
