# Concise MLB totals park_history_depth structural attribution v1

- Exact definition: integer count of earlier governed official games at the same venue; date-strict in the historical builder, accumulated across seasons, and used in `n/(n+50)` park-factor shrinkage.
- Intent: `SAMPLE_SIZE_SUPPORT_SIGNAL`. Frozen coefficient -0.026813037900692, training center 80.043836, scale 46.750716; each additional game multiplies expected location by 0.999426632.
- Distribution: development mean/median/max 80.043836/80.000000/161; prospective 291.269841/299.000000/302. `EXTREME` drift; 96.032% above development maximum.
- Calendar mechanics: `YES`; history is monotone within venue and does not reset by season. The deployed prospective bridge retains the through-Aug-5 state during this window.
- Interpretation: the negative coefficient mechanically lowers location as the support count rises. Training classification `LIKELY_SAMPLE_DEPTH_ARTIFACT`; calendar-controlled within-park actual-total correlation -0.014669, `ABSENT` signal.
- Out-of-support: 121/126 prospective games exceed the training maximum; outcome association `MODERATE` because the only five in-support rows are one low-depth venue and do not supply a balanced comparator.
- Prospective RAW: MAE 3.265727, RMSE 4.092129, actual-minus-forecast bias +0.558992, CRPS 2.313878.
- Training-mean A: MAE 3.322858, RMSE 4.102716, bias -0.472471, CRPS 2.304244; signed mechanical offset 184.522%.
- P95-cap B: MAE 3.295099, RMSE 4.073781, bias -0.104907, CRPS 2.294232; signed mechanical offset 118.767% and absolute mean-bias reduction 81.233%.
- Coefficient-zero C: algebraically identical to A under the frozen scaler; MAE 3.322858, RMSE 4.102716, bias -0.472471, CRPS 2.304244.
- Historical: p95 capping leaves actual-minus-forecast bias -0.010502 in 2025, +0.064473 early 2026, and +0.016649 late holdout; CRPS improves in all three, while MAE does not.
- Stability: `MODERATE`. The model-mechanical shift is broad across mature parks and common forecast bands, but daily error improvement is mixed and the nonlinear band shape remains.
- +0.493550 intercept: bias +0.065442, CRPS 2.285738; `PARTLY_COMPENSATES_PARK_DEPTH`. `INTERCEPT_DIAGNOSTIC_INTERPRETATION_COMPROMISED`.
- Related support/count features requiring later safety attention: home_starter_prior_starts (EXTREME, INFLATION), away_starter_prior_starts (EXTREME, INFLATION). None was counterfactually optimized.
- Design/root: `BETTER_AS_CONFIDENCE/WEIGHT_SIGNAL` / `MIXED_STRUCTURAL_DEFECT`. Focused redesign `JUSTIFIED`.
- Final: `PARK_HISTORY_DEPTH_PRIMARY_STRUCTURAL_DRIVER` (model-mechanical, not causal baseball attribution).
- Exact next human decision: authorize or decline a separately governed feature redesign comparison; do not alter V1 or promote any counterfactual from this task.
