# C park/context deployment stability

`PARK_CONTEXT_DEPLOYMENT_STABILITY=PASS`

- Raw `park_history_depth` is absent from C's 19 direct location inputs.
- Upstream shrinkage is unchanged and verified in the live bridge: `w=n/(n+50)` and `factor=w*direct+(1-w)*1.0`.
- All 141 retained Aug. 6–16 contexts used `DIRECT_REGRESSED_PARK_HISTORY`; no unseen-venue fallback was admitted.
- `strict_prior_total_run_factor` remains within the training min/max. An unseen venue fails to the explicit league factor `1.0`, rather than inventing direct park history.
