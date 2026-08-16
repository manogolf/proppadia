# Starter-count coefficient flip analysis

The row population and retained-feature scalers are identical, yet removing `park_history_depth` changes home count `+0.012677233712015` to `+0.000604547219713` and away count `+0.000826835652700` to `-0.010872043168453`. Home/away count correlation is `0.4021`; correlations with park depth are home `0.6215` and away `0.6173`; VIFs are home `1.907` and away `1.882`.

Classification: `MULTICOLLINEARITY_REASSIGNMENT`. The sign flip is not evidence that away-starter experience causally suppresses runs; it is the fixed-artifact symptom of correlated cumulative support proxies reallocating location weight after park-depth removal.
