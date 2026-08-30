# RAW training contract

- Population: exact 2023–2024 governed development spine, 4,859 rows, 2023-03-30 through 2024-09-30.
- Target: official final total runs (`OFFICIAL_FINAL_TOTAL_RUNS`).
- Location model: `sklearn.linear_model.PoissonRegressor`; Poisson deviance objective with L2 alpha `0.1`; fitted intercept; `lbfgs`; max iterations `1000`; tolerance `1e-4`; deterministic/no random seed.
- Preprocessing: `StandardScaler` fit on development rows only. Governed upstream fallbacks are preserved; the authoritative spine loader replaces non-finite direct inputs and fills remaining missing values with zero before fitting.
- Dispersion: refit after location by `max(0, sum(((y-mu)^2-y)) / sum(mu^2))`; NB support 0..30 with 30-plus tail folded into 30.
- Every Stage-3 model omits exactly one direct feature, refits the other 21 terms/intercept/scaler/dispersion once, and uses no evaluation row for fitting, tuning, scaling, thresholding, or selection.
- Count/support fields remain available to upstream gating, history sufficiency, fallback, and park shrinkage even when omitted from the direct location matrix.
