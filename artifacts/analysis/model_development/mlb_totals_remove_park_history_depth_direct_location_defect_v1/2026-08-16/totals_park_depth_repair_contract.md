# MLB totals park-depth repair contract

- Control: `DIRECT_NEGATIVE_BINOMIAL_RAW_V1` / `fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac` with 22 direct location inputs.
- Research challenger: `DIRECT_NEGATIVE_BINOMIAL_PARK_DEPTH_REPAIR_V1` / `43256ef8396ddfdb53c58f04cc5b8fa783b97c457abf0072b767e7df6050d1b7` with 21 direct location inputs.
- Sole location-schema removal: `park_history_depth`.
- Retained park input: `strict_prior_total_run_factor`.
- Retained upstream confidence rule: `n = park_history_depth`; `w = n/(n+50)`; `park_factor = w*direct_prior_park_ratio + (1-w)*1.0`.
- Unchanged: governed rows/outcomes/exclusions/missing rules, all other feature definitions, StandardScaler, Poisson location family, alpha=0.1, max_iter=1000, target, negative-binomial dispersion construction, and probability support.
- No cap, replacement feature, prospective intercept, hyperparameter tuning, or Aug 6–15 outcome was used in fitting.
- Challenger scoring reads only its 21-field artifact order. Raw depth is absent, and no repair downstream code reintroduces it.
- Production/control model and existing `+0.493550` diagnostic remain unchanged.

`TRAINING_POPULATION_PARITY = EXACT`
