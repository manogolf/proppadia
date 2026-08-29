# Totals C structural validation

- Direct location excludes `park_history_depth`, `home_starter_prior_starts`, and `away_starter_prior_starts`.
- Those counts remain limited to governed confidence, shrinkage, support, workload, and fallback state.
- Feature contract `d7551fd7798aa60ada1b96831e32bcb7748a17aabf67f53c8800f24c9f4a0927` held on all 156 admitted rows; H/I failures were zero.
- No live drift indicates indirect reintroduction of the removed direct-location pathology.
`COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12 = PROSPECTIVELY_SUPPORTED`
