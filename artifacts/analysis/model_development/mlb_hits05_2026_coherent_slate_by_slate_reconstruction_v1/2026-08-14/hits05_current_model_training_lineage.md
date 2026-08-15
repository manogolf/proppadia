# Hits 0.5 current-model training lineage

`CURRENT_ARTIFACT_TRAINING_LINEAGE = PARTIAL`

## Recovered exactly

- Semantic model: `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb`
- Artifact SHA-256: `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`
- Embedded fit timestamp: `2026-07-09T06:11:24.788077`
- Artifact filesystem birth/retention date: 2026-07-08 23:11 PT
- Training script at the fit-time Git boundary: `backend/mlb/model_trainer.py` at commit `6ebcebb2db14fbefc3f7a999c8a855f2ea797072`
- Fit-time script blob SHA-256: `a68056474d2ed6e4c22e770f5e300e26e84740826bc82ad098e198c30e1fbccb`
- Fit-time feature-metadata blob SHA-256: `e886bc6607968881042e403b07feaa8d77720bafd0066f6c91a3b27d5342c7c7`
- Embedded training configuration: `days_back=540`, `limit=150000`, legacy profile, no bookmaker filter, 73 numerical inputs.
- Model-index row count: 61,584; chronological 80/20 split implies 49,267 fit rows and 12,317 validation rows if no stratified fallback occurred.
- Components: median imputer; isotonic-calibrated logistic regression (`cv=3`, `max_iter=1000`); random forest (`n_estimators=300`, `random_state=42`, `class_weight=balanced`); validation-AUC-weighted probability blend.
- Embedded validation results: LR AUC `0.5156808568895628`, RF AUC `0.5040900420373041`, selected threshold `0.45`, weighted validation accuracy `0.5486725663716814`.
- Runtime line transformation: deterministic line-sensitivity code after the fitted blend; it is not an independently fitted artifact.

## Not retained

- Training row identities, labels, row order, exact earliest/latest event dates, and training-data hashes.
- The environment selecting `reconcile_csv`, `base_merge`, or an optional feature view.
- The exact database snapshot of `mlb.model_training_props`, `mlb.player_derived_stats`, and precomputed BvP inputs.
- A record of whether the chronological split or stratified fallback was used.
- Fold membership for isotonic calibration and the historical feature rows needed to reproduce each fit.

The currently retained reconcile file cannot be the recorded 61,584-row population: it has 360 total rows, 136 Hits rows, only date 2026-05-08 through 2026-05-08, and SHA `3d610fc403989ff77cc33fa3473fc9450a4d30808251ba3c358d637ddad7cacd`. The 61,584-row count therefore implies a different source branch or source state that was not retained.

## Knowledge cutoff

`MODEL_KNOWLEDGE_CUTOFF = 2026-07-08`

This is the strongest defensible upper bound: the artifact was fit at 2026-07-09 06:11 UTC, so completed MLB events through the July 8 slate were capable of influencing it. Because training identities were not retained, the exact maximum event actually present cannot be row-verified.
