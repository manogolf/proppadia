# Evidence Threshold for Retraining

The 141,162-row operational contamination proves that the selector requires correction. It does not by itself prove that a particular retained model consumed those rows.

Retraining a specific model is justified only when all of the following are available:

1. The model artifact is cryptographically identified.
2. Its exact training rows or a deterministic reconstruction are bound to exact gamePks, source hashes, code identity, feature/target contract, split rules, and random seeds.
3. Exact authority proves at least one non-regular or unresolved gamePk was admitted to that model's training, validation, calibration, or threshold-selection population.
4. A reproducible regular-only counterfactual can be created without inventing observations, targets, features, predictions, or prices.
5. A predeclared comparison measures parameter/artifact hash change and out-of-sample behavior on an untouched authoritative regular-season holdout.
6. The decision rule is set before examining counterfactual performance.

Minimum decision rule:

- If exact bound membership is wholly regular, do not retrain for phase correction.
- If exact bound membership includes non-regular rows but retraining cannot be reproduced, retire or quarantine the certification claim; do not manufacture a replacement.
- If exact bound membership includes non-regular rows and reproducible retraining changes the model artifact, prediction population, calibrated probabilities, decision threshold, or any certification metric beyond the model's predeclared reproducibility tolerance, require a new model version and certification.
- If reproducible retraining is artifact-identical and every governed metric is within predeclared deterministic tolerance, retain the model only with a superseding manifest that documents the contamination removal and proof.

Creation date, current table contents, aggregate MODEL_INDEX row counts, or proximity to a retained CSV are insufficient evidence. Until a model meets this threshold, its lineage classification remains `INPUT_MEMBERSHIP_UNPROVABLE` or `RECONSTRUCTABLE_BUT_UNBOUND` and no retraining claim is made.
