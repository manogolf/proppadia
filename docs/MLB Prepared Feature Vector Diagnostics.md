# MLB Prepared Feature Vector Diagnostics

`build_mlb_predictions_wide.py` writes a debug-only prepared feature export after
`prepare_prop()` applies runtime hydration, aliases, and fallbacks, and before
`predict_prop()` is called.

Default output:

`backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<date>/<prop_type>_features.csv`

This export is observability only. It does not change model logic, prediction
inputs, prediction outputs, slate output, or upload behavior.

For `hits`, `rolling_result_avg_7` is currently equivalent to `d7_hits` at
runtime when the payload does not provide `rolling_result_avg_7` directly.
