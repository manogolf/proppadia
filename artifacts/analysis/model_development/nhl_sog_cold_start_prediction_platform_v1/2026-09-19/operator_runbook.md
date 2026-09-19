# Operator runbook and rollback

The installed 900-second NHL shadow runner invokes the prediction-only observer in the already-authorized MIDDAY/FINAL windows. It never requests odds. Inspect `artifacts/operational/nhl/sog_prediction_only`. A repeated phase is a no-op. To roll back, revert the integration commit; retain immutable run artifacts. Never delete or rewrite a prediction run. Grading consumes canonical final outcomes only and creates a separate grade directory.
