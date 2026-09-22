# Remaining historical limitations

- Existing model binaries, prediction artifacts, and evaluation results predate mandatory input/result bindings. This cutover does not manufacture lineage or certify them retroactively.
- The cutover governs the ordinary `model_trainer.py` path. Research code that directly imports lower-level pipeline constructors remains a distinct consumer and is not registered or published by this path.
- Reconcile and database source selections are bound by deterministic selected-row hashes. This task did not create new immutable database snapshots or modify the retained observation relation.
- Configured-view rows are gated at the earliest boundary available to `model_trainer.py`. Any phase contamination inside an external view's already-computed historical aggregates requires a separate view-definition audit; it is not proven clean by row-membership filtering alone.
- The file authority has a finite supported window. A future run whose exact gamePks are absent, whose source evidence conflicts, or whose authority is stale fails closed. Extending authority evidence is a separate governed acquisition task.
- No future training run was executed. Runtime production credentials, source availability, and resource capacity were therefore not exercised.

Smallest justified next action: review this commit. Before authorizing the first controlled future training run, confirm authority-window coverage and, if view mode is selected, audit that view's strict-prior feature construction. Keep automated retraining disabled until that review explicitly authorizes execution.
