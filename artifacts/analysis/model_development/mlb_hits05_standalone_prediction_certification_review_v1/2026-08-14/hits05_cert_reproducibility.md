# Hits 0.5 reproducibility

`REPRODUCIBILITY = PASS_WITH_SMALL_PROVENANCE_PATCH`

- Frozen artifact and SHA: `models_out/latest/hits.joblib` / `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`.
- Artifact-defined feature contract is recoverable; semantic manifest is retained.
- Strict historical timing and original probability sources are durable.
- August 3–13 has exact Tier A semantic/model continuity and append-only prediction lineage.
- Outcomes are attached separately after population freeze.
- Remaining small gap: explicit `P_UNDER_0_5` is derived as `1-P_OVER_0_5`, not independently persisted.
