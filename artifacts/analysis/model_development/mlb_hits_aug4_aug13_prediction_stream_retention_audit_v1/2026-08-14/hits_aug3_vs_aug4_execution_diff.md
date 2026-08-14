# August 3 versus August 4 execution boundary

August 3 retained the older run-tagged slate/wide surfaces through downstream slate construction. After commit `052360a7d1380b48b9132cd2ee1c2b9f62e7f660`, August 4 first diverged at `build_mlb_slate_output.py::main`: `production_slate_generation` failed closed under `NO_QUALIFIED_MLB_PROP_MODEL`, so candidate/routing, upload, and public-production artifacts were not produced.

The divergence was **after scoring**. `build_mlb_predictions_wide.py` still generated line-specific Hits probabilities and wrote its mutable current-wide output. Commit `73160edf67a8afa9425f4b55d8f80c82a91dd112` also caused exact probability rows to be append-preserved before the guard in each date's `prospective_lineage/.../prediction_lineage_ledger.csv`.

Therefore the first concrete difference was downstream slate persistence/routing, not scorer invocation or raw probability retention.
