# MLB Hits 0.5 historical model provenance recovery v1

- Frozen board: 17,603 rows; SHA-256 `25d8a80c87b929a9550be2b9fd4a362ac0bd97db66afacc5bcdf4a45ef7aa0d6`; probabilities were not rebuilt.
- Producer: wide builder → strict-prior prop workflow → `make_prediction` → slate exporter.
- Model: standalone sklearn LR/RF AUC-weighted blend with deterministic line sensitivity; no market feature and no evidenced calibration.
- Feature contract: `EXACT_FEATURE_CONTRACT_RECOVERED` per retained artifact generation.
- Artifacts: `FITTED_ARTIFACT_EXACTLY_RECOVERED`; six generations span May 8–August 2.
- Replay: `PARTIAL_REPLAY` on 60 deterministic retained rows; exact=0, within export rounding=0, max abs diff=0.077789846, mean=0.011165681, side parity=57/60.
- Current semantic model: `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb`, SHA-256 `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`; byte-identical to the July 9 generation but not to all earlier benchmark generations.
- Continuity: `CURRENT_MODEL_IS_MATERIALLY_CHANGED_DESCENDANT`.
- Same-row diagnostic (no outcomes): rows=60, mean abs diff=0.016599, median=0.006197, correlation=0.955444, side agreement=95.000%, band migrations=5.
- Authority: `HISTORICAL_PREDICTION_EVIDENCE_VALID_BUT_MODEL_IDENTITY_PARTIAL`.
- Prospective decision: `DESCENDANT_MODEL_PROSPECTIVE_CAPTURE_REQUIRES_NEW_BASELINE`.
- Hits 1.5 Under: same producer/model family, evaluated at a different line.
- Final: `HITS05_HISTORICAL_MODEL_PROVENANCE_PARTIALLY_RECOVERED`.

Exact supported next step: start a separately labeled prospective baseline for the current semantic model with the minimum provenance invariant; do not merge it into the historical benchmark.
