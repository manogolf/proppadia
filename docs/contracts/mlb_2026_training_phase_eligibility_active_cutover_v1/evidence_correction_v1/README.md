# MLB 2026 Training Phase Active Cutover Evidence Correction V1

Status: **CORRECTED; TRAINING BEHAVIOR UNCHANGED**

## Correction

The original active-cutover validator used an unfiltered recursive `.joblib` scan of `models_out` and `backend/mlb/models`. Its 444-path population contained 428 MLB binaries and 16 NHL binaries. It omitted 110 MLB binary paths included by the already-reviewed correction-design inventory.

The corrected validator uses exactly these sources:

- allowed model extensions below `models_out`, excluding every path with a component named `nhl`;
- allowed model extensions below `artifacts/mlb_models_bundle`;
- `backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib`;
- MLB-tagged allowed model extensions below `artifacts/analysis/model_development`.

Allowed extensions are `.joblib`, `.pkl`, `.pickle`, `.onnx`, and `.pt`. The current population is 538 unique MLB binary paths: 534 `.joblib` and 4 `.pkl`, with zero NHL paths. It comprises 536 `(device, inode)` identities because two known pairs are hard-linked. Each path remains an independent inventory record.

## Evidence semantics

The safety monitor records path, byte count, nanosecond modification time, device, inode, and population source. Its digest is a metadata-state digest. It does not open or hash existing model contents, and metadata equality must not be described as byte identity.

Corrected validation found unchanged metadata before and after its 47 dependency-free assertions. There is no evidence that a model changed. Because the original run did not monitor 110 MLB paths and neither monitor calculated per-binary content hashes, exhaustive byte identity for all 538 binaries is unprovable.

## Supersession and unaffected conclusions

The original `validation_report.json` and `scheduler_and_safety_audit.json` remain byte-for-byte preserved. Their 444-count metadata observation is historical evidence and is explicitly superseded only for population scope and byte-identity interpretation. The original eligibility-gate, frozen-count, retained-value-invariance, zero-fit, and lineage-binding results remain valid. Existing model lineage classifications are unchanged.

No trainer, eligibility gate, training manifest, model binary, database, schedule, or pipeline was changed or invoked by this correction.

Classification: `ACTIVE_CUTOVER_EVIDENCE_CORRECTED`
