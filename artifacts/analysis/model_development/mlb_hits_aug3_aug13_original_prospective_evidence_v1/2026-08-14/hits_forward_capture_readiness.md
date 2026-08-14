# MLB Hits forward canonical capture readiness

`FORWARD_CANONICAL_CAPTURE_NEEDS_SMALL_PROVENANCE_PATCH`

The append-only scorer already preserves before authority blocking: canonical game/player identity, line, explicit P(Over), semantic model ID, exact model hash, feature-contract hash, run tag, prediction timestamp, scheduled start, feature-vector hash, odds-source path/hash/timestamp, and configuration hashes.

Missing from the exact requested schema: a dedicated explicit P(Under) field. P(Under) is currently deterministic as `1 - model_probability_over`, and identity subfields are encoded in canonical JSON as well as the exact feature vector rather than all being dedicated columns. No pipeline change is made here.
