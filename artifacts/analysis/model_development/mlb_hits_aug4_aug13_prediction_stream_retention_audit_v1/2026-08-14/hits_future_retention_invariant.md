# Future retention invariant

Every scoring run must durably preserve, before downstream authority checks: game/player identity, line, P(Over), P(Under), semantic model ID, exact model hash, feature-contract hash, source run tag, prediction timestamp, scheduled start, and source hashes. Publication authority may suppress routing or public output but must not erase research evidence.

Current code **partially satisfies, but does not exactly satisfy, this invariant**. The append-only certified lineage preserves identity, line, P(Over), selected-side probability, semantic/model/feature/source hashes, run tag, prediction timestamp, and scheduled start before the slate authority guard. It does not persist a dedicated explicit `P(Under)` field (the value is deterministically `1 - P(Over)`), and some identity fields are encoded in canonical JSON rather than dedicated columns. No implementation change is made by this audit.
