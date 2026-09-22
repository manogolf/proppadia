# Sidecar Logical Contract

This is a logical contract, not executable DDL.

## Justified objects

### 1. `mlb.game_phase_authority_v1`

Required because a shared database authority must cover games independently of membership in `game_info`, clean-room observations, predictions, markets, or outcomes. It has exactly one current authority row per exact gamePk.

### 2. `mlb.canonical_game_phase_v1`

A read-only consumer view is justified to isolate consumers from storage and privilege details. It reads only `mlb.game_phase_authority_v1`; it does not union candidates from other tables. Only rows with an unambiguous admitted status are eligible for positive phase membership. Recognized special games remain inspectable but never phase-eligible.

### Objects not justified

- No database observation ledger in V1. Immutable raw files and the hashed retained-source manifest already preserve every observation.
- No phase columns in existing tables.
- No materialized per-consumer phase copies.
- No conflict table initially. Conflict failure evidence is a source-hashed, immutable artifact; the sidecar row's authority status makes consumers fail closed.

## Required logical fields

| Field | Requirement |
|---|---|
| `game_pk` | Exact MLB gamePk; non-null; sole row identity; one row per gamePk. |
| `source_season` | Positive authoritative source season; never parsed from a played date. |
| `source_game_type` | Exact case-sensitive StatsAPI wire value; never trimmed, case-folded, or defaulted. |
| `season_phase` | `PRESEASON`, `REGULAR_SEASON`, or `POSTSEASON`; null only for recognized excluded special types. |
| `postseason_round` | Frozen normalized round; required for postseason and null otherwise. |
| `source_round` | Exact raw source round/type description when supplied; null is distinct from an empty or inferred value. |
| `source_provider` | Exact provider contract identity, initially `MLB_STATSAPI`. |
| `source_observed_at_utc` | Observation time belonging to the selected authoritative evidence. |
| `source_observation_identity` | Deterministic identity of provider, request scope, source path, and source hash. |
| `source_payload_sha256` | Lowercase SHA-256 of exact retained response bytes used for admission. |
| `source_artifact_path` | Repository-relative or governed retained-source reference; never a credential-bearing URI. |
| `schedule_relationships` | Exact source-provided rescheduled/resumed/related-game fields as a JSON object; no inferred relationships. |
| `authority_status` | `AUTHORITATIVE_UNAMBIGUOUS`, `SPECIAL_EXCLUDED`, `CONFLICT_BLOCKED`, or `CORRECTION_REVIEW_REQUIRED`. |
| `contract_name` | Frozen classifier identity. |
| `contract_version` | Exact semantic version governing type-to-phase mapping. |
| `contract_sha256` | Hash of the frozen executable classification contract. |
| `admitted_at_utc` | Time of first successful authoritative admission. |
| `admission_run_identity` | Source-hashed run/proposal identity; no mutable scheduler label alone. |
| `admission_proposal_sha256` | Hash of the exact proposal or prospective admission batch. |
| `authority_revision` | Starts at one; changes only in separately governed enrichment/correction handling. |
| `last_evidence_at_utc` | Latest accepted evidence verification time; it does not imply a core classification change. |
| `correction_evidence_sha256` | Null for ordinary rows; required when a separately governed provider correction is approved. |

The eventual physical design may use native enums, text with checks, or domains, but consumer values and invariants may not change.

## Classification invariants

- `S`, `E`, `I` map only to `PRESEASON`, with no postseason round.
- `R` maps only to `REGULAR_SEASON`, with no postseason round.
- `F`, `D`, `L`, `W`, `P`, and `C` map only to `POSTSEASON` and their frozen normalized rounds.
- `A` and `N` are recognized special types with null phase, null round, and `SPECIAL_EXCLUDED` status.
- Missing, empty, lowercased, padded, unknown, or conflicting types are not admitted as authoritative.
- Phase never depends on scheduled date, official date, status, market availability, prediction participation, or season expectations.
- A rescheduled or resumed `R` game remains regular season regardless of its eventual date.
- A lookup must match exactly one numeric gamePk; date/team fallback is forbidden.

## Authority and evidence decisions

### One authority row is sufficient

One current row plus immutable retained raw evidence and manifests is sufficient for the scientific and audit requirements. The sidecar answers the operational question “what classification is currently admissible?” Raw files answer “what was observed and when?” Duplicating every observation into PostgreSQL would add maintenance without increasing source fidelity.

### Repeated identical observations

An observation whose core classification matches the authority row is a validation no-op. It remains represented in the retained-source manifest. Ordinary repetition must not update core fields or create another authority row.

### Conflicts

A conflicting source type, source season, or deterministic phase/round mapping must never overwrite core fields. Admission fails; durable source-hashed failure evidence is written by the future caller; the current authority status becomes or remains fail-closed (`CONFLICT_BLOCKED`) through a separately atomic status transition. Consumers must reject that game until review resolves it.

### Null-to-authoritative enrichment

No placeholder row may be created for a missing core type or season. Therefore core null-to-value enrichment is not allowed: admission waits for complete authority.

Evidence-only fields may be enriched from null to a source-supplied value when the core classification is identical and the new raw hash is verified. This applies to raw round description and relationship keys. Non-null-to-different-non-null changes are conflicts, not enrichment.

### Core immutability and corrections

Core fields are immutable under every ordinary acquisition, replay, and consumer operation. A credible provider correction cannot silently overwrite them. It requires a separately authorized correction decision, immutable before/after evidence, a correction hash, and an incremented authority revision. Until approved, status is `CORRECTION_REVIEW_REQUIRED` or `CONFLICT_BLOCKED`, so consumers fail closed.

This governed correction escape hatch means the physical row is not metaphysically immutable, but every normal writer is prohibited from changing core authority.

### Prospective postseason admission

After retaining and hashing new schedule/feed bytes, a producer calls the same frozen classifier and authority admission interface. Known postseason types are admitted with `POSTSEASON` and the normalized round before any downstream phase-dependent consumer may accept the game. Missing or unknown types block that game. No assumption that a game is postseason because of its date is permitted.

### Relationship changes

Relationships are evidence, not phase inputs. New source-supplied keys or null-to-value completion may enrich the JSON evidence with a revision. Contradictory non-null relationship values block automatic enrichment and require review. Every earlier value remains recoverable from retained raw bytes.

## Stable application interface

Consumers target one semantic interface, provisionally `CanonicalGamePhaseAuthority`, with two operations:

- `lookup_exact(game_pk)` returns the complete authority record or a typed failure.
- `require_membership(game_pk, allowed_phases)` returns a record only when authority is unambiguous and its positive phase is in the allowed set.

Required typed failures:

- `GAME_PHASE_ABSENT`
- `GAME_PHASE_SPECIAL_EXCLUDED`
- `GAME_PHASE_CONFLICT_BLOCKED`
- `GAME_PHASE_CORRECTION_REVIEW_REQUIRED`
- `GAME_PHASE_NOT_IN_ALLOWED_MEMBERSHIP`
- `GAME_PHASE_AUTHORITY_STALE`
- `GAME_PHASE_EVIDENCE_HASH_MISMATCH`

Backends:

1. `HashedProposalAuthority`: verifies and reads the pinned 2,919-game proposal plus source manifest.
2. `DatabaseSidecarAuthority`: reads the future canonical view by exact gamePk.

Both return the same field set and failure taxonomy. Consumers must not branch on backend type.

## Consumer rules

- Regular evaluation requires positive `REGULAR_SEASON` membership.
- Postseason evaluation/reporting requires positive `POSTSEASON` membership and a non-null normalized round.
- Absence is an error, never regular season.
- Special, unknown, or conflicting games are rejected.
- Phase attachment occurs after probability computation for predictions.
- Phase may control admission, grading, evaluation, reporting, close, and strictly-prior feature eligibility.
- No consumer may reconstruct phase from a date or silently fall back to a raw nullable field.
