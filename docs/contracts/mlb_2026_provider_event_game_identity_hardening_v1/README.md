# MLB 2026 provider-event game identity hardening V1

Status: `VALIDATED_PROSPECTIVE_IDENTITY_HARDENING`

This bounded correction introduces one phase-neutral provider-event binding contract for the general/BetOnline player-prop path and the established Pinnacle h2h/totals/spreads path. A binding now requires a normalized provider event ID, exact oriented teams, one reproducible official schedule candidate after valid start/game-number evidence, canonical exact-gamePk authority, verified provider/schedule source hashes, and no contradictory provider-event reuse. It never selects the first same-team candidate, infers phase from a date, or adds a phase column to a market ledger.

No API/provider request, database connection, pipeline, schedule, model operation, prediction change, publication, or wager was executed to build or validate this package. The production call graph retains the same provider calls and the same already-existing official schedule fetches; the change retains those schedule bytes and binds their hashes. No retained market row was changed.

## Prospective behavior

- `build_mlb_predictions_wide.py` retains the immutable official schedule response and the immutable provider snapshot, resolves each provider event before feature construction, excludes an ambiguous/unproven event, preserves `provider_event_id`, writes a shared immutable JSONL receipt, and passes its identity plus both source hashes into mandatory prospective lineage.
- `capture_mlb_pinnacle_main_markets_v1.py` retains each already-fetched hydrated schedule response, certifies bindings through the shared resolver before ledger writes, and places receipt/schedule/provider provenance on new normalized rows. Existing pregame timing and price parsing remain unchanged.
- Provider snapshots and schedule captures are raw evidence writes. No normalized/derived row is produced for an event lacking a certified receipt.
- The receipt is idempotent for identical input, exclusive/immutable for a new identity, and fail-closed on conflicting content or reused event IDs mapped to different gamePks.

## Retained reconciliation

The offline replay verifies all 464 canonical schedule-source hashes and the 2,919-game authority population. It reconciles 2,976 retained provider-event identities:

| Population | Events | Result |
|---|---:|---|
| Pinnacle main markets | 636 | 636 `RECONSTRUCTABLE_BUT_PREVIOUSLY_UNBOUND`; the exact 636 event/gamePk pairs are unchanged |
| BetOnline/general player props | 2,340 | 2,325 `RECONSTRUCTABLE_BUT_PREVIOUSLY_UNBOUND`, 2 `AMBIGUOUS`, 13 `UNAVAILABLE` |
| Exact blocked ledger | 15 | 2 ambiguous and 13 unavailable; none silently upgraded |

`historical_reconciliation.csv` is a read-only determination. `RECONSTRUCTABLE_BUT_PREVIOUSLY_UNBOUND` does not make an old row provenance-complete and does not authorize backfill. The 15-event `blocked_event_ledger.csv` is the exact offline replay exception population.

## Required classifications

| Classification | Result |
|---|---|
| BetOnline/general player-prop identity integrity | `PROSPECTIVE_READY_EXACT_GAME_PK`; historical lineage remains 2,325 reconstructable/unbound, 2 ambiguous, 13 unavailable |
| Pinnacle identity integrity | `PROSPECTIVE_READY_EXACT_GAME_PK`; existing 636 event/gamePk mappings preserved |
| Schedule-provenance integrity | `READY_HASH_BOUND_IMMUTABLE_SOURCE` prospectively |
| Feature-lineage integrity | `READY_MANDATORY_BINDING_PROVENANCE` prospectively; historical rows are not upgraded |
| Historical lineage status | `PARTIALLY_UNPROVEN_NO_BACKFILL_AUTHORIZED` |
| Doubleheader safety | `READY_FAIL_CLOSED`; missing/invalid time cannot select an ambiguous candidate set and official game number is supported |
| Paid-request impact | `ZERO_ADDED_REQUESTS_ZERO_PAID_CREDITS` |
| Prediction-quality effect | `NONE_IDENTITY_ONLY` |
| Market/ROI effect | `NONE_NO_PRICE_OR_METRIC_CHANGE` |
| Postseason code readiness | `READY_EXACT_GAME_PK_INTERFACE` |
| Postseason operational readiness | `BLOCKED_PENDING_ORDINARY_AUTHORITATIVE_POSTSEASON_CAPTURE` |
| Remaining blockers | 15 historical player-prop events remain unprovable; operational postseason evidence is absent; no historical backfill is authorized |

## Validation

The dependency-free suite executes 22 tests covering unique games, both doubleheader discriminators, missing/invalid time, zero/multiple candidates, event reuse conflict, team reversal/missing identity, rescheduled and resumed identity, timezone boundaries, both evidence hashes, canonical-type conflict, receipt idempotence, Pinnacle verified-write gating, feature-lineage enforcement, the unchanged 636 mapping population, and zero added provider calls. Package validation additionally verifies exact reconciliation populations, manifests, source boundaries, and protected historical status.

Smallest next action: allow one ordinary, already-scheduled capture window to exercise the hardened path; inspect its receipt and blocked-event evidence. Do not activate postseason on synthetic evidence.
