# MLB 2026 Game Phase Sidecar Design V1

Status: **DESIGN READY FOR REVIEW; NOT IMPLEMENTED**

This package supersedes the proposed dual-table persistence architecture as a design decision only. It does not authorize or implement database objects, migrations, backfills, consumer changes, scheduler changes, or production activation.

## Governing decision

Authoritative MLB game phase should be represented once, provisionally as `mlb.game_phase_authority_v1`, keyed by exact MLB `gamePk`. A read-only canonical consumer view may expose that relation without sourcing authority from `mlb.game_info` or `mlb_cleanroom_v1.games`.

The rejected design must not be applied:

- no phase columns in `mlb.game_info`;
- no phase columns in `mlb_cleanroom_v1.games`;
- no historical clean-room updates;
- no append-only trigger disabling;
- no union view over duplicated phase authority.

All previously completed migration, preflight, rehearsal, and installation evidence remains preserved. Nothing in this package deletes, edits, or rewrites those assets or their history.

## Priority 0 migration-discovery result

The rejected migration is **not automatically discoverable or runnable** by the repository's current automation:

- there is no Supabase CLI `supabase/migrations` directory;
- no GitHub workflow, Make target, shell script, or Python runner globs or executes `backend/mlb/sql/migrations/*.sql`;
- no general repository migration runner was found;
- tests and validators read the rejected file but do not execute it;
- the activation program requires an explicit migration path, `--execute`, the exact authorization phrase, proposal hashes, target identity, and expected mutation counts.

It remains manually reachable through the preserved activation runbook and guarded loader. That is an operator-discovery risk, not an automatic-application path. The new architecture decision and superseded-assets inventory are the controlling design status pending later implementation.

## Design evidence

- File-backed proposal: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl`
- Proposal SHA-256: `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`
- Proposal rows: 2,919 unique gamePks
- Retained source manifest: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/retained_source_manifest.jsonl`
- Source-manifest SHA-256: `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`
- Source files: 464
- Authoritative observations: 9,092
- Consistent repeat observations: 6,173
- Phase population: 489 preseason, 2,430 regular season, 0 postseason
- Missing, unknown, source-conflict, and duplicate-identity-conflict counts: zero

## Package contents

- `ARCHITECTURE_DECISION.md` — superseding architecture decision and risk classification.
- `SIDECAR_LOGICAL_CONTRACT.md` — fields, invariants, state transitions, and stable consumer interface.
- `AUTHORITY_PROVENANCE_DECISION.md` — why retained raw evidence removes the need for another database ledger.
- `consumer_cutover_map.csv` — evidence-backed consumer-by-consumer cutover plan.
- `TRANSITION_PLAN.md` — file-backed authority, staleness controls, parity proof, and database transition.
- `superseded_assets_inventory.csv` — preserved assets and their revised status.
- `IMPLEMENTATION_SEQUENCE.md` — ordered future work and the smallest justified first task.
- `NON_GOALS.md` — explicit boundaries.

No file in this package is executable SQL.
