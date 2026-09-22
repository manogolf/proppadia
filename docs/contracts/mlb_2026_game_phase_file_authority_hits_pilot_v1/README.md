# MLB 2026 Game Phase File Authority Hits Pilot V1

Status: **VALIDATED; LOCALLY IMPLEMENTED; DATABASE NOT USED**

## Scope

This pilot implements the backend-neutral `CanonicalGamePhaseAuthority` interface with the exact hashed 2026 proposal backend and cuts over only `backend/mlb/scripts/evaluate_hits_model_candidates.py`.

It does not implement the database sidecar, change another production consumer, alter model features or scoring, write predictions, connect to a database, call a provider, or run a pipeline.

## Authority identity

- Proposal: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl`
- Proposal SHA-256: `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`
- Proposal population: 2,919 unique gamePks
- Source manifest: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/retained_source_manifest.jsonl`
- Source-manifest SHA-256: `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`
- Retained sources: 464 files and 9,092 observations; every retained file is byte/hash verified at load
- Frozen classifier: `backend/mlb/season_transition/contract_v1.py`
- Classifier identity: `MLB_2026_REGULAR_SEASON_CLOSE_AND_POSTSEASON_DATA_PLAN_V1`, version `contract_v1`
- Classifier SHA-256: `eed52e24d123c24e0fbbff21249705d653d77787eab98eb756d5c9752a481c19`
- Authority counts: 489 preseason and 2,430 regular season; zero missing, unknown, conflicting, or duplicate identities
- Supported file-authority window: 2026-02-20 through 2026-09-27

The file backend fails stale outside that window. Dates establish coverage only; they never classify phase.

## Consumer change

The old database predicate admitted a missing type as regular season:

`COALESCE(NULLIF(upper(trim(to_jsonb(m)->>'game_type')), ''), 'R') = 'R'`

The evaluator now fetches its otherwise unchanged rows and applies exact-gamePk authority before scoring. Regular-season admission requires positive `REGULAR_SEASON` membership. It reports:

- admitted regular-season rows;
- excluded preseason rows;
- excluded postseason rows;
- blocked missing gamePk, absent authority, special type, unknown type, conflicting type, or stale authority.

Any blocked row aborts evaluation after writing the gate report. Expected preseason/postseason rows are excluded. The scoring, metric, decile, threshold, and cohort functions retain their pre-pilot AST identities.

## Offline retained-input result

The frozen retained Hits ledger contains 7,564 rows over 651 exact gamePks from 2026-05-08 through 2026-08-02. All 651 gamePks have positive authoritative regular-season membership.

Before and after results are identical:

- rows: 7,564 / 7,564;
- distinct gamePks: 651 / 651;
- newly excluded or blocked gamePks: none;
- prediction/pick hash: `a0337d0450c05db5af8800db903d4c8458858d79197a9bdefa91d99a35434753` / identical;
- outcome hash: `204603647e10cfb937aeda6ed806641e3d833e9c4fb921a3a83019d3b3b67d22` / identical;
- cohort hash: `f5c8663a1abfd744b8da4b5e44d096b9d2bb5a8ca61f0bc0a5aaa7f18c455c40` / identical;
- probability invariance: pass;
- all recorded evaluation metrics: identical.

## Validation

- Pilot dependency-free `unittest` runner: 13 passed, 0 failed, 0 skipped.
- Existing canonical dependency-free runner: 25 passed, 0 failed, 0 skipped, 0 unexecuted; existing 13-check validator passed.
- Source-completion validator: 14 checks passed, 0 failed.
- Pilot offline validator: 9 checks passed, 0 failed.
- Network requests: 0.
- Database connections and writes: 0.

## Cutover conclusion

The pilot justifies a separately reviewed, one-consumer-at-a-time cutover for consumers whose exact gamePks and complete evaluation window are covered by a verified authority package. It does not justify a prospective postseason consumer cutover while this proposal contains no postseason games or any consumer window beyond 2026-09-27.
