# MLB 2026 prospective exact-game feature shadow writer V1

This activation adds a file-backed, shadow-only producer for exact
`(player_id, game_pk, MLB_PLAYER_GAME_FEATURE_STATE_V1)` feature states. It
does not create a database relation, replace `mlb.player_derived_stats`, or
change any prediction, scoring, publication, or wagering consumer.

## Natural boundary

The installed daily wrapper invokes the hook after a successful current-slate
roster refresh and before BvP, stat-derived refresh, predictions-wide, and all
player scoring. This is the earliest boundary at which both the immutable
current-run schedule and refreshed roster exist. The earlier Moneyline lane is
game-level and has no dependency on player/game feature state.

Each invocation establishes one PostgreSQL snapshot cutoff using an explicit
`REPEATABLE READ READ ONLY` transaction. It never requests a transaction ID
and performs no write. Rows observed after the cutoff, games at or before
start, unsupported phases, missing exact identity, non-playable schedule
states, and missing/stale source provenance fail closed.

## Authority and grain

- Current games come from the one immutable, hash-verified history schedule and
  selection receipt created after the current wrapper start. Discovery scans
  only the create-only dated source directory and fails unless the current-run
  source is unique; no mutable `latest` receipt participates.
- Phase membership comes from the active V1-pinned
  `CanonicalGamePhaseAuthority` file backend.
- Current players come from the refreshed `mlb.player_ids` rows in the same
  read-only snapshot.
- Strict-prior exact facts come only from 2026 `mlb.player_stats` rows with an
  exact game ID and a retained observation time at or before the cutoff.
- The offline candidate builder enforces exact event chronology. Doubleheader,
  makeup, split, and resumed-game identities are never collapsed by date or
  `MAX(game_id)`.
- `mlb.player_derived_stats` is neither read nor used as fallback.

## Immutable storage

Natural output is create-only beneath:

`artifacts/ops/mlb_exact_game_feature_shadow_v1/<slate-date>/<run-identity>/`

Each directory contains `snapshot_receipt.json`,
`admitted_candidates.csv`, `rejected_candidates.csv`,
`source_manifest.jsonl`, `summary.json`, and `SHA256SUMS.json`. Directories are
mode 0700 and files mode 0600. There is no `latest` pointer. An identical
existing run is a success; a conflicting reuse of a run identity fails closed.

## Failure and production isolation

The hook emits visible START/DONE/WARN log lines. A shadow failure does not
suppress independently valid production stages and cannot authorize a stale or
legacy fallback. No production barrier or reader consumes the package. Network
requests and paid credits attributable to this writer are fixed at zero.

The known Totals boundary `OFFICIAL_FINAL_SOURCE_COUNT_824462_2` is unrelated
and unchanged.

## Validation and activation

Focused tests cover ordinary games, doubleheaders, 824785/824784,
resumed-game 824912, chronology, missing identity, phase partitioning,
conflicting duplicates, create-only idempotency, and semantic prohibitions.
The existing exact-game foundation, BvP, Moneyline finality, and stat-derived
containment suites are also run to prove isolation.

The installed wrapper changed only by the fragment recorded in
`installed_wrapper.patch`. Generated daily shadow packages remain untracked.
The first natural post-commit window is observed only if it occurs while this
activation task is still in progress; it is never manually invoked.
