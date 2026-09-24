# MLB stat-derived containment receipt fingerprint correction V1

Required ancestry `eba0964f3647a71db86d861a80ddc2eab8f72723` is present. This correction changes only the retained failure-stream capture and receipt classifier. It does not repair or rerun stat-derived ingestion.

## Stream capture

The installed wrapper still invokes `make mlb-stat-derived-refresh` once and preserves its exit code. Zsh `MULTIOS` mirrors stdout and stderr to their existing LaunchAgent destinations while independently retaining complete per-run files under:

`artifacts/ops/mlb_stat_derived_stage_output_v1/<slate-date>/<run-identity>/{stdout,stderr}.log`

The directory is mode `0700`; stream files are created under `umask 077`. Per-stream ordering is preserved. The classifier does not invent a cross-stream order: its combined-output hash is a length-delimited ordered pair of complete stdout and stderr bytes.

## Fingerprint V2

`MLB_STAT_DERIVED_FAILURE_FINGERPRINT_V2` parses complete raw streams before sanitization, truncation, or Make-error selection. It records exception class, constraint, player, game, database detail, stage exit, raw stream hashes, a canonical stream-pair hash, a stable structured fingerprint, paths, byte counts, and bounded sanitized excerpts.

Unknown failures retain `UNCLASSIFIED_STAT_DERIVED_NONZERO` and no guessed identity. Historical V1 receipts remain unchanged and verifiable under their original bytes.

## September 24 retrospective proof

Minimal byte-exact excerpts were taken only from the retained natural stdout and from stderr after the `local_daily_20260924T180004Z` boundary. Offline replay produces:

- classification: `UNIQUE_VIOLATION_PLAYER_DERIVED_STATS_PLAYER_GAME`
- exception: `UniqueViolation`
- constraint: `player_derived_stats_player_id_game_id_key`
- player: `453286`
- gamePk: `824785`
- boundary: `true`
- exit: `2`
- stable structured fingerprint: `51f32d3b48a31d98c938cf14358fb8b97696ed72556d7db6fac2193ebaa51fa6`

The historical operational receipt was not rewritten; its SHA-256 remains `3a6a28105d1ac93cf0c26a5472b1f7a34fd2a8c59de2609456034e9f187924ed`.

## Invariance

Stage count, independent continuation, dependent/unknown skipping, original wrapper exit, claims and credit eligibility, BvP behavior, locks, EXIT trap, completion checkpoint behavior, exact-game foundation, and legacy database state are unchanged. Tests and validation use retained or synthetic files only. API requests, database connections, pipeline runs, migrations, and credits are zero.

The separate Totals defect `OFFICIAL_FINAL_SOURCE_COUNT_824462_2` remains unresolved. The stat-derived root defect remains `PRESENT_NOT_RESOLVED`; natural V2 receipt validation awaits the next natural failure.
