# MLB Stat-Derived Restricted Writer Readiness — 2026-10-17

## Decision

**Status: bounded normal production validation passed; fresh-row and changed-row
coverage remain unproven.** The restricted login passed the stat-derived role
gate and completed one exact-date normal invocation. It exercised the source,
training, identity, game-info, and date-wide synchronization paths against
already populated rows. Only the fixed game-info call reported a touched row;
the complete before/after fingerprints were unchanged. This validates the
idempotent populated-row path, but does not establish successful insertion or
protected NULL-fill behavior for new source/training rows.

The initial assessment used production only for read-only role, privilege, RLS,
and restricted-session checks. The separately authorized bounded validation
and its read-only verification are recorded below. No grant, role, credential,
alias, or scheduler change was made.

## Production identity and read-only checks

The independently connected production database reported PostgreSQL 15.8 and
database/session/current identity `postgres`. Both the owner URL and restricted
ingest URL independently resolved to production project reference
`cnwwhhmpashijqbspvhf`, matching the reviewed production pin. Its restricted
login and group matched the reviewed identities:

- `proppadia_mlb_ingest_login`: LOGIN, NOINHERIT, and no superuser, database
  creation, role creation, replication, or RLS bypass attributes.
- `proppadia_mlb_ingest`: NOLOGIN, NOINHERIT, and the same restricted
  attributes.
- The login has exactly one membership, to the ingest group, without admin
  option.
- The restricted `MLB_INGEST_DATABASE_URL` session reported
  `session_user=proppadia_mlb_ingest_login` and
  `current_user=proppadia_mlb_ingest` after role activation.
- `configure_mlb_ingest_connection(..., require_streak_read=True)` passed.
  The required six-column streak SELECT also succeeded as a zero-row
  readback. RLS limits visible streak records to `prop_source='mlb_api'` and a
  non-NULL `last_game_date`.

The production role gate confirmed MLB schema USAGE without CREATE, the fixed
function permissions, and the source, outcome, adjacent-table, and destructive
write boundaries. RLS is enabled and not forced on `player_stats`,
`player_derived_stats`, `model_training_props`, and
`player_streak_profiles`; the game metadata relation is not RLS-enabled.

## Normal execution path and exact write dependencies

The ordinary path is `make mlb-insert-stat-derived`, which invokes
`backend/mlb/scripts/insert_mlb_stat_derived.py`. `make mlb-stat-derived-refresh`
adds a separate recent-volume guard after the writer. The script obtains a
schedule, then a live feed and boxscore for each selected playable final game.
No provider request was made during this assessment.

| Relation | Normal read/write dependency | Restricted write boundary and reported result |
| --- | --- | --- |
| `mlb.game_info` | Reads existing game IDs; writes seven schedule fields for selected final games. | `SELECT` plus `EXECUTE mlb.safe_upsert_game_info(jsonb)`. The scalar is aliased `result_count` and read through `scalar_int_result`. No direct table DML. |
| `mlb.player_ids` | Does not read the relation; ensures the foreign-key player identity exists before source writes. | `EXECUTE mlb.safe_insert_player_id_if_absent(jsonb)`. The scalar is aliased `result_count`. No direct table SELECT or DML. |
| `mlb.player_stats` | Reads exact player/game rows and date/history populations; writes official source observations and may repair missing/zero batter at-bats. | `SELECT` plus `safe_upsert_player_stats(jsonb)` and `safe_fill_player_stats_at_bats(...)`. The source upsert currently has one scalar result column without an explicit alias; its caller validates a one-key mapping or tuple. At-bats uses `result_count`. Direct INSERT/UPDATE/DELETE/TRUNCATE are denied. Populated source disagreements are quarantined; NULL-fill/replay is idempotent. |
| `mlb.player_streak_profiles` | Reads the six required streak fields to construct strictly prior training metadata. | Column-only SELECT plus the row-limited SELECT policy. No write privilege. The live restricted readback and startup gate passed. |
| `mlb.model_training_props` | Reads an exact player/game/property/source result tuple before materializing synthetic training observations; reads rows for date-wide rolling synchronization. | `SELECT`, `safe_upsert_model_training_props(jsonb)` with scalar alias `result_count`, and direct UPDATE limited by column ACLs to metadata/rolling fields. Training result columns stay protected by the fixed function's equality guard and app conflict quarantine. INSERT/DELETE/TRUNCATE are denied. |
| `mlb.player_derived_stats` | Reads source and existing rolling rows, locks identity neighborhoods, then recomputes rolling features. | Direct INSERT/UPDATE only on the allowlisted identity/context/rolling columns under RLS and column ACLs. DELETE/TRUNCATE are denied. The write count is accumulated from cursor row counts. |

The separate rolling synchronization returns `COUNT(*) AS n`; its caller reads
`n` for the reported count. It changes only `team`, `rolling_result_avg_7`,
and `updated_at` on eligible existing `mlb_api` training rows, and does not
replace the protected outcome tuple.

The fixed entry points used by this path are SECURITY DEFINER functions owned
by `postgres`; production reported EXECUTE for the restricted group on
`safe_upsert_player_stats`, `safe_fill_player_stats_at_bats`,
`safe_upsert_model_training_props`, `safe_insert_player_id_if_absent`, and
`safe_upsert_game_info`. The access helper also checks the wider reviewed
function allowlist. Direct writes to `game_info`, `player_ids`, and
`player_stats` remain denied.

## Date scope and transaction behavior

The writer stages source observations for the selected exact games, but its
normal derived-feature refresh and training synchronization are per affected
official operational date:

1. `_refresh_player_derived_stats(conn, date, date)` reads the date's source
rows and recomputes legacy player/date rolling rows for every eligible player
on that date. It writes the resulting rows through direct, column-limited
INSERT/UPDATE. It does not delete rows. Player/date collisions with a different
exact game are rejected before derived writes.
2. `_sync_training_rows_rolling_result_avg(conn, date, date)` can update
existing eligible training rows for that whole date by joining derived
features. It is not limited to the selected source game.

Therefore `--max-games-per-date=1` bounds selected source-game ingestion, but
does not bound the downstream derived refresh or rolling training sync to one
game or player. Both run in the same per-date transaction as game metadata,
player identities, source stats, and training-row materialization. An
exception before commit rolls the date transaction back. The script commits
before publishing its immutable success receipt, so a post-commit receipt
publication failure cannot undo committed database changes.

## Existing canary evidence and remaining gaps

The earlier Max Muncy preparation receipt was blocked because its app-side
comparison treated stored numeric team IDs and incoming abbreviations as
different. Current `_team_identity_token` compares both via the canonical team
map; this is an offline comparison-only fix and does not rewrite either value.
The resumed rollback-only canary then passed under the restricted writer
identity. It invoked `safe_upsert_player_stats` and an exact player/game
derived refresh; both reported zero changed rows. Explicit rollback followed,
and source, derived target, date-neighbor, player-other-game, and training
outcome fingerprints matched before and after. That canary intentionally did
not invoke date-wide training synchronization.

The canary does not establish normal-path compatibility for game metadata,
placeholder identity, at-bats repair, training materialization, or date-wide
derived/training synchronization. It also does not establish committed row
counts from an ordinary run. No current production SELECT or direct-write gap
was found for the normal stat-derived dependencies, and the restricted role
gate passed.

Focused offline verification on 2026-10-17:

- Restricted access contract, writer result-shape, and active-loader tests:
  **53 passed**.
- Root-loader correction and degraded-stage containment tests:
  **44 passed**.
- Total: **97 passed**.

An additional legacy test path under `backend/tests/test_shared_insert_mlb_stat_derived.py`
did not collect because it imports the removed `backend.scripts` module path;
`./bin/check-python-env` passed, and the canonical MLB-scoped tests above
collected and passed.

## Bounded production validation — 2026-10-17

The one authorized normal invocation used the existing
`backend/mlb/scripts/insert_mlb_stat_derived.py` entry point and the unchanged
production aliases. Its exact range was 2026-10-15 through 2026-10-15, with
`MLB_USE_RESTRICTED_INGEST=1`, `MLB_STAT_MAX_GAMES=1`, and
`MLB_STAT_SKIP_EXISTING_DATES=0`. It ran through `./bin/python` with the script's
one-game cap. No full MLB wrapper or other writer was invoked. The normal daily
launcher had already completed its stat-derived stage; its only remaining
child was local artifact indexing, with no active stat-derived/database writer.

The official schedule contained two final postseason games. The fresh run
selected game **849810 (NLCS Game 4, Milwaukee Brewers at Los Angeles Dodgers)**
and recorded game 849818 as `REJECTED / NOT_PROCESSED` because of the one-game
cap. The selected game was final (`gameType=L`, official date 2026-10-15). The
schedule SHA-256 was
`9aad711bfd60845932316dc2b1e5ee63231aa44c07fe6569c55bda7b9ff4fb85`; the
selected live-feed SHA-256 was
`6a56bb88fe8f1f32efc4168e8896b3bf8cf753ce4617a02047ed433b4559ce58`. The
feed contains the official boxscore data used by the normal loader.

The writer's `pg_connect()` path enforces the process opt-in, pins the
restricted URL to the production project, activates the ingest group, and
asserts `session_user=proppadia_mlb_ingest_login` plus
`current_user=proppadia_mlb_ingest` and the reviewed privilege boundaries.
A post-run read-only connection through the same route passed that gate and
recorded those exact identities. The writer committed with
`COMMITTED_AFTER_COUNT_RECONCILIATION` and exited 0.

The full before/after row snapshots used the existing `SUPABASE_DB_URL` in
repeatable-read, read-only PostgreSQL transactions; both session and current
user were `postgres`. The existing moneyline audit read identity is scoped to a
different relation and cannot supply these snapshots. No read grant or role was
changed; the owner connection's transaction was explicitly read-only.

| Operation | Writer-reported result | Full-date before/after |
| --- | ---: | --- |
| `safe_upsert_game_info(jsonb) AS result_count` | 1 selected-game row result | 2 rows; fingerprint unchanged |
| `safe_insert_player_id_if_absent(jsonb) AS result_count` | 0 inserts | Selected game's post-run identity set: 46 rows |
| `safe_upsert_player_stats(jsonb)` | 46 admitted, 0 changed, 6 quarantined | 91 rows; fingerprint unchanged |
| `safe_upsert_model_training_props(jsonb) AS result_count` | 189 admitted/unchanged, 0 changed, 81 quarantined | 368 rows including outcomes; fingerprint unchanged |
| Date-wide `player_derived_stats` refresh | 0 changed | 91 rows; fingerprint unchanged |
| Date-wide `rolling_result_avg_7` synchronization | 0 changed | Training outcome fingerprint unchanged |
| `safe_fill_player_stats_at_bats(...) AS result_count` | 0 backfills | Source fingerprint unchanged |

The six source quarantines were `NO_BATTING_OR_PITCHING_STATS`; 81 training
quarantines were `STAT_VALUE_MISSING`. There were no source-stat or protected
training-outcome conflicts. The entire October 15 `player_stats`,
`player_derived_stats`, and `model_training_props` rows matched before and after.
The protected training values (`prop_value`, `line`, `over_under`, `outcome`,
`status`, and `was_correct`) are included in the complete-row fingerprint.
Prior-30-day player-stat, derived, and training neighborhoods also matched.

The before-state capture's separate `player_ids` fingerprint was initially
scoped to game 849818 based on the prior routine receipt; the fresh run selected
849810. The before-state still fingerprints all 91 exact-date source rows,
including their player IDs. The insert-if-absent fixed function reported zero
inserts, and the post-run 849810 identity set contains 46 rows. Thus no identity
row was changed, but the separately scoped pre-run `player_ids` fingerprint
does not independently cover 849810. This limitation is retained in the
validation evidence rather than presented as a direct identity hash comparison.

The retained receipt is
`artifacts/ops/mlb_stat_derived_restricted_validation_2026-10-17/natural_receipts/restricted-stat-derived-validation-20261017T131300-849818/2026-10-15/receipt.json`
(SHA-256 `74ded511c8f1baa1eba75b20188d911078cf87ba6a82ccc526c58eca4cbe62d0`).
The repeatable-read snapshots, comparison, restricted-session verification,
and invocation output are under
`artifacts/ops/mlb_stat_derived_restricted_validation_2026-10-17/`.

## Remaining validation need

The remaining blocker for full routine certification is a normal restricted
run whose selected official source yields an actual `player_stats` or training
row insert or protected NULL fill. The October 15 run proved the already
populated/idempotent path and date-wide synchronization, with zero net row
changes. Keep routine aliases and scheduler unchanged until that evidence is
available; do not force a new or historical source row merely to manufacture a
write.
