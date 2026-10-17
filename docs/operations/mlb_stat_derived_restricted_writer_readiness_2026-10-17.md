# MLB Stat-Derived Restricted Writer Readiness — 2026-10-17

## Decision

**Status: permission-ready, not yet production-certified.** The existing
restricted login can pass the stat-derived role gate and read the six streak
profile columns. Offline tests cover the writer SQL/result handling and current
team-ID comparison. A rollback-only canary exercised the protected source
writer and an exact player/game derived refresh. No ordinary stat-derived run
has yet completed under the restricted login, so the writer is not ready for
routine routing.

This assessment used production only for read-only role, privilege, RLS, and
restricted-session checks. It performed no acquisition, writer invocation,
data mutation, backfill, grant change, role change, alias change, or scheduler
change.

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

## Recommended bounded production validation

After separate authorization for a normal restricted writer run, choose one
ordinary due date and run the existing stat-derived command once with the
process-scoped opt-in, an exact from/to date, `MLB_STAT_MAX_GAMES=1`, and
`MLB_STAT_SKIP_EXISTING_DATES=0`. Confirm no scheduled stat-derived process is
active before starting and do not overlap the scheduled window. Keep the
existing database aliases and scheduler unchanged.

Before the run, establish the selected exact game and capture read-only
fingerprints for its source row, same-date derived/training rows, and protected
outcomes. After completion, retain the effective writer identity, schedule and
source evidence hashes, per-operation counts, conflict/quarantine evidence,
and committed receipt; compare the same fingerprints and verify no protected
outcome changed. Interpret the one-game cap correctly: the date-wide derived
and rolling synchronization still needs whole-date before/after accounting.
If any step fails, preserve the receipt/error and let the date transaction's
rollback behavior stand; do not broaden privileges or retry in the same run.

This validation is a readiness recommendation only; it was not run here.
