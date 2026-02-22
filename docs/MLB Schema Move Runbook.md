# MLB Schema Move Runbook

This runbook moves MLB-owned relations from `public` to `mlb` while preserving compatibility for existing code paths during transition.

## Scope

Phase 1 moves these base tables to `mlb` and creates same-name compatibility views in `public`:

- `bvp_stats`
- `game_info`
- `model_training_props`
- `player_props`
- `player_derived_stats`
- `player_ids`
- `player_profiles_cache`
- `player_stats`
- `player_streak_history`
- `player_streak_profiles`
- `player_team_by_game`
- `prop_features_precomputed`

Out of scope in phase 1:

- remaining legacy MLB views/materialized views/tables in `public`

## Apply

```bash
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f backend/mlb/sql/migrations/20260221_move_mlb_owned_tables_to_mlb_schema.sql
```

## Verify

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name IN (
  'bvp_stats',
  'game_info',
  'model_training_props',
  'player_props',
  'player_derived_stats',
  'player_ids',
  'player_profiles_cache',
  'player_stats',
  'player_streak_history',
  'player_streak_profiles',
  'player_team_by_game',
  'prop_features_precomputed'
)
AND table_schema IN ('mlb', 'public')
ORDER BY table_name, table_schema;
```

Expected result:

- One `mlb.<table>` base table per name.
- One `public.<table>` view per name.

Optional check:

```sql
SELECT n.nspname AS schema_name, c.relname AS object_name, c.relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'mlb'
ORDER BY c.relkind, c.relname;
```

## Rollback

```bash
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f backend/mlb/sql/migrations/20260221_rollback_move_mlb_owned_tables_to_mlb_schema.sql
```

Rollback behavior:

- Drops `public` compatibility views.
- Moves base tables and owned sequences back to `public`.
- Drops schema `mlb` if empty.

## Notes

- The forward migration grants usage/table/sequence permissions for common Supabase roles when they exist (`postgres`, `anon`, `authenticated`, `service_role`).
- Existing SQL that explicitly references `public.<phase1 table>` keeps working through compatibility views.
- Phase 2 removes compatibility views once code no longer depends on `public.<table>`:

```bash
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f backend/mlb/sql/migrations/20260221_drop_public_mlb_compat_views.sql
```

Phase 2 rollback:

```bash
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f backend/mlb/sql/migrations/20260221_rollback_drop_public_mlb_compat_views.sql
```

## Phase 3: Move Remaining MLB Relations Out of `public`

Phase 3 moves the remaining MLB-owned relations (tables, views, and materialized views) from `public` to `mlb`.

Apply:

```bash
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f backend/mlb/sql/migrations/20260222_move_remaining_public_mlb_objects_to_mlb_schema.sql
```

Verify public is clear of MLB runtime relations:

```sql
SELECT n.nspname AS schema_name, c.relname AS object_name, c.relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'mlb')
  AND c.relkind IN ('r', 'v', 'm')
ORDER BY n.nspname, c.relname;
```

Expected after phase 3:

- `mlb` contains MLB relations (including legacy views/materialized views).
- `public` has no MLB-owned `r/v/m` relations.

Phase 3 rollback:

```bash
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f backend/mlb/sql/migrations/20260222_rollback_move_remaining_public_mlb_objects_to_mlb_schema.sql
```
