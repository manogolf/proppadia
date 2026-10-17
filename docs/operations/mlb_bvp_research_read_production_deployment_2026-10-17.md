# MLB BvP Research Read Production Deployment — 2026-10-17

## Result

The reviewed migration committed successfully on the MLB production database.
The transaction output was `BEGIN`, `DO`, `GRANT`, `COMMIT`. No rollback was
run because the migration completed and the post-deployment checks passed.

## Target identity

- Production Supabase project reference: `cnwwhhmpashijqbspvhf`
- Preview project reference independently established during preview testing:
  `vbnzmvmhuwvpkijxeuy`
- Database: `postgres`
- PostgreSQL: `15.8`
- Owner route identity: `session_user=current_user=postgres`
- Research route identity: `session_user=current_user=proppadia_mlb_research_login`
- Existing membership: research login is a member of
  `proppadia_mlb_research_ro`; group is `NOLOGIN`; both roles have safe
  non-superuser attributes and inherit privileges as expected.

The configured owner and research routes named the production project
independently of the preview project. The preflight was read-only and confirmed
the migration's role, membership, schema usage, relation, RLS, policy, column,
and existing SELECT-scope assumptions before applying the migration.

## Exact migration and rollback

- Migration:
  `backend/mlb/sql/migrations/20261017_mlb_bvp_research_read.sql`
- Migration SHA-256:
  `378ce8f6b527bc93b8ed3ab32d050ea920108b66aaaa29bcceb79f45dbe3aaf7`
- Rollback:
  `backend/mlb/sql/migrations/20261017_mlb_bvp_research_read_rollback.sql`
- Rollback SHA-256:
  `38e3178aaa97776797883480456692735275cbde09e126be2fca44e0290f98a3`
- Migration result: committed
- Rollback disposition: not run; successful migration retained

The grant is column-level `SELECT` for `prop_type`, `player_id`, `game_id`,
`game_date`, `features`, `feature_set_tag`, `model_tag`, and `computed_at` to
the existing `proppadia_mlb_research_ro` group. No role, membership, schema,
RLS, policy, or ingest permission was changed.

## Permission verification

The research login read the 286 rows from the retained 2026-10-17 BvP
post-commit snapshot. The composite identity set and all eight selected values
matched row for row. The canonical readback SHA-256 and retained snapshot
SHA-256 both equal:

`4248bb6dccbbbb5948f26c62f9d5f127d2d537be62d8491a3f642a6499b590b2`

Verified denials and scope:

- Table-wide SELECT on `mlb.prop_features_precomputed`: denied.
- SELECT on each of the eight approved columns: granted to the group and
  effective for the research login.
- SELECT on `lineup_slot`: denied by ACL and direct SQL probe.
- SELECT on `mlb.player_stats`: denied by ACL and direct SQL probe.
- Direct INSERT, UPDATE, DELETE, and TRUNCATE: no table privileges; no
  column-level INSERT or UPDATE privileges.
- `proppadia_mlb_ingest`: no table-wide or column-level SELECT on the target.
- Target relation: ordinary table owned by `postgres`, RLS disabled, FORCE RLS
  disabled, no policies.

Because the target has no RLS policies, the eight-column grant permits access
to all rows through those columns. The `features` column returns its complete
JSONB value. This is the reviewed migration's scope.

The 286-row comparison used the retained snapshot at
`artifacts/ops/bvp_inline_v1/validation/2026-10-17/after_state.json`; the
comparison was read-only. No BvP acquisition or writer was rerun.

## Next step

The BvP reader permissions and retained 286-row readback are verified for the
next normal certified run. That ordinary due-date run remains pending and is
required to close backlog item P1.2; the prior 286-row write is already
retrospectively certified and does not need a rerun.
