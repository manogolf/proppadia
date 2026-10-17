# MLB Restricted Access Cutover Readiness — 2026-10-17

> **Superseding owner decision:** GitHub-hosted MLB roster automation is
> retired. The Mac-based local MLB pipeline is the operational authority, and
> its roster refresh is operational under restricted ingest. The hosted
> workflow was disabled on GitHub and has been removed locally. Historical
> hosted rows and steps below record the earlier assessment only; no hosted
> credential provisioning or cutover remains required.

## Decision

**Readiness: PARTIAL.** The restricted login and role gate are deployed and
verified. The roster path has a rollback-only production canary, and the
stat-derived source plus player-game-scoped derived path has a rollback-only
production canary. No ordinary stat-derived run has yet been certified under
the restricted role, and hosted roster execution remains unverified. The
earlier stat-derived `dict_row` blocker below is stale: current callsites and
focused tests support mapping results. See
`docs/operations/mlb_stat_derived_restricted_writer_readiness_2026-10-17.md`
for the current stat-derived assessment and its date-wide scope. No production
configuration was changed for that assessment.

## Workflow matrix

| Workflow | Current route and required access | Restricted status and evidence | Remaining blocker |
| --- | --- | --- | --- |
| Local roster refresh | `refresh_mlb_players_rosters.py`; shared aliases by default, process opt-in routes through `MLB_INGEST_DATABASE_URL` and sets `proppadia_mlb_ingest`. Fixed roster functions only; no table DML. | **Canary verified.** Taylor Walls identical-value invocation ran in a rollback-only restricted transaction; source values were restored/unchanged. The standalone roster connection uses tuple rows, compatible with its scalar result handling. The writer retries the exact legacy `(player_name, team)` uniqueness collision without the team display label, preserving official player and team IDs. | One ordinary restricted roster invocation remains unobserved. The label-collision fallback has offline regression coverage but has not been exercised against production; its inactive marker is a production-schema no-op. |
| Stat-derived source + derived refresh + training synchronization | `insert_mlb_stat_derived.py` uses `pg_connect(require_streak_read=True)`; process opt-in routes through `MLB_INGEST_DATABASE_URL`. | **Permission-ready; not production-certified.** Production role gate and six-column streak read passed. Max Muncy source and exact player/game derived rollback canary passed with unchanged source, derived, neighboring, and outcome fingerprints. Current result-shape and loader tests pass. | No ordinary restricted run has exercised game metadata, placeholder identity, at-bats repair, training materialization, or normal date-wide derived/training synchronization. A one-game cap does not bound the latter two to one player/game. The scoped validation plan and whole-date accounting requirements are documented in the linked assessment. |
| Plate-appearance backfill | `backfill_mlb_player_pa.py` uses `pg_connect()` and the same process opt-in. Fixed PA-fill function plus approved derived PA columns. | **Code/access compatible; live production write not verified.** The function result explicitly handles mapping rows and tuples. Preview tested fill, replay, conflict rejection, and rollback. | Routine script commits batches and rolling updates; do not use it as a rollback-only canary. A normal bounded run needs a real eligible candidate and separately authorized execution. |
| Game metadata | Called inside stat-derived through `safe_upsert_game_info`; role has function EXECUTE and required `game_info` SELECT. | Preview migration/entry point passed. Current caller uses `scalar_int_result` with `AS result_count`; offline result-shape coverage passed. | No ordinary restricted production invocation observed. |
| BvP/PvB refresh | `refresh_mlb_bvp_pvb.py` uses `pg_connect()` and the process opt-in; fixed feature upsert function. | Preview tests passed; permission gate covers function access and denies direct target DML. Current caller uses mapping-aware scalar result handling with `AS result_count`; offline result-shape coverage passed. | No ordinary due-date restricted production invocation observed. |
| Prediction backfill | `_persist_prediction_backfill` uses `pg_connect()` and the process opt-in; `safe_fill_prediction_backfill` only fills an empty prediction tuple. | Preview protected-outcome tests passed. Function does not update `was_correct`. Current caller uses mapping-aware scalar result handling with `AS result_count`; offline result-shape coverage passed. | No ordinary restricted production invocation observed. |
| Reconciliation, grading, result persistence | Protected realized outcome fields remain behind fixed training entry point; routine result materialization is part of stat-derived. | Preview protections passed; Max Muncy canary confirmed protected training outcome fingerprints unchanged, with training sync not invoked. | No complete restricted production grading/reconciliation invocation was made. Keep exceptional correction and historical maintenance paths on their separately controlled route. |
| Hosted roster workflow (retired) | Historical workflow passed `secrets.SUPABASE_DB_URL` to the full roster command. | Secret value was not inspected. The workflow is disabled on GitHub and removed locally. | No hosted identity verification, credential provisioning, or execution is planned. |
| Other active shared-alias users | Database-backed model training and read/export/scoring utilities use `DATABASE_URL` or `SUPABASE_DB_URL` directly or through SQLAlchemy. | These are not the scoped restricted writer paths; some are read-only, and some have separate write/materialization needs. | Preserve their current route. Do not globally replace aliases; classify and route them individually only when a dedicated read/operations contract exists. |

The earlier scalar-result findings are stale for the callers described above.
Current at-bats, game metadata, placeholder identity, training materialization,
BvP/PvB, and prediction-backfill callers use mapping-aware result handling;
the player-stats source entrypoint validates its one-column mapping shape.
Focused offline tests cover the restricted access contract and result shapes.
Production checks remained read-only and did not invoke these writer functions.

## Protection and continuity notes

- The dedicated login remains narrowly restricted to the ingest group. The
  process opt-in is unset by default; setting it to anything other than `1`
  fails closed. The existing aliases and their fallback behavior are unchanged.
- The database fixed entry points and column/RLS protections are distinct from
  Supabase API-role ACLs. Existing broad `anon`, `authenticated`, and
  `service_role` grants were not changed; actual PostgREST dependencies remain
  unverified. Do not infer that the restricted PostgreSQL login constrains
  service-role API access.
- Exceptional completeness, preseason cleanup, and correction tools are not
  ordinary restricted-writer cutover candidates. Leave them on their
  separately authorized maintenance path.
- The MLB contract's opening status paragraph is stale: it says no login or
  canary exists, while later deployment receipts and the 2026-10-17 Taylor
  Walls and Max Muncy receipts establish login activation and rollback-only
  canaries. This assessment relies on the later receipts.

## Staged cutover and first stage

1. **Stage local roster refresh first**, after separate owner authorization.
   Invoke it once in a controlled window after confirming the scheduled/local
   roster process is not already active and using the reviewed local code
   revision containing the `unique_player_team` fallback. The process command is:

   ```sh
   set -a; source backend/.env; set +a; MLB_USE_RESTRICTED_INGEST=1 make mlb-roster-refresh-all
   ```

   This leaves the file and shared aliases unchanged. Verify on the
   writer session `session_user=proppadia_mlb_ingest_login` and
   `current_user=proppadia_mlb_ingest`; retain team/player counts, function
   results, `team_label_conflicts`, and a source-integrity comparison. Success
   means normal completion, expected coverage, no unexpected identity
   conflicts, and no protected-value differences. A positive
   `team_label_conflicts` count means a display label was omitted while
   canonical player/team IDs were retained. Do not launch a duplicate full
   refresh to test routing.
2. If the restricted run fails, preserve its receipt and allow no further
   restricted retries in that run. Leave existing aliases untouched; the next
   normal invocation returns to the established route by omitting the
   process-only opt-in. Do not change grants to make a failure pass.
3. **Before routing stat-derived, BvP/PvB, or prediction backfill**, correct
   and regression-check scalar result extraction in those callers, including
   the game-metadata, placeholder-identity, at-bats, and training-materializer
   helpers. Accept the named `dict_row` result and retain tuple compatibility
   only where tests require it. Do not change grants or writer semantics.
4. After that correction, authorize and observe one ordinary
   stat-derived scheduled execution under the opt-in. Verify streak reads,
   fixed source/training writes, derived row counts, conflict/quarantine
   receipts, and protected outcomes. Do not include exceptional maintenance.
5. Treat PA, BvP/PvB, and prediction backfill as separate later stages. Use
   their existing command once at a normal due window, only after a real
   eligible candidate or expected empty result is established. For PA, inspect
   its batch commits and only-missing selection; for BvP and prediction,
   verify the corrected result mapping before invocation. Keep old aliases
   until each route has passed.
6. Hosted roster cutover is retired by owner decision; keep the local Mac MLB
   pipeline as the roster authority.

The reversible first-stage control is process-scoped routing, not a global
alias replacement. Do not overlap the one-shot command with a scheduled roster
execution; retain the schedule and let its next run proceed normally. The roster script performs its upserts in one transaction;
an uncaught writer error rolls back that transaction. On failure, preserve the
receipt and omit the opt-in on the next normal run; no database grant or
credential rollback is required. Team-fetch failures are handled by the
existing success threshold and are not themselves database rollback events.
Do not start a second run while the first process or its database transaction
is still active.

## Recommendation

**First workflow:** local roster refresh, because its dedicated route and
restricted role were verified in the single-record rollback-only canary and it
does not require direct table DML. The Mac-based local MLB pipeline is now the
operational roster authority under restricted ingest. The unrelated scalar-result
fixes are prerequisites for stat-derived, BvP/PvB, and prediction-backfill
cutovers, not for roster. Keep all other shared-alias consumers unchanged until
their own identity and compatibility checks are complete.
