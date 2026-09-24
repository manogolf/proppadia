# NHL prediction-only operational contract V1

## Readiness vocabulary

- `MODEL/FIXTURE_READY`: frozen model identity, scorer parity, and offline fixtures pass.
- `GENERAL_PREDICTION_ONLY_ENTRY_POINT_READY`: a governed reusable command can create a canonical, immutable, strictly pregame snapshot without any provider event, market directory, bookmaker credential, or price.
- `NATURAL_LIVE_CAPTURE_OBSERVED`: the installed 900-second observer has produced a valid immutable snapshot on an eligible live slate.
- `MARKET_ATTACHMENT_READY`: a separately governed downstream market run can attach qualified prices. This is not required for prediction creation.

Before this correction, Points and Saves were only `MODEL/FIXTURE_READY`. Their normal shadow commands required quote-run directories, and their only market-free operational helper was fixed to the September 19 catch-up. They now have general prediction-only entry points. `NATURAL_LIVE_CAPTURE_OBSERVED` remains pending until the next nonempty pregame slate on or after September 21, 2026.

## Commands

Points:

```sh
.venv/bin/python backend/nhl/scripts/run_nhl_points_prediction_only.py \
  --season 2026 --slate-date YYYY-MM-DD --phase MIDDAY \
  --observation-timestamp-utc ACTUAL_UTC_TIMESTAMP \
  --canonical-run-identifier GOVERNED_RUN_ID
```

Saves:

```sh
.venv/bin/python backend/nhl/scripts/run_nhl_saves_prediction_only.py \
  --season 2026 --slate-date YYYY-MM-DD --phase MIDDAY \
  --observation-timestamp-utc ACTUAL_UTC_TIMESTAMP \
  --canonical-run-identifier GOVERNED_RUN_ID
```

`FINAL_PREGAME` is the only other accepted phase. The commands reject partial or post-start slates and all dates before September 21, 2026. Repeating the same canonical identity is idempotent; a competing identity for the same slate and phase fails closed.

Points preserves every frozen model output, applies the existing ladder gate, and emits no candidate when price evidence is absent. Saves emits the complete conditional-on-start distribution while leaving starter identity unknown; it selects no starter or candidate without separately authorized evidence.

The installed 900-second observer invokes Points, Saves, and SOG prediction-only lanes before consulting the market-readiness gate. A lane failure is WARN-only and isolated. It cannot create or retry a paid request.

## Comprehensive daily runner integration

The operator-facing comprehensive path remains:

```sh
.venv/bin/python -m backend.nhl.cli daily --with-odds
```

Its schedule, roster, feature, and prediction stages are independent of odds.
Predictions are made durable before the optional governed odds observation and
market attachment. Omitting `--with-odds` makes no odds-provider request. A
successful empty or unmatched market observation cannot suppress or erase
Points, Saves, or SOG outputs, and each market builder accepts only the
explicit run-bound immutable observation rather than falling back to mutable
`latest` data.

The append-only odds/roster evidence contracts and the unactivated
first-puck-aware phase planner are defined in
`NHL Comprehensive Daily Capture Contract V1.md`. Nothing in that contract
changes the installed 900-second observer or activates a schedule.

## Postgame immutable-source binding

Postgame reconciliation resolves existing immutable operational artifacts in
place. For each slate it requires exactly one `FINAL_PREGAME` cross-market run
for Moneyline/puck line and exactly one `FINAL_PREGAME` SOG prediction-only run.
Before creating a request run or accessing the database or network, local-only
validation verifies manifests and file hashes, slate and canonical game
identities, schemas and row counts, pre-puck observation times, duplicate and
conflict absence, and recomputed immutable prediction identities. The
`--local-input-preflight` mode performs this validation with zero database and
network access.

Beginning with the September 21 prospective boundary, postgame binding also
fully verifies the unique Points and Saves `FINAL_PREGAME` publications:
manifest, run/date/game identity, schema, prestart timestamps, natural keys,
prediction identities, model ladder and metadata cardinalities. Market
attachment remains optional and has no effect on grading.

Points rows join authoritative outcomes only on `(game_id, player_id)`.
Participating rows settle the three half-point lines from official goals plus
assists; nonparticipants, source exclusions, missing prospective predictions
and unresolved rows remain explicit surfaces.

Saves rows retain their conditional-on-start meaning. Official boxscore team
membership and a unique maximum official TOI select exactly one starter per
team. Only those starters settle the thirteen-line ladder. Predicted
nonstarters, relief appearances, unpredicted starters and other unpredicted
goalie outcomes remain separate. A tie, missing team, unusable TOI or identity
conflict fails closed.

Authoritative staging contract `NHL_AUTHORITATIVE_STAGING_SYNC_V2`, preflight
`NHL_AUTHORITATIVE_STAGING_PREFLIGHT_V3`, and correction authorization
`NHL_AUTHORITATIVE_STAGING_CORRECTION_AUTHORIZATION_V3` derive all skater,
goalie and starter cardinalities from the verified official response set.
They retain exact natural-key equality, date/game write scoping, locking,
rollback and set-difference deletion gates without assuming a seven-game or
36-skater/four-goalie shape.

September 19 retains its completed catch-up binding and package identity. No
compatibility copy is created for September 20. Because Points and Saves were
not prospectively active before September 21, September 20 publishes
schema-valid zero-row grading surfaces with, respectively,
`NO_PROSPECTIVE_POINTS_PREDICTIONS` and
`NO_PROSPECTIVE_SAVES_PREDICTIONS`, both carrying reason
`PROSPECTIVE_NOT_BEFORE_2026-09-21`. Missing Points or Saves sources on
September 21 or later fail closed unless a separately approved contract says
otherwise.

Only retained prospective SOG rows are graded. D is reported as the champion;
A, B, C, and G remain separate operational-shadow populations; F, if present,
is `UNQUALIFIED_SHADOW_DIAGNOSTIC_ONLY`. Arms are never pooled. Participating
skaters without a prospective row are explicitly excluded and no retrospective
prediction is manufactured.

Postgame staging repair is not prediction creation and cannot continue into
grading or publication. The separate `--staging-set-preflight` and
`--correct-staging-set` modes consume only verified preserved official
boxscores, create no retrospective predictions or prices, and return before
shift/play-by-play acquisition, prediction grading, package publication or any
model-promotion path.

## SOG arm E clarification

`E_TEAM_CHANGE_AWARE` is historical diagnostic-only. It is not in the operational frozen shadow set. The operational set remains selected D with A/B/C/G comparators; no September 20 E rows are added or reconstructed.

## Rollback

Revert the implementation commit to remove the general entry points and observer integration. Retain all immutable prediction artifacts already created; never delete or rewrite them. The existing 900-second schedule does not change.
