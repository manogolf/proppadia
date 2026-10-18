# NHL prediction-only operational contract V1

## Readiness vocabulary

- `MODEL/FIXTURE_READY`: frozen model identity, scorer parity, and offline fixtures pass.
- `GENERAL_PREDICTION_ONLY_ENTRY_POINT_READY`: a governed reusable command can create a canonical, immutable, strictly pregame snapshot without any provider event, market directory, bookmaker credential, or price.
- `NATURAL_LIVE_CAPTURE_OBSERVED`: the installed 900-second observer has produced a valid immutable snapshot on an eligible live slate.
- `MARKET_ATTACHMENT_READY`: a separately governed downstream market run can attach qualified prices. This is not required for prediction creation.

Before this correction, Points and Saves were only `MODEL/FIXTURE_READY`. Their normal shadow commands required quote-run directories, and their only market-free operational helper was fixed to the September 19 catch-up. They now have general prediction-only entry points. The October 18 comprehensive daily run generated Points, Saves, and SOG predictions, but that does not satisfy `NATURAL_LIVE_CAPTURE_OBSERVED`, which specifically requires a valid immutable observer snapshot. The October 18 SOG MIDDAY observer status explicitly declared `NONE_PREDICTION_ONLY`; it and the associated parent did not bind certified prior history or roster proof. That source remains `CORROBORATED_PREGAME` research evidence only. The 18:42 poll was outside the prediction window and produced no FINAL_PREGAME observer cards. Governed in-window observer acceptance therefore remains pending; see the comprehensive contract and backlog P2.15–P2.17.

## Commands

Points:

```sh
./bin/python backend/nhl/scripts/run_nhl_points_prediction_only.py \
  --season 2026 --slate-date YYYY-MM-DD --phase MIDDAY \
  --observation-timestamp-utc ACTUAL_UTC_TIMESTAMP \
  --canonical-run-identifier GOVERNED_RUN_ID
```

Saves:

```sh
./bin/python backend/nhl/scripts/run_nhl_saves_prediction_only.py \
  --season 2026 --slate-date YYYY-MM-DD --phase MIDDAY \
  --observation-timestamp-utc ACTUAL_UTC_TIMESTAMP \
  --canonical-run-identifier GOVERNED_RUN_ID
```

`FINAL_PREGAME` is the only other accepted phase. The commands reject dates before September 21, 2026. On a nonempty slate, they retain the complete canonical game spine but qualify each game independently at the supplied observation time. Games whose scheduled start has passed are excluded; any still-future game may contribute predictions if its lane inputs qualify. A final durability-time check removes games that start while publication is being prepared, and refuses to publish if no represented game remains pregame. No prediction row is created or relabeled after its game's scheduled start. Repeating the same canonical identity is idempotent; a competing identity for the same slate and phase fails closed.

Each new package records `NHL_GAME_LEVEL_PREGAME_COVERAGE_V1`, the full canonical game IDs and hash, observation-eligible game IDs and hash, the game IDs represented by prediction rows, and a manifest-bound `game_exclusions.csv`. Exclusion reasons distinguish already-started games, games that start before durable publication, missing cutoff-bounded active roster identities, and games with no qualified lane rows. Player-level feature-history and Points ladder exclusions remain in their existing manifest-bound exclusion surfaces. `coverage_status` reports `FULL_SLATE` only when every scheduled game is still eligible and there are no player/input exclusions; otherwise it reports partial coverage. A zero-row lane attempt produces an explicit observer no-op/failure status and no prediction card.

The full, verified, complete, strictly prestart current-slate roster observation remains a publication prerequisite. Game-level coverage does not permit a partial or corrupt roster observation, changed canonical identities, same-day feature leakage, or uncertified prior-day history. Points retains complete coherent ladders for each included player-game; a ladder-ineligible player is excluded from prediction rows. Saves remains conditional-on-start and does not infer a starter when authorized pregame starter evidence is absent. An uncertain starter does not invalidate other eligible goalie rows or games.

Points retains each frozen line-model probability as `raw_prob_over`, then constructs the operational ladder with an equal-weight non-increasing isotonic projection. The line meanings remain `P(points >= 1)`, `P(points >= 2)`, and `P(points >= 3)` for 0.5, 1.5, and 2.5. Projection version and model lineage are recorded in each run; no candidate is emitted when price evidence is absent. Saves retains the complete conditional-on-start distribution. Authorized starter sources include NHL.com projected-lineup articles (`NHL_COM_PROJECTED_LINEUP`) and any separately governed source explicitly supplied to the gate. An NHL.com record qualifies as `PROJECTED` only when it has an exact scheduled game date and matchup, identifies the team, explicitly names one starter, binds that name to exactly one canonical goalie ID on that team, and is captured fresh and before scheduled start. Article URL/identity, content hash, capture time, and publication date or timestamp are retained; date-only publication metadata remains date-only. `CONFIRMED` remains distinct. Ambiguous, stale, conflicting, unauthorized, post-start, or market-only identity cannot qualify a prediction. The policy allowlist does not itself acquire articles; Saves remains blocked on runs without qualifying captured evidence.

Every Points, Saves, and SOG observer validates the immediately prior date's
reconciliation certificate before reading historical feature rows or
publishing a prediction. It also requires the validated current-date schedule
and the complete, strictly prestart immutable roster observation produced by
the daily roster child. The gate is read-only and never starts a second
reconciliation or acquisition path. A missing, incomplete, wrong-date,
corrupt, or non-final certificate produces an explicit failed observer status
and no prediction. Missing, mismatched, incomplete, or post-start roster
evidence likewise blocks publication. A verified empty official prior-date
schedule is the only no-package exception; an empty current slate requires no
roster package and produces no prediction.

The installed 900-second observer continues to invoke these lane entry points
before consulting the market-readiness gate. Each child enforces the
reconciliation certificate itself, so LaunchAgent wake timing and independent
invocation cannot bypass the integrity checkpoint. A lane failure remains
WARN-only for unrelated market capture and cannot create or retry a paid
request. Mainline capture retains its own official-outcome validation before
history export, phase selection, and paid odds access.

## Comprehensive daily runner integration

The operator-facing comprehensive path remains:

```sh
./bin/python -m backend.nhl.cli daily --with-odds
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
`PROSPECTIVE_NOT_BEFORE_2026-09-21`. For September 21 or later, an entirely
absent Points `FINAL_PREGAME` card is classified
`NOT_GRADEABLE_NO_PREDICTION`: reconciliation records a warning and a
schema-valid zero-row Points surface, skips Points grading, and continues the
other reconciliation lanes. This means no valid immutable prediction was
published; it is distinct from a bound prediction whose outcome is
unavailable. A present Points card still undergoes all manifest, identity,
provenance, and strict-pregame checks, and conflicting cards still fail
closed. Saves retains its exact-one prospective-card requirement during direct
reconciliation. In `--prior-learning` mode only, an absent Saves card is
classified `NOT_GRADEABLE_NO_PREDICTION`, emits a schema-valid zero-row
surface, and does not block official-outcome certification; a present invalid
or duplicate card still fails closed. This also covers a legitimate observer
no-op when no game or goalie row qualified before puck drop. No retrospective
prediction or grade is created.

New game-level Points and Saves packages retain the full canonical schedule
spine and bind only prediction rows for games whose scheduled start followed
both the observation and durable publication timestamps. The manifest-bound
`game_exclusions.csv` declares started or otherwise unrepresented games, and
the lane's existing input/player exclusion surface retains finer-grained
reasons. Reconciliation independently recomputes the eligible and started
game sets from the retained schedule and observation timestamp; predictions
for excluded or started games, missing exclusion coverage, mismatched hashes,
or altered manifests remain blocking. Grading joins on game and player/goalie
identity, so missing game/player predictions remain explicit ungradeable or
unpredicted outcomes without manufacturing rows.

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
