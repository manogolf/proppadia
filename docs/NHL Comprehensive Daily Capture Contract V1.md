# NHL comprehensive daily capture contract V1

## Canonical operator entry point

The canonical comprehensive command is:

```sh
./bin/python -m backend.nhl.cli daily --with-odds
```

`bin/nhl_ops.sh daily` is a wrapper for that module command. Git history
retains `backend/nhl/scripts/nhl_all.sh` as a removed predecessor. The reported
historical `backend/nhl/scripts/cli.py` path is absent from the current tree and
has no retained tracked Git object. Neither path is an independent current
implementation. There must be only one evolving daily graph.

Operational NHL dates use `America/Los_Angeles`; durable timestamps use UTC
and, where useful for review, their Pacific representation. The season is
identified by its single starting year, such as season `2026`.

Season derivation and authorization (`backend/nhl/game_phase.py`): a game's
season is the first four digits of its game ID; a date belongs to the season
starting that year from July onward, so January–June 2027 is season 2026
(`season_for_date`, `season_for_game_id`). Production lanes derive every
`season=<s>` path, season query, canonical-season gate and metadata value from
the slate date, and require it to be in `AUTHORIZED_SEASONS` (currently
`{2026}`). A date in any other season fails with
`NHL_SEASON_NOT_AUTHORIZED:<s>` instead of silently finding no artifacts.
Rolling over to a new season is a deliberate change: decide which models carry
over or requalify, record the new season's cross-market activation, and add the
season to `AUTHORIZED_SEASONS`. Frozen model artifacts and their training data
(for example `evaluation_season=2025` paths) stay pinned by design.

The comprehensive graph is:

1. database sanity;
2. prior-date official outcome acquisition, reconciliation, and certification;
3. current-date schedule acquisition and canonical-slate validation;
4. current-slate roster capture and player normalization;
5. full-league roster refresh;
6. prior-date log, shift, manpower, and feature finalization;
7. feature construction, prediction scoring, and prediction durability;
8. optional governed odds observation, only with `--with-odds`;
9. optional market attachment from that run-bound observation;
10. research refresh, archival, integrity checks, and final sanity counts.

## FINAL_PREGAME timing and game-level finalization review (2026-10-18)

Current observer behavior has two timing layers. AUTO phase selection for
mainline, SOG, Points, and Saves uses the earliest unstarted game's start time:
the lane selects `FINAL_PREGAME` only when that next game is 20–75 minutes
away (with lane-specific MIDDAY/no-phase behavior outside the window). Once a
phase is selected, prediction eligibility and final durable-write checks are
per game. Thus a later game's timing can open a FINAL_PREGAME poll after an
earlier game starts, while rows for the started game are excluded.

The immutable publication identity remains slate-and-phase scoped. SOG stops
if a run already exists for the phase. Points and Saves return the existing
canonical run idempotently, and competing phase runs fail closed. Mainline's
ordinary observer suppresses a repeated phase, while forced/refresh behavior
can create separate states. Direct reconciliation resolves exactly one
cross-market FINAL_PREGAME state and exactly one SOG FINAL_PREGAME run;
multiple states/runs are errors. Points may be absent and ungradeable, while
Saves absence is permitted only by prior-learning mode. Existing Points/Saves
packages retain the full canonical game spine and can declare a subset of
eligible and prediction games. REFRESH and MIDDAY mainline states do not
satisfy the direct cross-market FINAL_PREGAME source requirement.

This creates a staggered-slate coverage gap: if the one phase package is
published while some later game's required inputs are unavailable, a later
poll cannot append that game's predictions to the immutable phase package.
If the first designated window is missed and no card exists, a poll near a
later game's start can still create a partial card for games that remain
pregame; already-started games stay excluded. The designated 20–75 minute
interval is therefore an AUTO scheduling preference, while certified
prior-day history, valid date-bound roster/source evidence, immutable lineage,
and strict per-game prestart observation and durable-write timestamps are the
actual validity controls. Explicit phase requests currently bypass AUTO's
window choice but not per-game chronology validation.

## Per-game slate coverage and participation grading (2026-10-18/19)

Operator rule: use what was captured for the rest of the slate. One started
game or one game without inputs never fails the other games.

Multiple daily snapshots (2026-10-20). A slate may have up to four daily
runs, each a timestamped snapshot with its own roster capture, and every
snapshot is kept. Reconciliation accepts a resilient SOG package bound to any
roster capture verified for the slate, not only the listing run's, and records
that package's own roster as its source. A package whose certificate differs,
or whose roster matches no verified capture, still fails closed. The prior-day
schedule check accepts the importer's recorded hash, which covers the compact,
key-sorted JSON, as well as the exact file bytes.

Roster snapshots after a puck drop (2026-10-20, P2.24 A). A later daily
snapshot may be captured after some games have started. Observers and
reconciliation accept it (`require_strictly_prestart=False`) because they
judge pregame per game: observers predict only games not yet started, and
reconciliation checks each prediction row against its own game's start. The
snapshot keeps its `strictly_prestart` flag and timestamp. Reusing one roster
capture for a whole slate (`--reuse-roster-observation`, roster refresh) still
requires a capture taken before the first puck.

Schedule snapshots (2026-10-20, P2.24 F). Every schedule fetch is retained,
create-only, beside the current files in the existing `slates/<date>/` folder
as `raw_schedule_response.fetch=<timestamp>.json` and
`slate_health.fetch=<timestamp>.json` (no new directory, per the operator). A successful fetch also
updates the current `slates/<date>/` files. A failed or partial fetch is
kept only in its snapshot, and the importer still exits with an error, so it
never replaces a good current schedule that the next morning's prior-day
check depends on.

Per-row pregame and snapshot provenance in resilient grading (2026-10-20,
P2.24 G/H/I):
- Prop and SOG packages drop only the rows whose own game had started at
  prediction or durable publication, and record the count
  (`post_start_rows_excluded`). One late row no longer fails a package or
  aborts the slate. Unknown games or timestamps still fail closed.
- SOG packages listed by a daily run keep the observer's own CAPTURED
  status and durable publication time. If the listing run linked a later
  no-op status, the CAPTURED status is found in
  `sog_prediction_only/status/<date>/` by exact `run_dir`.
- Rows from the daily path and the orchestration path hash identically, so
  the same package found both ways is not a conflict.
- Points/Saves child and source roster checks accept any roster snapshot
  verified for the slate, not only the orchestration parent's.

Resilient cross-market restatement (2026-10-20). Packages sealed before
per-game resilient grading may hold no cross-market grades even though a
pregame refresh captured some games. `backend/nhl/scripts/restate_nhl_resilient_cross_market.py
--date <slate>` re-runs the resilient grader read-only. It writes only
`graded_resilient_moneyline_production_champion.csv`,
`graded_resilient_moneyline_shadow.csv` and `graded_resilient_puck_line.csv`
to a create-only `learning_restatements/<slate>/restatement=<id>`
(contract `NHL_RESILIENT_CROSS_MARKET_RESTATEMENT_V1`), bound to the package
manifest. `reconciliation_grades.restated_grade_path` resolves those files.
Applied to 2026-10-17: 4 games, Moneyline production, shadow and Puck Line.
`export_nhl_all_candidate_evaluation.py` reads these resilient files only for
games that the original `graded_moneyline.csv` or `graded_puck_line.csv`
does not grade. Where both exist, the original grade is kept, so no game is
counted twice.

Scheduled per-game closing odds (2026-10-20). For closing-line value,
`backend/nhl/scripts/run_nhl_closing_prop_odds.py`
(LaunchAgent `com.proppadia.nhl.closing-prop-odds`, every minute;
tracked in `backend/nhl/launchagents/`) captures player-prop odds once per
game, 13-10 minutes before that game's own scheduled start. The first attempt
lands about 13 minutes before puck, which leaves room for retries before the
10-minute mark (changed from 30-10 on 2026-10-20, when the 5-minute cadence
put captures about 26.6 minutes out):
- The markets, alternate lines, regions and books are the same as the daily
  run's (`daily_capture.capture_odds_observation`).
- Snapshots use phase `CLOSING` in the existing
  `odds_observations/season=<s>/slate_date=<ET date>/observation=*` layout.
  Each game has its own claim (`phase=CLOSING.game=<id>.claim.json`), and no
  compatibility files are written.
- It never requests inside 10 minutes of a start or after it.
- Unbound or failed attempts are retried in the window, up to 3 attempts
  per game; discovery is free, and only a bound event is paid (21 credits).
- Every decision is appended to `closing_capture_log.jsonl` in the day
  folder. Binding failures also go to the LaunchAgent stderr log. A game
  that leaves its window uncaptured is logged as
  `MISSED_NO_CLOSING_CAPTURE`.
- Readers that take a day's latest snapshot or pool all snapshots skip
  side captures, CLOSING and EARLY_TRIAL
  (`daily_capture.is_side_capture_observation`): the 8rain export and the
  monthly SOG/BetOnline quote pool. The all-candidate research export reads
  only EARLY and FINAL_PREGAME snapshots.
- Verification: `report_nhl_closing_odds.py --date <d>` lists, per game, the
  minutes between the closing snapshot and puck drop.

06:00 PT odds-only trial (operator-approved 2026-10-22; slates 2026-10-23 to
2026-10-29). `backend/nhl/scripts/run_nhl_early_prop_odds_trial.py`
(LaunchAgent `com.proppadia.nhl.early-prop-odds-trial`, 06:00 and 06:15 PT,
run under `caffeinate -i`; launchd does not wake a sleeping Mac, so the
operator schedules a 05:58 wake with `pmset` for each trial day)
captures the day's player-prop odds once, for games not yet started, to
measure early coverage and prices against later snapshots and CLOSING:
- Same markets, books and regions as the daily run; phase `EARLY_TRIAL`, one
  snapshot per slate. No predictions, database access or daily-run change.
- The morning run imports the schedule at 07:30, so the trial fetches the
  official schedule itself into `early_trial_schedule/` in the day's odds
  folder (the import's `NHL_SLATE_HEALTH_ROOT` redirect), never the shared
  slate folder.
- It captures only between 05:55 and 06:30 PT (a missed job run on wake is
  logged `OUTSIDE_CAPTURE_WINDOW`), skips once a snapshot exists, and does
  nothing outside the trial dates (`TRIAL_INACTIVE`), so it stops spending
  after the week even if left installed. Decisions go to
  `early_trial_capture_log.jsonl` in the day folder. About 21 credits per game.

- Daily lanes. Phoenix Points and Saves input preparation, the HGB Points
  capture, and the HGB production artifact score every pregame game that has
  inputs. A pregame game with no feature rows or slate identities is recorded
  in `input_excluded_games` (reason `NO_SCORING_INPUT_ROWS`) beside
  `scored_game_ids`; started games remain `started_excluded_game_ids`.
  `eligible_pregame_game_ids` keeps meaning "not yet started". The production
  artifact still fails unless its gap equals the capture's recorded exclusion
  (`HGB_PRODUCTION_CAPTURE_INPUT_EXCLUSION_MISMATCH`).
- Reconciliation chronology. SOG and cross-market FINAL_PREGAME cards are
  checked per game against each predicted game's own start, not the slate's
  first puck (`IMMUTABLE_SOURCE_OBSERVED_AFTER_GAME_START`). A cross-market
  card captured after an earlier game started carries only the later games;
  it must equal the canonical slate minus its recorded
  `started_excluded_game_ids`, each truly started before capture
  (`IMMUTABLE_CROSS_MARKET_STARTED_EXCLUSION_INVALID`), and grading stays bound
  to the full canonical slate. The SOG production reference summary grades
  predicted games and lists eligible games without predictions.
- Per-game prediction source (2026-10-21). The performance summary's
  production SOG reference and the Points HGB grade use the resilient
  selector (`postgame_reconcile.resilient_finalization.select_latest_atomic_sources`,
  `NHL_RESILIENT_PREGAME_FINALIZATION_V1`). Every daily run whose SOG lane
  completed, and every HGB capture, is a candidate; all stay retained, and
  each player-line (SOG) or player-game (Points) is graded from its latest
  source whose feature cutoff, prediction and publication all precede that
  game's own scheduled start (`canonical_admitted_slate.csv`). Proofs:
  - prior-day certificate and roster from the parent daily receipt, re-verified
    by `reconciliation_certificate.daily_receipt_source_proofs` (runs before
    2026-10-18 re-verify from the prior-day package they recorded);
  - SOG identity uses the fixed arm `PRODUCTION_REFERENCE` with the fitted
    model identity; Points uses `NHL_POINTS_COUNT_HGB_V1` with the fixed
    construction `POISSON_SURVIVAL_FROM_FROZEN_HGB_EXPECTED_COUNT` (changed
    2026-10-25, P2.31; it was the capture's fitted identity), so a mid-day
    fitted-artifact change still grades each player-game once, from its latest
    pregame capture. The grade records `fitted_model_identities` and, per
    contributing capture, `fitted_model_identity_sha256` and
    `model_artifact_sha256`;
  - SOG prediction and publication time are the SOG lane's `ended_at_utc`;
    runs before 2026-10-10 recorded no SOG feature cutoff and use the same
    bound;
  - a run that failed after its lane completed counts when its proofs verify;
    Points combines only captures in the latest capture's production/shadow
    role.
  When more than one run contributes, the summary lists them under
  `source_daily_runs` (SOG) or `source_captures` (Points).
- Participation. Official skater outcomes list only dressed players. When a
  final game's official box score lists dressed skaters for both teams, a
  predicted skater absent from it did not play: SOG grades
  `NO_APPEARANCE_UNGRADED`, Points `NONPARTICIPANT_UNGRADED` with
  `NOT_IN_OFFICIAL_LINEUP`. Without that evidence the row stays unresolved.
  Confirmed non-participants are not counted as unresolved in the
  performance summary, as Saves non-starters already were. Resilient SOG maps
  `PARTICIPATED` to `APPEARED` exactly as the FINAL_PREGAME path does.
- Restatement V4. Sealed packages are never rewritten. Phase restatement
  `NHL_PHASE_RESTATEMENT_V4` writes corrected grades beside each package under
  `postgame_reconciliation/learning_restatements/<slate>/restatement=<id>/`.
  It re-grades SOG and resilient SOG from the package's sealed prediction rows
  and `canonical_skater_outcomes.csv`, and reclassifies Points absences. Only
  previously ungraded rows may change; any other difference refuses the
  restatement. `lineage.json` binds the source hashes (including the skater
  outcomes) and per-lane `grade_changes`. Header-only Moneyline/puck line
  files (slates without those cards) pass through unchanged. All 28 packages
  retained through 2026-10-18 have a V4 restatement.
- Readers. The daily prior-day counts and performance summary read the
  restatement. Research and export code resolves package grade files through
  `backend/nhl/reconciliation_grades.restated_grade_path`, which uses a
  restatement only when its lineage binds that package's manifest and its own
  manifest verifies. Frozen single-package reproductions keep their sealed
  inputs (NHL Backlog P2.21).

Prior-date shot-attempt check. During prior-date finalization,
`seed_skater_logs_for_date.py` reconciles each completed game's official
play-by-play against its box score (`reconcile_attempts_per_skater`, wrapped by
`derive_attempts_from_final_pbp`). Since 2026-10-28 (NHL Backlog P2.37) the
check is per skater: a skater is accepted when his PBP shots on goal equal the
box score; a mismatched skater stays unknown; a goalie's attempt is skipped; an
attempt whose shooter is not a skater or goalie of the owning team leaves that
team's skaters unknown. Game-level problems (not final, game or team identity,
duplicate event ids, no attempt events, no reconciled skater) still leave the
whole game unknown. Unknown is never zero, and populated values are preserved.
The child summary adds `pbp_unresolved_skaters` and
`pbp_unresolved_skater_reasons`. The HGB Points scorer scores an unknown
attempt rate beside a known SOG rate as SOG rate x 1.993 (training median
ratio), not zero; the capture receipt counts these as
`attempts_imputed_from_sog_count`. The run diagnostic
`shot_attempts_source_diagnostics/slate_date=<date>/run_id=<run>/attempt_source_diagnostic.json`
lists failure reasons and, since 2026-10-19, retains the failing game's box
score and play-by-play as `failed_source_<game_id>_<kind>.json` with hashes
under `failed_source_responses`, so a failure can be diagnosed afterward.

Roster coverage is per game (2026-10-22, P2.13): a game is usable only when
both its teams have clean roster evidence. A partial-coverage roster package
excludes the other games with a recorded reason; Points, Saves and SOG predict
the covered games only (see "Roster observations" below).

## Resilient pregame evaluation population

The separately identified `RESILIENT_PREGAME` population supplements the
historical FINAL_PREGAME-only population. Lane phase eligibility is explicit:
Moneyline and Puck Line may use `MIDDAY`, `FINAL_PREGAME`, or `REFRESH`; SOG,
Points, and Saves may use `MIDDAY` or `FINAL_PREGAME`. Each source must pass
its lane's model, feature, identity, receipt, certificate, roster/source,
manifest, and chronology checks. Mainline production V3 and shadow V2 remain
separate populations; Puck Line retains its own model role; SOG arms and
contracts remain separate. Points selects a complete player-game ladder and
Saves a complete goalie-game conditional distribution from one observation.

The common outcome-independent selector is implemented in
`backend/nhl/postgame_reconcile/resilient_finalization.py` and is invoked by
reconciliation. It selects the latest eligible durable publication per
atomic identity; equal-time identical payloads use lexicographic source path,
while differing payloads fail closed. It records the full canonical game set,
missing coverage, proof references, source phase, hashes, model/feature
identities, and chronology. It has no outcome input.

The reconciliation adapter discovers sources only through retained daily
parent receipts or a mainline observer receipt that links the independent
Points/Saves/SOG child receipt. It verifies source manifests, prior-day
`CERTIFIED_FINAL` packages, date-bound complete roster packages where
applicable, source identity, atomic ladder/distribution integrity, and
per-game feature/publication chronology. Supported independent sources are
Moneyline/Puck Line `MIDDAY`, `FINAL_PREGAME`, and `REFRESH`; SOG, Points, and
Saves `MIDDAY` and `FINAL_PREGAME`. Moneyline V3 production and V2 shadow,
Puck Line, SOG contract arms, Points model ladders, and Saves conditional
distributions remain separate atomic populations.

After a zero-game prior day (an off day, such as the Christmas break) the
receipts and observers carry a `VERIFIED_EMPTY_SCHEDULE` certificate instead
of a package. The adapter re-verifies it against the retained empty official
schedule (`slates/<prior date>/`) and accepts it like `CERTIFIED_FINAL`. Every
certificate comparison (receipt, observer readiness evidence, child status,
recorded `certificate_sha256`) uses one identity,
`reconciliation_certificate.certificate_identity`: the package manifest hash
for a reconciled day, the retained schedule response hash for an off day. A
recorded identity that differs from the re-verified one is not accepted.

Current source-proof boundary: daily-parent-bound Moneyline/Puck Line outputs
and observer-parent-bound Points/Saves/SOG sources are discoverable. A
standalone mainline Moneyline/Puck Line observer receipt that lacks a distinct
feature-cutoff timestamp is recorded as
`RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN`; its prediction timestamp is
not substituted for missing cutoff proof. Historical SOG packages without an
exact source receipt and durable-publication evidence are unqualified. The
SOG producer writes readiness evidence and a completion marker; its child
status receipt now records publication confirmation after the final manifest.
Points/Saves child receipts likewise record publication confirmation after
their atomic package publication.

The append-only reconciliation package now includes
`resilient_finalization_index.json` and separate `graded_resilient_*.csv`
artifacts with source, parent receipt, certificate, roster, and payload hashes.
Its summary reports lane coverage and per-output denominators. The established
`graded_*.csv` FINAL_PREGAME population and its resolver remain distinct. The
command uses an explicit resilient mode to let official outcome reconciliation
continue when a FINAL_PREGAME card is absent; the absent legacy prediction is
classified ungradeable, while resilient selection independently uses only
verified retained evidence. Duplicate, corrupt, contradictory, or invalid
present cards remain blocking. Quote matching and observed-price returns are
not part of prediction-quality grades. No historical grade or source capture
is rewritten.

Historical sources without embedded readiness proof qualify only when an
exact linked retained observer/daily receipt proves the certificate and roster
evidence. Earlier daily receipts may bind certification through an exact
`REUSED_VALID_PACKAGE` path and `FINAL` status; the referenced package is still
fully hash, date, completion, official-outcome, and exact-game-set validated.
Standalone captured prop status receipts that do not link their package to the
parent certificate and roster remain unqualified. The resolver reports that
proof gap and does not infer linkage from neighboring receipts.

Prop durable-publication proof must come from the exact captured child status
receipt after the immutable package has been published, or from a
hash-bound completion timestamp in that package. An early
`actual_write_timestamp_utc` does not prove completion of the manifest or
publication. A later daily lane end time cannot stand in for an earlier
observer package's publication time. Points/Saves observers now retain
`durable_publication_timestamp_utc` after atomic publication; SOG retains the
same field after writing its completion marker and final manifest. Historical
sources still qualify when their exact contemporaneous parent receipt binds
the certificate, roster, child status, and a valid completion timestamp.

## Prediction evidence classes: research-only classification

Prediction-value validity and compliance with reconciliation-first operations
are separate questions. The existing governed `RESILIENT_PREGAME` definition
continues to require its exact parent/observer receipt, prior-day
`CERTIFIED_FINAL` evidence, applicable date-bound roster proof, manifest-bound
source identity, and durable prestart publication time. A missing receipt does
not by itself establish that a prediction was mathematically invalid, but
neither may adjacent or later receipts be used to assert operational
compliance retroactively.

The following evidence labels are implemented for a separate research-only
evaluation population. They do not qualify a source for governed
reconciliation, model promotion, prospective claims, or replacement of a
legacy grade:

- `VERIFIED_PROSPECTIVE`: an immutable source artifact and its complete input
  snapshot pass hash, model/feature, canonical identity, and atomic-group
  checks; a contemporaneous durable witness binds those exact hashes and
  publication time; each feature cutoff and publication precedes the
  authoritative scheduled start; and no contradictory source evidence exists.
  To additionally claim reconciliation-first operational compliance, the
  source-time parent must bind a valid prior-day certificate and applicable
  roster observation. The label alone does not waive those separate controls.
- `CORROBORATED_PREGAME`: the immutable artifact and input snapshot are
  internally coherent, and explicit per-game prestart times are corroborated
  by contemporaneous retained execution records and the canonical schedule,
  but the durable witness does not bind every source hash or required
  operational prerequisite. This can support only a separately named,
  non-governed retrospective sensitivity analysis, with its own denominator
  and visible evidence limitations.
- `CHRONOLOGY_UNVERIFIED`: chronology rests on source self-assertions or
  filesystem modification times alone; relevant inputs or model identity are
  absent; evidence conflicts; or any prediction may have been made or
  published after its game's start. Exclude it from scored evaluation; it may
  be inventoried as unverified coverage only.

Minimum evidence even for the separate research path is: stable canonical
game/player/market identity; immutable prediction values and verifiable
artifact hashes; model and feature-contract identity; actual retained input
values with an as-of cutoff; authoritative per-game scheduled starts; and a
contemporaneous execution-time witness independent of the prediction file's
self-declared timestamp. Require both cutoff and publication before puck drop.
For Points ladders, Saves conditional distributions, or any multi-arm atomic
group, all values must come from one observation. Ambiguous identities,
conflicting values, corrupt manifests, or after-start timestamps are excluded
or blocking according to scope. No missing value, timestamp, certificate, or
roster proof may be reconstructed.

Bounded artifact inventory and research evaluation are recorded under backlog
P2.17. In brief, the October 18 SOG MIDDAY package contains 1,593 rows for 96
players across games `2026020135` and `2026020136`; its 16:10:14.719263Z
prediction and feature timestamps precede scheduled starts at 22:00Z and
00:00Z. The linked child/mainline receipts corroborate prestart execution, but
do not bind its manifest hash or reconciliation/roster prerequisites. It is
not governed reconciliation evidence. October 17 SOG has 11,262 rows but
lacks source-time certified-history and roster linkage. October 14 prop
packages have valid manifests, input snapshots, and prestart times, but lack
the same governed source-time proof.

The separately implemented research evaluator classifies retained atomic
groups and grades only against independently verified certified-final
outcome packages. On October 14 it selected 497 SOG groups (1,491 rows), 95
Points ladder groups (285 rows), and 9 Saves distributions (117 rows); on
October 17 it selected 3,754 SOG groups (11,262 rows). Those groups are
`CORROBORATED_PREGAME`, not `VERIFIED_PROSPECTIVE`. Replay reproduced retained
SOG, Points, and Saves values and identities from snapshots within `1e-12`;
this does not prove historical input availability at the asserted cutoff.
October 18's 531 SOG groups (1,593 rows) were not graded because no complete
certified-final outcome package was retained. No Points/Saves package was
found for October 17 or 18. October 14 and 17 legacy grade-file hashes were
unchanged. See P2.17 for scored denominators, metrics, and limitations.
The October 18 follow-up found no date directory under retained
`postgame_reconciliation`, so its status remains `NOT_READY`; cross-market
`canonical_outcomes.csv` files do not substitute for certified official
outcomes. The retained SOG source hash and prior legacy-grade hashes still
match. No additional grading run was performed.

### Late outcomes and delayed research grading

Missing a preferred pregame poll never authorizes creating, relabeling, or
backdating a prediction after its game's start. Preserve any retained partial
or otherwise useful source with its original phase and evidence class; missing
procedural proof remains missing. The ordinary prior-day outcome handoff may
later acquire and reconcile official finals, but prediction absence does not
block certification of complete official outcomes in prior-learning mode.
Research grading may proceed only after the exact date-bound official outcome
package is independently verified as complete and `CERTIFIED_FINAL`. Then the
local research evaluator may grade eligible retained observations without
acquisition, database writes, or changes to governed or legacy populations.
If that package is absent, incomplete, corrupt, or not final, the research
result is `NOT_READY`; do not substitute cross-market outcome summaries or
reconstruct outcomes. A later certificate does not upgrade
`CORROBORATED_PREGAME` to `VERIFIED_PROSPECTIVE` without the stronger
source-time evidence required by that class.

Prior-date reconciliation is the first substantive daily operation after the
database connectivity check. Daily work may continue only with a verified,
date-bound complete reconciliation package whose official outcome rows are
FINAL and exactly cover the admitted game set. `NOT_FINAL`, `NO_PACKAGE`,
incomplete outcomes, malformed or corrupt packages, and date/game-set
mismatches block before current-slate schedule acquisition, roster writes,
history ingestion, evaluation, feature construction, predictions, or odds.
`NO_PRIOR_GAMES` is accepted only when the retained official NHL schedule
response and its date-bound health/hash record verify an empty prior date.
A postponed, suspended or cancelled prior-day game (official
`gameScheduleState` `PPD`, `SUSP`, `CNCL`) is void and does not block: the
package admits and certifies the played games, and lists each void game with
its state in `schedule_voided_games.csv` and the summary. The certificate
reports them as `voided_game_ids`; the cross-market prior-day check compares
its certified set with the date's games less those. Void games' predictions
are never graded in any lane (FINAL_PREGAME or resilient); resilient sources
are still verified against the full scheduled slate they were captured for,
and their void-game rows are recorded as excluded (`GAME_VOID_<state>`).
The daily run's prior-date history seeding (goalie and skater logs, roster
refresh, shift charts) receives the certificate's `voided_game_ids` as
`NHL_SCHEDULE_VOIDED_GAME_IDS` and skips those games, so a suspended game's
partial box score does not reach history tables.
A game the official schedule dropped from its date is void as `MOVED` when
its own official box score gives another date; a day on which every scheduled
game is void is certified as `VERIFIED_ALL_GAMES_VOID`, like an off day, with
no package.
Request-level detail is in the Postgame Official Request Accounting Contract.
Official-outcome certification is independent of prediction grading. In
prior-learning mode, a missing immutable lane prediction must remain ungraded
for that lane and must not be replaced with a synthetic capture. Present
duplicate, corrupt, or invalid cards remain blocking. Prior-learning must not
require a missing lane card solely to certify otherwise complete official
outcomes; that lane's grading remains unavailable until valid prediction
evidence exists. Direct reconciliation retains its exact-one-card
requirements.

The host wake event is preparation for early network responsiveness and
availability. No separate NHL stir process or LaunchAgent is identified in
the retained configuration; wake itself does not run NHL code or write NHL
analytical state. A wake or observer poll is not a reconciliation bypass.
Repository command, environment, and sandbox network-escalation rules are
owned by root `AGENTS.md`; this contract does not duplicate them. NHL tests
that require database writes must use an authorized isolated test database.

After that checkpoint, the current-slate schedule and canonical identities are
validated, then the existing governed roster child captures official roster
evidence and transaction-backed player/roster updates in the `roster` lane. It
runs once; explicit roster reuse disables provider fetching. Schedule and
roster failures remain blocking for downstream work. A valid empty current
slate skips the roster child after the prior-date checkpoint.

Points and Saves prediction-only observers retain the complete canonical
schedule spine but determine pregame eligibility separately for each game.
Games already started, or that start before immutable publication completes,
are excluded with manifest-bound reasons. Other future games may still be
captured when their lane inputs qualify. Game and player exclusions remain
visible in the prediction package and observer receipt; partial coverage is
never labeled full-slate coverage. A verified roster observation (complete,
or partial with per-game coverage, see "Roster observations") and certified
prior-day history remain prerequisites. A lane with
no eligible prediction rows creates no card. Reconciliation grades only
retained prediction keys; missing Points predictions remain explicitly
ungradeable and missing Saves predictions remain unpredicted starters or
outcomes. In prior-learning mode an absent Saves card is ungradeable, while a
present invalid or duplicate card still blocks; direct reconciliation keeps
its exact-one-card contract.

HGB Points fitted artifact (2026-10-24, P2.31). Production scores with the
PP-corrected refit of the `NHL_POINTS_COUNT_HGB_V1` recipe:
`artifacts/analysis/nhl/points_hgb_pp_corrected_refit/production_2026-10-24/`
(`nhl_points_count_hgb_v1_pp_corrected.joblib`, artifact SHA-256 `94e75216…`;
`shadow_contract.json`, model identity `fb701c67…`, superseding V1 identity
`a53f43d5…`). Same ten features, recipe and Poisson ladder; trained on the
2023-24 history rebuilt from the corrected database with
`export_nhl_points_hgb_features.build_features`. The model name stays
`NHL_POINTS_COUNT_HGB_V1`, so authority, grading and export are unchanged; the
capture's `model_identity.model_identity_sha256` and `model_artifact_sha256`
record which fitted artifact produced each prediction. Selection is
`CONTRACT_PATH` in `backend/nhl/scripts/score_nhl_points_hgb_shadow.py`;
one-step rollback sets it to `V1_CONTRACT_PATH` (V1 artifact unchanged under
`artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/`).

Production SOG scorer (2026-10-25, P2.33). Production SOG is the frozen HGB
Poisson count model `NHL_SOG_COUNT_HGB_V1` (receipt identity `hgb_count /
NHL_SOG_COUNT_HGB_V1`, scorer `backend/nhl/sog_hgb_count_model.py`, artifact
`artifacts/analysis/nhl/sog_hgb_count_model/frozen_2026-10-25/nhl_sog_count_hgb_v1.joblib`,
SHA-256 `f7bf625d…`, pinned in the scorer). The daily run still scores the
Poisson formula (`score_sog_poisson_baseline.py`) first, into
`sog_predictions_wide_formula.csv`; the formula's scored rows define the
population, and the HGB model rescores them from strict-prior skater logs
(read-only) into `sog_predictions_wide_calibrated.csv` with the formula's
exact column layout (`poisson_source` = `hgb_count`, or `formula_fallback`
where a player has no qualifying history). The `legacy_sog` lane, its fitted
model evidence, `nhl.predictions`, the 8rain export and governed grading read
that production file as before; the lane output also records the HGB history
SHA, artifact SHA, the formula file and the per-row feature file
`sog_hgb_features.csv`. The retained SOG feature package binds the formula's
inputs to the formula's own output. The formula is a shadow
(`NHL_SOG_POISSON_FORMULA_V1`, lane `sog_poisson_formula_shadow`, captures
under `artifacts/operational/nhl/sog_formula_shadows/`). Formula-variant
research shadows (fixed blends, D10/D20 divergence, structural correction,
defense surprise) keep reading the formula file; the SHRUNK_180 shadow pairs
with production. The fixed-blend, divergence and structural lanes (and the
divergence and structural manifests) record `production_model_identity` as the
actual production SOG model and `comparator_model_identity` as
`poisson_baseline/baseline_v1`, the formula file they pair against; their
`*_vs_production` grade fields therefore compare against the formula, and the
daily summary labels the divergence comparison "vs the Poisson formula".
Captures before 2026-10-28 still list the formula as production. Grading accepts either identity
(`PRODUCTION_SOG_SCORERS` in `performance_summary.py`), so slates before the
cutover keep the formula as production. One-step rollback:
`NHL_SOG_SCORER=poisson_baseline` (the formula writes the production file and
the HGB model returns to its shadow lane).

Game-market roles in the daily performance summary (2026-10-26). Moneyline
shows `Production (<model>)` from the governed grades and, when present,
`Shadow (<model>)` from the cross-market grade package's
`graded_moneyline_shadow_results.csv` (V2 since the 2026-10-14 cutover). The
challenger file still grades V3, which is now production; when its model equals
production it is not shown, so slates before the cutover keep `Challenger
(V3)` and no duplicate shadow. Puck Line shows `Reference (<model>)` and
`Challenger (<model>)`. The weekly evaluation takes the Moneyline shadow's
daily record from those grade packages for days whose summary does not carry
it (each game's latest grade).

Input watch (2026-10-28, NHL Backlog P2.38). Lane `input_watch` runs after
production SOG, Points HGB and the odds observation, before
`research_integrity`. It is read-only (no database access, no provider
requests, predictions unchanged) and non-blocking: a failure is
`FAILED_NONBLOCKING`, and flags never change the run classification. Output:
`artifacts/operational/nhl/input_watch/slate_date=<d>/run_id=<r>/`
(`input_watch.json`, `input_watch.md`, `SHA256SUMS`), and the console line
`NHL_INPUT_WATCH=flags=<n> <flags> path=<md>`. Contents
(`backend/nhl/input_watch.py`): (1) input sanity per player: impossible
values (attempts below shots, team shots outside 15-45, shot share outside
0-0.5, PP TOI above TOI), substituted attempts, and Points inputs outside the
frozen model's training p01-p99
(`backend/nhl/config/points_hgb_training_feature_ranges.json`); a check is a
flag when any impossible value appears or more than 10% of players are out of
range or missing (2% for substituted attempts); (2) the ten largest gaps per
lane between model P(selected side) and the median same-book no-vig market
probability on two-sided lines observed before the watch ran (quote matching
reuses `export_nhl_all_candidate_evaluation.load_retained_quotes`), each with
reasons: rule reasons from P2.29/P2.37 and inputs at least 1.5 standard
deviations from players the market prices within 5 points on the same line;
(3) the prior date's shot-attempt source diagnostic (games dropped is a
flag). SOG has no retained training matrix, so its out-of-range check is not
available.

Two-sided line level in the daily performance summary (2026-10-28). Next to
the all-predictions tables, SOG production and Points HGB production show W/L
by line and side for only the graded predictions whose exact line had a
two-sided quote: the same book or prediction market quoted both Over and Under
in a retained EARLY/FINAL_PREGAME snapshot (CLOSING excluded) whose quote and
observation precede the prediction, which precedes the scheduled start. One
side quoted, or Over and Under from different books, does not count. Matching
reuses `export_nhl_all_candidate_evaluation` (`--price-both-sides`) through
`backend/nhl/two_sided_lines.py`; SOG rows are the summary's own production
grade rows, Points rows are the export's HGB production rows and are shown only
when their W/L equals the governed HGB selected-side record. The level is
descriptive (`affects_grading_denominator: false`), is stored as
`two_sided_lines` on `models.sog.reference` and `models.points`, and, like
market coverage, is left out of the grade projection used to select the
authoritative summary. If odds evidence cannot be read the level shows
`UNAVAILABLE` with its reason and the summary is otherwise unchanged.

Points probability ladders have two construction contracts.
`NHL_POINTS_COUNT_HGB_V1` predicts one nonnegative expected count and derives
the 0.5, 1.5, and 2.5 exceedance probabilities from the same Poisson count
distribution. The thresholds are structurally ordered. Its capture validator
requires exactly those three lines and non-increasing values. A player-game
whose expected count or probabilities are not finite, out of range, or crossing
is excluded on its own (`HGB_INVALID_PREDICTION_VALUE`, `HGB_LADDER_CROSSING`;
NHL Backlog P2.13): `features.csv` keeps every scored identity, `predictions.csv`
only the valid ones, and the receipt records `player_exclusions`. The
deterministic replay must reproduce the same exclusions. The production
artifact accepts a feature identity without a prediction only when it is one
of those recorded exclusions (otherwise
`HGB_PRODUCTION_FEATURE_PREDICTION_IDENTITY_MISMATCH`), leaves it out of the
graded rows, and carries `player_exclusions`; game coverage and the Phoenix
control comparison still use every scored identity. A capture with no valid
prediction still fails (`COHERENCE_FAILED:NO_VALID_HGB_PREDICTIONS`). Frozen Phoenix points scores the
three thresholds with separate binary models, retains `raw_prob_over`, and
applies equal-weight non-increasing isotonic projection
(`EQUAL_WEIGHT_ISOTONIC_EXCEEDANCE_V1`) to published values. Missing/duplicate
lines, non-finite or out-of-range raw probabilities, and model identity
failures invalidate a player-game ladder. The prediction-only publisher
records ladder exclusions per player-game and retains other eligible
predictions. It uses `partition_coherent_ladders`, so a structurally invalid
player-game is excluded in `prediction_exclusions.csv` with decision
`EXCLUDED_INVALID_LADDER` and its reason (`POINTS_INCOMPLETE_LADDER`,
`POINTS_DUPLICATE_LADDER_LINE`, `POINTS_INVALID_LADDER_PROBABILITY`) instead of
failing the batch (P2.13). `construct_coherent_ladders` keeps its whole-batch
failure for the frozen shadow and research callers. Model values and
qualification are unchanged. Raw threshold crossings
from the independent Phoenix models are repaired by the documented projection;
they are not themselves invalid published probabilities. A ladder is valid
only when the required three distinct lines and model identities are present,
raw values are finite and within `[0,1]`, and the published sequence is
non-increasing. The HGB count-derived sequence is checked for exact line
cardinality and ordering at capture validation.

The roster observation is sourced from official NHL team roster endpoints
(`/v1/roster/{team}/current`, with a season-specific resolved path recorded
when applicable). “Complete” means every team appearing in the verified
canonical slate has a source response and every game has normalized rows for
both scheduled teams, with no missing/unexpected teams, conflicting
natural-key values, or divergent repeated team snapshots. It is not a
whole-league roster census nor a guarantee that every dressed player is
present. Player rows missing canonical IDs are counted as exclusions; current
roster membership does not prove game participation.

Package integrity and roster coverage are separate: missing response/team or
per-game coverage makes a package incomplete; malformed source/sections,
conflicting identities, altered manifest files, absent completion marker,
wrong date/game-set binding, or non-prestart observation makes it invalid.
The full-slate verifier (`verify_roster_observation_reuse`) is unchanged and
still requires complete coverage and valid package integrity. There is no
maximum-age check beyond date/game binding and observation before first puck.
Since 2026-10-22 (P2.13) a team-level coverage gap no longer blocks unrelated
games: the writer records it in a separate partial-coverage package, verified
by its own validator, and consumers restrict themselves to the covered games
(see "Roster observations"). Integrity failures still invalidate the whole
package.

### Roster availability incident finding (October 18, 2026)

The retained incidents do not establish one recurring NHL roster-provider,
identity, or database defect. Two distinct operational conditions are
confirmed:

- On September 30, the Points and Saves observers each recorded
  `*_FEATURE_EXPORT_EMPTY` at 14:30:10Z. The same daily run had started at
  14:30:08Z; its existing `refresh_players_and_roster_today.py` child completed
  at 14:32:11Z with an immutable, hash-verified six-team observation and a
  committed transaction (239 player and roster rows). This is a confirmed
  observer/readiness timing race on a successful acquisition, not evidence of
  a failed official endpoint or failed database write. The roster observation
  was prestart, and immutable Points/Saves FINAL_PREGAME captures were later
  retained for the date.
- On October 17, prior-day learning failed before the current-slate roster
  stage; the two retained failed daily attempts recorded the roster lane as
  `NOT_STARTED`. On October 18, the 14:30Z parent failed its governed prior-day
  authority/roster child with
  `IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:0`; only schedule import had
  completed, and no roster observation or roster database transaction was
  attempted. These are reconciliation/source-certification stops, not roster
  endpoint, normalization, or roster-write failures. Current reconciliation-
  first behavior is intentional and must not be bypassed to recover a pregame
  opportunity.

The October 17 source query and later complete observation are documented in
backlog P2.9. Repeated empty feature exports on other dates are symptoms only
unless their date-bound daily receipt, source-response journal, and database
transaction evidence identify the first failed stage. A later successful
roster package does not prove its source response or database rows existed at
an earlier observer poll. Do not generalize a provider outage,
team-normalization failure, or transaction failure from an empty export.

The durable operating correction is to preserve the single governed roster
child after successful prior-day certification and current-slate schedule
validation, and have observers require its valid same-slate certificate before
feature export. An observer that starts earlier must report the unmet
certificate/readiness condition without treating an empty feature query as a
provider diagnosis. If certification fails, stop and repair the actual
prior-day evidence or contract; do not move roster acquisition ahead of
certification. The current code already enforces the certificate and roster
prerequisites before Points/Saves feature export; natural runs must confirm
that behavior and transaction evidence. Per-game partial coverage (P2.13)
addresses a single team's bad response; it is not a correction for the
confirmed timing incidents and is never written for blocked or late
acquisition (zero covered games writes no package).

Points/Saves observers classify an absent current-slate roster package as
`ROSTER_NOT_READY`, write a lane status receipt, and remain eligible to retry
on a later ordinary poll. They do not start feature export or create a
prediction attempt in that state. A present but corrupt, contradictory,
wrong-date, wrong-game-set, or non-prestart package remains a blocking
validation failure. A verified partial-coverage package is
`VERIFIED_PARTIAL_CURRENT_SLATE_ROSTER`, not a failure. Freshness is established by current-slate date
and canonical game-set binding plus strictly prestart observation; no separate
maximum-age threshold is defined by this contract.

The mainline prior-day source handoff uses the reconciler's `--prior-learning`
mode for authority acquisition, read-only source preflight, database identity
preflight, and execution. In that mode an absent active Saves or cross-market
prediction card is explicitly ungradeable and does not block acquisition or
certification of official outcomes. Any present card is still fully validated;
duplicates and corrupt or contradictory evidence remain blocking. Direct
reconciliation keeps exact-one requirements. Before continuing, the mainline
verifies the completed date-bound reconciliation certificate and checks its
game IDs against the expected prior-date set. A database result with no games
continues only when the retained official schedule independently certifies an
empty date. Child failures retain bounded, redacted stage, exit code, stdout,
and stderr diagnostics in the parent observer receipt.

Official roster endpoints establish observed team-roster membership only.
They do not certify dressed players, lines, availability, or confirmed goalie
starters. Those remain separate evidence and qualification requirements.

### Natural-run verification (October 18, 2026)

Daily run `nhldaily_20261018T183659295070Z_49d62ee8` completed READY from
18:36:59Z to 18:40:32Z. Its October 17 `CERTIFIED_FINAL` package covered all 14
games and completed at 18:12:09Z. The daily implementation validates that
date-bound certificate before current-slate schedule import; the retained
child order places schedule import before the one governed October 18 roster
observation. That observation contains 100 rows from four official NHL API
team-roster responses, covers both scheduled games and all four teams, is
strictly prestart, has no missing teams or conflicts, and has valid manifest
and completion hashes. Its corresponding player and roster-status writes are
recorded COMMITTED.

The same daily graph separately ran `refresh_players_and_roster_today.py`
with `SLATE_DATE=yday` during October 17 history finalization. Its 686-row
committed refresh did not create or replace the October 18 immutable roster
observation. The `refresh_all_team_rosters.py` child is a separate full-league
player and identity refresh. The available receipts and caller inputs support
these as distinct history/full-league preparation stages; no removal is
justified by the current evidence.

The daily run retained both eligible games in SOG (91 scored rows, none
unscored), Points (300 predictions from 100 scoring inputs), and Saves (9
predictions). Their feature cutoff was 18:40:03Z, before the first game start
at 22:00Z; recorded source and prediction hashes match retained artifacts and
model identity checks passed. Its odds observation used 42 of 42 authorized
credits over three network attempts. The Moneyline V3/V2 package is explicitly
`REFRESH` and uses that odds manifest; it is not a FINAL_PREGAME package.

At 18:42:15Z the next retained mainline poll produced a separate `MIDDAY`
capture and used 6 credits. Points and Saves reported
`NOOP_OUTSIDE_PREDICTION_WINDOW`; SOG reported `NOOP_OUTSIDE_WINDOW`. No
FINAL_PREGAME observer card was retained by that poll. Natural acceptance
therefore remains pending for an observer poll within the configured 20–75
minute first-start window, including verified game-level card coverage and
immutable manifests. That poll must also confirm SOG's missing-roster status
classification when the roster package is absent; an outside-window no-op
does not exercise that condition. See backlog P2.15–P2.16.

Before V1, the optional odds step wrote mutable odds files directly, could retry individual
provider calls, and represented failures as empty results. V1 retains the same
comprehensive graph while placing immutable roster evidence before roster DML,
placing prediction durability before optional odds, and binding market builders
only to the current immutable observation. Prediction work does not depend on
odds availability or canonical market matching.

## Odds observations

Each explicit slate/phase attempt is claimed before provider access and is
written beneath:

```text
artifacts/operational/nhl/odds_observations/
  season=2026/slate_date=YYYY-MM-DD/
    observation=TIMESTAMP_STABLE-ID/
```

The package contains `request_metadata.json`, `response_envelope.json`,
`events_response.json`, `raw_response.json`,
`transport_response_bodies.jsonl`, `normalized_odds.jsonl`,
`game_binding.jsonl`, `observation_summary.json`, `SHA256SUMS`, and either
`RUN_COMPLETE.json` or `ATTEMPT_COMPLETE.json`. Transport bodies are retained
in base64 with their exact byte lengths and SHA-256 identities. Request URLs
and parameters are sanitized; credentials and authorization headers are never
written.

The classifications are:

- `CAPTURED_NONEMPTY`: usable price rows exist and at least one provider event
  binds uniquely to the canonical slate.
- `CAPTURED_VALID_EMPTY`: the provider succeeded but returned no events, no
  requested markets, or no usable price rows. The exact empty reason and exact
  successful body are retained.
- `CAPTURED_UNMATCHED`: markets were returned but no provider event bound to a
  canonical game.
- `FAILED_PROVIDER`: transport, HTTP, authentication, quota, or declared
  provider failure.
- `FAILED_MALFORMED_RESPONSE`: the response could not be validated or parsed.
- `SKIPPED_NO_AUTHORIZATION`: odds were requested but credentials or explicit
  authorization were absent.

Captured empty and unmatched observations are nonblocking and yield daily
health `READY`. Provider, malformed, and authorization failures yield
`READY_WITH_ODDS_WARNING` after independent prediction work has completed.
They are never relabeled as successful empty responses.

A create-only claim is scoped to season, Pacific slate date, and phase. Replay
of a completed observation reuses it without provider access. A competing,
stale, or failed claim fails closed with no implicit retry or fallback. The
package records logical and transport counts, every HTTP status, retry and
fallback counts, response identities, matched/unmatched/ambiguous counts,
provider as-of values, and provider credit headers when supplied.

Mutable site files are compatibility derivatives written only after the
immutable package verifies. `odds_nhl_playerprops_today.json` and
`events_today.json` describe the latest successful captured attempt.
`odds_observation_latest.json` identifies that attempt. `odds_latest.json`
remains the latest nonempty captured body, so a valid empty observation cannot
destroy the last nonempty research snapshot. Failed attempts update none of
these files. Prediction/market builders never implicitly read these mutable
files; they accept only an explicitly supplied run-bound observation.

## Roster observations

The comprehensive runner writes official roster evidence beneath
`artifacts/operational/nhl/roster_observations/`, separately from odds. Each
package binds UTC/PT observation time, phase, parent daily run, Pacific slate
date, canonical game-set hash, game/team/player natural keys, requested and
resolved source URLs and endpoint families, response hashes, exclusions,
duplicates, conflicts, per-game team coverage, `SHA256SUMS`, and
`RUN_COMPLETE.json`.

Repeated or split-squad team appearances remain represented once per
game/team/player even when one identical team response supports multiple
games. A `/roster/current` request and its season-specific resolved endpoint
are recorded distinctly. Divergent repeated snapshots or conflicting
natural-key values fail
before player or roster DML. `/roster/current` is explicitly provenance for a
pregame roster observation, not proof of dressed-game participation.

### Per-game roster coverage (2026-10-22, P2.13)

- Package types. When every game has clean evidence for both teams the
  writer produces the unchanged `NHL_ROSTER_RESEARCH_OBSERVATION_V1` package
  (marker `COMPLETE`). When only some games do, it writes
  `NHL_ROSTER_PARTIAL_COVERAGE_OBSERVATION_V1` (marker
  `PARTIAL_COVERAGE_COMPLETE`, `complete_per_game_coverage=false`), which the
  full-slate validator rejects by construction. It keeps every V1 field and
  file, binds the full canonical game set and hash, and adds the
  manifest-bound `roster_game_coverage.csv` (one row per canonical game:
  `game_id`, `home_team`, `away_team`, `coverage` `COVERED`/`EXCLUDED`,
  `home_team_reason`, `away_team_reason`) and summary fields
  `covered_game_ids`, `covered_game_set_hash`, `excluded_game_ids`,
  `excluded_teams`, `team_failure_details`. `roster_snapshot.jsonl` holds
  covered games only; raw responses and conflicts are all retained.
- Team reasons, in precedence order: `TEAM_NATURAL_KEY_CONFLICT`,
  `TEAM_DIVERGENT_SNAPSHOTS`, `TEAM_RESPONSE_FAILED` (HTTP error or exhausted
  transport after ordinary retries), `TEAM_RESPONSE_MISSING`,
  `TEAM_RESPONSE_EMPTY` (including an empty roster the request journal
  rejected). An empty response is never coverage. A game is covered only if
  neither team has a reason.
- Still fail-closed with no package: a malformed response or section, an
  unexpected team, any journal/redirect/identity integrity error, and zero
  covered games. The governed current-slate roster child records a single
  team's provider failure and continues; other callers of the same script
  (the prior-date refresh) keep their previous behavior. There is no second
  fetch and no retry change.
- States. `verify_slate_roster_coverage` picks the validator by
  `schema_version` and never falls back. The observer gate reports
  `VERIFIED_CURRENT_SLATE_ROSTER` (result unchanged),
  `VERIFIED_PARTIAL_CURRENT_SLATE_ROSTER` (adds covered/excluded game IDs and
  `roster_game_exclusions`), `VERIFIED_EMPTY_CURRENT_SLATE`, `ROSTER_NOT_READY`
  (schedule not imported or no package; retry), or
  `CURRENT_SLATE_ROSTER_OBSERVATION_INVALID` (blocking). The newest snapshot
  is used, as before.
- Daily run. The roster child writes player/roster rows only for teams in
  covered games. The `roster` lane finishes `COMPLETE_WITH_BOUNDED_LIMITS`
  with reason `ROSTER_PARTIAL_GAME_COVERAGE`, and the receipt's
  `roster_observation` lists `covered_game_ids` and `roster_game_exclusions`.
  Saves and Phoenix Points scoring inputs drop excluded games
  (`roster_excluded_games`), and prediction validation rejects any row on an
  excluded game. HGB Points scores from the Phoenix rows; if Phoenix is
  unavailable on a partial slate HGB is blocked. Daily moneyline/puck line is
  `BLOCKED_LANE_LOCAL` with `CURRENT_SLATE_ROSTER_PARTIAL_COVERAGE_NOT_SUPPORTED`.
  `--reuse-roster-observation` still requires a complete prestart package.
- Observers. Points/Saves exclude each uncovered game in
  `game_exclusions.csv` and the observer receipt with reason
  `ROSTER_COVERAGE_EXCLUDED:<team>=<reason>`; their `eligible_game_ids` stay
  time-based and `coverage_status` is never `FULL_SLATE`. SOG drops those
  rows and records them in `excluded_players.csv` with the same reason. The
  mainline cross-market observer reports `FAILED_WARN_ONLY` with
  `CURRENT_SLATE_ROSTER_PARTIAL_COVERAGE_NOT_SUPPORTED` before export or any
  odds request.
- Grading. Resilient grading verifies roster packages through the
  dispatcher. A package bound to a partial roster with any row on an
  uncovered game is excluded as `RESILIENT_ROSTER_GAME_NOT_COVERED`; a Points
  or Saves package's embedded covered games must equal the verified
  package's. The immutable package validator accepts a roster exclusion
  reason only when it is exactly the one the package's partial roster
  evidence names. `daily_receipt_source_proofs` rejects a partial roster
  unless the caller passes `accept_partial=True`; Points HGB evaluation and
  the performance summary do not, so they keep the previous behavior.
- Compatibility. All 104 retained V1 packages (2026-09-24 to 2026-10-22)
  verify unchanged through the dispatcher; no package is rewritten.

## Provisional phase planner

`NHL_FIRST_PUCK_PHASE_PLANNER_V1` is pure and has no provider, database,
scheduler, or filesystem side effects. From the verified whole-slate first
puck it proposes:

- `EARLY = min(06:30 PT, first puck - 4h30m)`;
- `REFRESH = min(13:30 PT, first puck - 2h30m)`;
- `FINAL_PREGAME = first puck - 75m`.

The planner returns no phases for a valid empty slate, never emits a phase at
or after first puck, honors Pacific DST, permits unusually early targets on
the prior Pacific calendar date, and reports `PLANNED`, `DUE`, `COMPLETED`,
`MISSED`, or `SUPERSEDED`. Completed identities are not repeated. Odds
availability is not a planning input.

This contract does not activate the planner. LaunchAgents, the installed
900-second observer interval, and every production schedule remain unchanged.

## Safety and policy

The comprehensive runner performs no odds acquisition without `--with-odds`.
Zero matched markets cannot suppress Points, Saves, or SOG predictions.
Preseason mainline results remain research/non-evaluation, and this capture
contract never publishes, promotes a model, or places a wager. A live daily
run or cadence activation requires separate authorization.
