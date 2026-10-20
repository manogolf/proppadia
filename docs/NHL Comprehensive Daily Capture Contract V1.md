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
play-by-play against its box score (`derive_attempts_from_final_pbp`). A game
that fails keeps its attempts unknown (never zero) and preserves populated
values. The run diagnostic
`shot_attempts_source_diagnostics/slate_date=<date>/run_id=<run>/attempt_source_diagnostic.json`
lists failure reasons and, since 2026-10-19, retains the failing game's box
score and play-by-play as `failed_source_<game_id>_<kind>.json` with hashes
under `failed_source_responses`, so a failure can be diagnosed afterward.

Roster coverage remains slate-wide: an incomplete roster package still blocks
Points/Saves feature export for the slate (see "Roster observations" below and
NHL Backlog P2.13).

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
never labeled full-slate coverage. The complete, strictly prestart roster
observation and certified prior-day history remain prerequisites. A lane with
no eligible prediction rows creates no card. Reconciliation grades only
retained prediction keys; missing Points predictions remain explicitly
ungradeable and missing Saves predictions remain unpredicted starters or
outcomes. In prior-learning mode an absent Saves card is ungradeable, while a
present invalid or duplicate card still blocks; direct reconciliation keeps
its exact-one-card contract.

Points probability ladders have two construction contracts.
`NHL_POINTS_COUNT_HGB_V1` predicts one nonnegative expected count and derives
the 0.5, 1.5, and 2.5 exceedance probabilities from the same Poisson count
distribution. The thresholds are structurally ordered. Its capture validator
requires exactly those three lines and non-increasing values; a failed ladder
currently fails the HGB capture as a whole. Frozen Phoenix points scores the
three thresholds with separate binary models, retains `raw_prob_over`, and
applies equal-weight non-increasing isotonic projection
(`EQUAL_WEIGHT_ISOTONIC_EXCEEDANCE_V1`) to published values. Missing/duplicate
lines, non-finite or out-of-range raw probabilities, and model identity
failures invalidate a player-game ladder. The prediction-only publisher
records ladder exclusions per player-game and can retain other eligible
predictions. However, the shared ladder constructor currently raises on an
invalid player-game before that per-player exclusion stage, so malformed
Phoenix input can still fail the batch. This is a known scope mismatch, not
authorization to change model values or qualification. Raw threshold crossings
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
The current verifier requires both complete coverage and valid package
integrity, and Points/Saves require that slate-level package before either
lane exports features. There is no maximum-age check beyond date/game binding
and observation before first puck. Consequently, a missing team/game currently
blocks unrelated games even when their own roster evidence is present. A
verified partial-game package could support only games with both teams and
valid identity/source evidence, but the current writer and observer contract
do not support that mode. Supporting it requires an explicit partial-coverage
manifest state and per-game downstream binding; do not simply relax the
current completeness or hash checks.

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
that behavior and transaction evidence. Full-slate roster coverage remains the
current contract. Partial coverage is a separate proposed capability and is
not established as a correction for the confirmed incidents.

Points/Saves observers classify an absent current-slate roster package as
`ROSTER_NOT_READY`, write a lane status receipt, and remain eligible to retry
on a later ordinary poll. They do not start feature export or create a
prediction attempt in that state. A present but corrupt, contradictory,
wrong-date, wrong-game-set, incomplete, or non-prestart package remains a
blocking validation failure. Freshness is established by current-slate date
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
