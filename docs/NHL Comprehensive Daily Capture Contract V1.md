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

Current source-proof boundary: daily-parent-bound Moneyline/Puck Line outputs
and observer-parent-bound Points/Saves/SOG sources are discoverable. A
standalone mainline Moneyline/Puck Line observer receipt that lacks a distinct
feature-cutoff timestamp is recorded as
`RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN`; its prediction timestamp is
not substituted for missing cutoff proof. Historical SOG packages without a
durable-write/completion timestamp are also unqualified. The SOG producer now
writes and hash-binds `actual_write_timestamp_utc` and
`RUN_COMPLETE.completed_at_utc` for future captures.

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

Bounded read-only replay of retained final slates on 2026-10-14 and
2026-10-17 verified the source adapters and separate grading outputs. On
October 14, Moneyline V3 and V2 shadow and Puck Line selected both staggered
games from REFRESH; SOG selected 1,482 atomic groups across both games from
MIDDAY. Points and Saves immutable captures were present but lacked the
required parent-bound certificate/roster proof. On October 17, the only
retained market REFRESH was after first puck and roster reuse failed the
strict-prestart check, so no source qualified. October 14 legacy grades
remained unchanged in retained files; recomputed shared grade values matched
exactly for Moneyline, Puck Line, Points, and Saves. SOG had only three
floating-point round-trip differences below 3.31e-24, with the retained grade
timestamp used. See backlog P2.17 for per-lane counts, proof exclusions,
verification details, and remaining natural-run acceptance.

Prior-date reconciliation is the first substantive daily operation after the
database connectivity check. Daily work may continue only with a verified,
date-bound complete reconciliation package whose official outcome rows are
FINAL and exactly cover the admitted game set. `NOT_FINAL`, `NO_PACKAGE`,
incomplete outcomes, malformed or corrupt packages, and date/game-set
mismatches block before current-slate schedule acquisition, roster writes,
history ingestion, evaluation, feature construction, predictions, or odds.
`NO_PRIOR_GAMES` is accepted only when the retained official NHL schedule
response and its date-bound health/hash record verify an empty prior date.
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
When an explicitly authorized read-only test fails on sandbox DNS or transport,
Codex must request elevated network permission and retry that same test before
classifying the condition as a genuine provider or database connectivity
failure. Load configured environment values without printing credentials.

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
