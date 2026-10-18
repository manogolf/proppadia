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
