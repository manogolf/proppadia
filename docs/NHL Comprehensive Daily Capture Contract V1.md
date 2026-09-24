# NHL comprehensive daily capture contract V1

## Canonical operator entry point

The canonical comprehensive command is:

```sh
.venv/bin/python -m backend.nhl.cli daily --with-odds
```

`bin/nhl_ops.sh daily` is a wrapper for that module command. Git history
retains `backend/nhl/scripts/nhl_all.sh` as a removed predecessor. The reported
historical `backend/nhl/scripts/cli.py` path is absent from the current tree and
has no retained tracked Git object. Neither path is an independent current
implementation. There must be only one evolving daily graph.

Operational NHL dates use `America/Los_Angeles`; durable timestamps use UTC
and, where useful for review, their Pacific representation. The season is
identified by its single starting year, such as season `2026`.

The preserved comprehensive graph is:

1. database sanity;
2. full-league roster refresh;
3. prior-date outcome, log, shift, manpower, and feature finalization;
4. current-date schedule acquisition and canonical-slate validation;
5. slate roster capture and player normalization;
6. feature construction, prediction scoring, and prediction durability;
7. optional governed odds observation, only with `--with-odds`;
8. optional market attachment from that run-bound observation;
9. research refresh, archival, integrity checks, and final sanity counts.

Before V1, step 7 wrote mutable odds files directly, could retry individual
provider calls, and represented failures as empty results. V1 retains the same
comprehensive graph while placing immutable roster evidence before roster DML,
placing prediction durability before optional odds, and binding market builders
only to the current immutable observation. Roster and prediction work does not
depend on odds availability or canonical market matching.

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
