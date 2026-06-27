# Project Doctrine

This doctrine is a permanent engineering standard for Proppadia. It applies to every sport, model lane, research board, artifact, derived field, report, and automation path.

The project lesson is simple:

Backfilled data is not finished data.

A feature is not complete because it worked once historically. It is complete only when the daily system keeps it correct, watches it, surfaces regressions, and documents the ownership path.

## Feature Completion Lifecycle

A feature is `COMPLETE` only after all of these are true:

1. Historical backfill is complete for the intended window.
2. Daily generation keeps the feature current going forward.
3. Automation is wired into the correct daily, intraday, or postgame workflow.
4. Health checks verify the expected coverage, freshness, row counts, and core invariants.
5. Ops Brief visibility exists when the feature affects daily interpretation or operational confidence.
6. Daily Index visibility exists when the feature produces artifacts or research outputs Jerry may need to open.
7. Regression detection is explicit: missing fields, stale artifacts, low coverage, and date mismatches must warn or fail.
8. Documentation identifies the source, command, owner workflow, expected cadence, and repair command.
9. Historical and current validation both pass.

If any item is missing, the feature remains `INCOMPLETE`.

## Research Lifecycle

Research may start as an exploratory notebook, temporary script, one-off audit, or local artifact. That is fine, but the lifecycle must be explicit:

- `exploratory`: one-off analysis, not durable, not operational.
- `research-active`: repeated analysis or board, but not production-affecting.
- `operational-research`: daily artifact or Ops Brief input, still no production selector/upload behavior.
- `production-candidate`: shadowed or monitored with health checks and outcome tracking.
- `production`: wired into user-facing or upload/scoring behavior with deployment controls.

Crossing from one state to the next requires updating automation, health checks, documentation, and visibility. A research artifact that is read daily must follow the same sustainability rules as a production-adjacent artifact, even if it never changes production behavior.

## Backfill Rule

Every backfill must answer:

What keeps this field populated tomorrow?

If the answer is unclear, the backfill is not done.

Backfill work must include:

- date range and row counts;
- source tables/artifacts;
- no-future-data guarantee where relevant;
- output paths;
- coverage before and after;
- validation commands;
- daily generation command or automation hook;
- regression health check.

## Daily Sustainability Rule

Any derived field, feature column, review board, score, report, or artifact that will be used after today must have a daily sustainability path.

The daily path must specify:

- producer command or script;
- execution order;
- required inputs;
- output artifacts;
- downstream consumers;
- failure behavior;
- repair command.

Manual commands are acceptable for optional research. They are not acceptable for routine daily artifacts required by the Ops Brief, Daily Index, review boards, model evaluation, reconcile, or upload preparation.

## Operational Visibility Rule

If a feature can change daily interpretation, it must be visible in the operator surfaces.

Required visibility depends on scope:

- Ops Brief: concise status when daily decisions or confidence depend on it.
- Daily Index: links, row counts, freshness, missing-input status, and current research status.
- Preflight: hard gate or warning when missing/stale artifacts would make daily output misleading.
- Detailed report: full explanation, diagnostics, and repair commands.

Zero rows must not silently masquerade as normal when input artifacts are missing or stale.

## Regression Rule

Every durable feature must define what regression looks like.

Common regression checks:

- coverage below threshold;
- stale source date;
- artifact missing;
- date mismatch;
- row count unexpectedly zero;
- duplicate keys;
- future-source leakage;
- unresolved join identity;
- source-not-ready vs true failure classification.

Regression checks should fail loudly enough that the next daily run cannot quietly publish misleading confidence.

## Time-Derived Fields

Time-derived fields must name their timezone and bucket boundaries in code and documentation.

For MLB, canonical time-of-day buckets are ET-based:

- `morning`: 00:00 <= ET hour < 12:00
- `afternoon`: 12:00 <= ET hour < 16:00
- `evening`: 16:00 <= ET hour < 20:00
- `late`: 20:00 <= ET hour < 24:00

Any feature, artifact, or report using `time_of_day_bucket` should use the shared MLB helper instead of hand-rolled local logic. If a source timestamp is UTC, it must be converted to ET before bucket assignment.

## Canonical Identity

Canonical identity is part of feature completeness.

Reference: `docs/CANONICAL_IDENTITY_DOCTRINE.md`

The project standard is:

- IDs are identity.
- Names are labels.
- Aliases are bridges.
- Fallback joins are diagnostics, not foundations.
- Stored analytical rows should carry canonical IDs whenever available.

Every durable row must be able to answer:

- What is the canonical identity for this row?
- Which fields prove identity?
- What fallback was used, if any?
- Is that fallback visible?
- What keeps identity complete tomorrow?

For MLB, canonical player identity is MLB `player_id`; canonical game identity is MLB `game_id`; canonical market research identity is `date + game_id + player_id + prop_type + side + line`.

External provider rows may begin as name/event/team aliases, but derived analytical artifacts should preserve canonical IDs once they are available.

## Code Review Standard

When reviewing any change that adds or modifies one of these:

- columns;
- artifacts;
- reports;
- research outputs;
- derived fields;
- model features;
- scoring context;
- reconciliation fields;
- automation commands;

the reviewer must check:

- How is historical data populated?
- How is tomorrow's data populated?
- Where is the automation wired?
- What health check watches it?
- Where does Ops Brief or Daily Index surface it?
- What fails or warns if it regresses?
- What documentation tells Jerry how to repair it?
- What is the canonical identity for every durable row?
- Did any name/team fallback replace an available numeric ID?
- Are fallback joins visible as diagnostics instead of hidden foundations?

The mandatory review question is:

Does this remain correct tomorrow?

If not, the feature is incomplete.

## Done Definition

`Done` means:

- backfilled;
- generated daily;
- automated;
- health-checked;
- visible;
- regression-detected;
- documented;
- validated historically and currently.

Anything less is useful progress, but it is not complete.
