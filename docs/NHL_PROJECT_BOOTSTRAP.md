# NHL Project Bootstrap

NHL work must follow the Proppadia Project Doctrine from day one.

References:

- `docs/PROJECT_DOCTRINE.md`
- `docs/FEATURE_COMPLETION_CHECKLIST.md`

## Operating Standard

Every NHL feature must be designed as a durable daily system, not a one-time research result.

Before marking any NHL feature complete, answer:

What keeps this field, artifact, report, or signal populated tomorrow?

If that answer is missing, the feature is incomplete.

## Required Lifecycle

For every NHL feature that adds columns, artifacts, reports, derived fields, model features, reconciliation fields, or research boards:

1. Backfill the historical window needed for research or validation.
2. Wire daily generation for current and future slates.
3. Add automation in the correct workflow phase.
4. Add health checks for coverage, freshness, row counts, and core invariants.
5. Add Ops Brief visibility when operator interpretation depends on it.
6. Add Daily Index or equivalent navigation visibility for daily artifacts.
7. Add regression detection for stale/missing/zero-row/low-coverage cases.
8. Document sources, commands, outputs, cadence, downstream consumers, and repair commands.
9. Validate historical and current outputs.

## NHL-Specific Expectations

NHL should not repeat the MLB pattern where a field is backfilled once and then slowly becomes incomplete because daily automation never sustained it.

For NHL, any durable work item should include:

- source data lineage;
- player/team/game identity strategy;
- no-future-data rule where applicable;
- current-slate generation command;
- postgame/reconcile path if outcomes are used;
- health thresholds;
- daily repair command;
- documentation link.

## Completion Rule

An NHL feature is complete only when it is:

- historically filled;
- daily-sustained;
- automated;
- health-checked;
- visible;
- regression-monitored;
- documented;
- validated.

Anything less is progress, not completion.
