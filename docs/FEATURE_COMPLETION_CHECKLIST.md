# Feature Completion Checklist

Use this checklist for every new durable feature across Proppadia.

Reference: `docs/PROJECT_DOCTRINE.md`

## Required Answer

Before closing the feature, answer:

What keeps this field, artifact, report, or signal populated tomorrow?

If there is no clear answer, the feature is incomplete.

Also answer:

What is the canonical identity for this row?

If the row relies on a fallback, the fallback must be visible in the artifact or diagnostic output.

## Checklist

Use `[x]` only when the item is actually implemented and validated.

- [ ] Historical backfill complete for the intended date range.
- [ ] Current-day generation works.
- [ ] Daily/intraday/postgame automation is wired in the correct order.
- [ ] Health check exists for coverage, freshness, row counts, and core invariants.
- [ ] Ops Brief visibility exists if daily interpretation depends on the feature.
- [ ] Daily Index visibility exists for artifacts Jerry may need to open.
- [ ] Regression detection exists for stale, missing, zero-row, low-coverage, duplicate, or future-leak cases.
- [ ] Canonical identity fields are present where available.
- [ ] Identity provenance/fallback fields are present when canonical IDs are unavailable.
- [ ] Documentation names the source, producer command, output artifacts, cadence, downstream consumers, and repair command.
- [ ] Historical validation passed.
- [ ] Current-slate/current-day validation passed.
- [ ] Feature is explicitly marked `COMPLETE`.

## Review Notes

Feature name:

Owner / sport / lane:

Lifecycle state:

- [ ] exploratory
- [ ] research-active
- [ ] operational-research
- [ ] production-candidate
- [ ] production

Historical range:

Daily producer command:

Health check command:

Ops Brief section:

Daily Index section:

Repair command:

Canonical identity:

Identity proof fields:

Fallback used, if any:

Fallback visibility:

Known limitations:

## Code Review Gate

Reviewer must confirm:

- [ ] The change does not rely on a one-time backfill only.
- [ ] The daily workflow keeps the feature current.
- [ ] Missing/stale inputs cannot silently become normal-looking zeroes.
- [ ] The health check distinguishes source-not-ready from real failure.
- [ ] The change does not silently join by name when player/game IDs are available.
- [ ] Alias/name fallbacks are diagnostic and visible.
- [ ] Documentation includes the repair path.
- [ ] No production behavior changed unless the change explicitly requested it.

Final reviewer question:

Does this remain correct tomorrow?
