# Platform Feature Maturity Playbook

## Purpose

This playbook describes how Proppadia matures a platform capability from idea to production-grade dependency.

It is not MLB-specific. The pattern emerged from MLB work, but the lifecycle applies to NHL, NBA, NFL, and future sports whenever a new feature, data source, research lane, or operational capability is being introduced.

The central rule is simple: features earn promotion through evidence, not optimism. A useful idea is not a platform capability until its source, lineage, freshness, coverage, failure modes, daily automation, and operational meaning are understood.

This document records observed Proppadia practice and turns it into a standard. Some phases are already deeply proven by completed work. A few later phases are intentionally more aspirational because the platform has often stopped at research visibility or promotion review rather than full production migration.

## Core Principles

- Inventory before implementation.
- Understand lineage before changing formulas.
- Treat platform maturity as different from model improvement.
- Prefer passive observation before decision influence.
- Separate research visibility from operational guidance.
- Preserve explainability before creating scores.
- Row existence is not the same as row correctness.
- Current production behavior stays stable until evidence justifies change.
- Final upload schemas are locked unless schema migration is explicitly approved.
- Daily collection matters as much as historical backfill.
- Every promoted dependency needs health checks, provenance, and rollback.
- Reporting must represent populations honestly.
- Production follows evidence; research is allowed to ask questions.

## Standard Lifecycle

### Phase 1: Discovery

**Purpose**

Identify the question the capability is supposed to answer and decide whether it is a parking-lot idea, an active research thread, a platform foundation, or a production candidate.

**Typical deliverables**

- Problem statement
- Known non-goals
- Initial source/artifact list
- Research registry row or parking-lot note

**Exit criteria**

- The project has a clear name, scope, owner concept, and reason to exist.
- It is clear what production behavior must not change during discovery.

**Lessons learned**

Opportunity Consumption was preserved as a future idea without being forced into implementation. Environment v2 started with a question about whether offensive environment should eventually replace the second tier, not with a new formula.

### Phase 2: Inventory

**Purpose**

Find what already exists before adding anything new.

**Typical deliverables**

- Column inventory
- Artifact inventory
- Script/table/source map
- Coverage summary
- Initial maturity matrix

**Exit criteria**

- The team knows which fields exist, where they live, what is missing, and which gaps are source gaps versus propagation gaps.

**Lessons learned**

PA looked conceptually mature, but inventory showed it was not broadly retained downstream. BvP proved more mature because its payload and propagation were already better established.

### Phase 3: Lineage

**Purpose**

Trace how a value is created, transformed, retained, displayed, and used.

**Typical deliverables**

- Lineage audit
- Formula trace
- Producer and consumer map
- Current behavior statement
- Field direction explanation

**Exit criteria**

- The exact source path is known.
- The formula or transformation is documented.
- It is clear whether the field is context, research, or production decision logic.

**Lessons learned**

Starter Expected Hits Allowed could not be responsibly improved until the project confirmed the formula: pitcher base multiplied by offense factor. Review Aid Layers could not be explained until they were reclassified as provenance labels rather than A/A-style tiers.

### Phase 4: Data Integrity

**Purpose**

Verify that retained data is correct, current, and semantically safe to use.

**Typical deliverables**

- Source-of-truth comparison
- Identity and date semantics audit
- Mismatch trace
- Health check recommendations
- Failure-mode classification

**Exit criteria**

- Known data-quality risks are documented.
- The project knows whether row presence implies correctness.
- Hard gates and warning gates are separated.

**Lessons learned**

Completed Game Lineage Integrity showed that local player stats can exist while still differing from current official source lines. Pitcher game v4 inherited local player stats drift. That finding turned a BF question into a broader lineage project.

### Phase 5: Historical Recovery

**Purpose**

Recover historical values only when authoritative existing source data supports it.

**Typical deliverables**

- Recoverability assessment
- Hydration or reconstruction dry run
- Write summary when approved
- Before/after coverage
- No-overwrite manifest

**Exit criteria**

- Historical coverage is improved or the permanent gap is explained.
- No values are invented.
- Existing nonblank values are preserved unless an explicit correction project exists.

**Lessons learned**

Environment v1.1 first hydrated from existing snapshots, then reconstructed only after starter identity could be resolved safely. The process separated "source existed but was not retained" from "source never existed."

### Phase 6: Daily Collection

**Purpose**

Make the capability current every day, not just historically complete.

**Typical deliverables**

- Daily generation target
- Wrapper integration when appropriate
- Immutable daily artifacts
- Current-date health output
- Reconciliation-ready row design

**Exit criteria**

- The capability updates through normal daily operation.
- Stale-source failures are visible.
- Missing research artifacts are warning-only unless production depends on them.

**Lessons learned**

PA was not platform-ready while source freshness stopped at 2026-05-29. Environment v2-beta became more useful when daily profiles were captured and later reconciled instead of remaining a static research report.

### Phase 7: Passive Visibility

**Purpose**

Expose the capability beside existing decisions without letting it change those decisions.

**Typical deliverables**

- Diagnostic or research output fields
- Companion artifacts
- Coverage health
- Schema unchanged confirmation
- Documentation that the field is context, not a rule

**Exit criteria**

- Row counts are preserved.
- Final upload schemas remain unchanged.
- Operators and researchers can see the context without mistaking it for production logic.

**Lessons learned**

PA became a platform foundation by flowing into generator-owned non-upload diagnostics. Environment v2-alpha deliberately exposed components side by side instead of creating a score.

### Phase 8: Research Observation

**Purpose**

Measure whether the capability explains outcomes, separates profiles, or improves interpretation.

**Typical deliverables**

- Research dashboard
- Profile or bucket evaluation
- Live observation report
- Reconciled ledger
- Stability validation

**Exit criteria**

- The signal has been observed across windows.
- Small-sample and concentration risks are labeled.
- Research claims are not presented as production rules.

**Lessons learned**

Environment v2-beta showed how profile families can be studied without replacing the pitcher tier. Reporting Integrity showed that any population used in Ops must be explicit about whether it was known pregame.

### Phase 9: Controlled Experiments

**Purpose**

Test whether a capability could improve decisions under controlled, reversible conditions.

**Typical deliverables**

- Shadow cohort
- Experiment plan
- Baseline comparison
- Guardrails
- Rollback plan
- Promotion or retirement criteria

**Exit criteria**

- The experiment has a frozen population definition.
- It can be evaluated without changing production outputs unless explicitly approved.
- Failure does not contaminate production behavior.

**Lessons learned**

This phase is less mature than the earlier phases in the current repository. PA Opportunity Shadow Test work and research promotion reviews point toward this pattern, but many candidates remain in observation rather than true controlled production experiment.

### Phase 10: Promotion Review

**Purpose**

Decide whether a research capability is ready for more visibility, a controlled experiment, production candidacy, continued observation, or retirement.

**Typical deliverables**

- Promotion review
- Priority ranking
- Maturity matrix
- Evidence gaps
- Recommended next implementation step

**Exit criteria**

- The next status is explicit.
- The reason for promotion or non-promotion is documented.
- The project knows what evidence is still missing.

**Lessons learned**

The MLB O1.5/U1.5 Research Promotion Review separated attractive research from implementation candidates. The User O1.5 Filter Watch was retired because it failed representation integrity despite correct arithmetic.

### Phase 11: Production Rollout

**Purpose**

Promote a mature capability into production behavior only after evidence, rollback, monitoring, and communication are ready.

**Typical deliverables**

- Production change plan
- Schema or contract review when needed
- Backward compatibility plan
- Rollback/disable switch
- Operator-facing explanation
- Post-rollout monitoring

**Exit criteria**

- The production behavior change is explicit and reviewable.
- Uploads, selectors, models, thresholds, and reports have known impacts.
- A rollback path exists before launch.

**Lessons learned**

This phase should remain conservative. Many recent capabilities intentionally stopped before production because their evidence supported research visibility, not replacement behavior.

### Phase 12: Operational Monitoring

**Purpose**

Keep mature capabilities healthy after promotion or daily integration.

**Typical deliverables**

- Health reports
- Freshness checks
- Invariant checks
- Daily/weekly automation hooks
- Failure diagnostics
- Repair path
- Canonical documentation updates

**Exit criteria**

- The system can detect stale, missing, malformed, or misleading data.
- Failures are visible without weakening gates.
- The current operating surface stays focused on today's work.

**Lessons learned**

Morning Gate ordering, Feature Lineage health, PA health, and repository working-set reduction all reinforced the same point: operational visibility is scarce, and health checks must show the exact failing invariant rather than only a generic fail state.

## Typical Artifacts

- Discovery note
- Research registry row
- Inventory audit
- Column inventory
- Lineage audit
- Formula trace
- Source map
- Coverage report
- Data integrity audit
- Historical hydration or reconstruction plan
- Dry-run manifest
- Write manifest
- Daily health report
- Research dashboard
- Live observation ledger
- Promotion review
- Rollout checklist
- Rollback plan
- Operational health output
- Canonical state document
- Journal entry for major milestones

## Promotion Criteria

A feature should not move closer to production unless most of the following are true:

- The source of truth is known.
- Current daily freshness is verified.
- Historical coverage is measured.
- Lineage from raw input to displayed output is documented.
- Identity joins are stable and audited.
- Date semantics are understood.
- Missingness behavior is explicit.
- Failure modes are known.
- Research evidence supports the intended use.
- The exact row-level population can be frozen and explained.
- Production touchpoints are identified.
- Final upload schema impacts are known.
- Monitoring exists or is planned.
- Rollback is possible.

Promotion does not always mean production. A capability can be promoted from concept to active research, from research to daily observation, or from daily observation to controlled experiment without touching production decisions.

## Anti-Patterns

- Changing formulas before understanding lineage.
- Treating incomplete historical data as a stable foundation.
- Promoting research directly into Ops as if it were actionable.
- Reporting a proxy or counterfactual population without labeling it.
- Evaluating one market or side using rows sourced from a different side without disclosure.
- Treating row existence as data correctness.
- Weakening validation gates to increase coverage.
- Inventing values when authoritative source data is missing.
- Mutating final upload schemas for context fields.
- Depending on a feature that cannot be refreshed tomorrow.
- Letting historical artifacts compete with today's operational files.
- Creating black-box scores when component visibility would answer the question.

## Applying This Playbook To New Sports

New sports should begin at Phase 1, not at model optimization.

For NHL, NBA, NFL, or any future sport, the first questions should be:

- What is the sport-specific question?
- Which raw sources already exist?
- What does each field mean?
- Can the feature be refreshed daily?
- Can outcomes be reconciled?
- Can a row-level population be frozen before the event?
- What would make this safe enough to observe, but not yet use?

Only after inventory, lineage, integrity, and daily collection are understood should a new sport attempt production scoring or selector changes. Mature MLB work should serve as a template for process, not as a shortcut around sport-specific validation.

## Case Studies

### Feature Lineage

Feature Lineage Restoration demonstrated that a useful value is not mature until it survives the full path from source to diagnostics, candidates, reconcile rows, health, and reporting. It also showed that passive retention can improve explainability without changing production behavior.

### PA Foundation

PA started as opportunity research and became a platform foundation only after source freshness was restored, backfill was completed, daily refresh was wired, passive downstream propagation was added, and health could distinguish intentionally excluded upload paths from missing diagnostic coverage.

### BvP Restoration

BvP showed the value of treating a context payload as a platform capability even before it becomes decision logic. Its maturity came from retention and lineage, not from forcing it into a selector.

### Environment v2

Environment v2 showed the strongest version of "components before formulas." The project retained lineage, hydrated and reconstructed history, evaluated component quality, built alpha and beta dashboards, and created daily observation before any production migration.

### Starter Expected Hits Allowed

Starter Expected Hits Allowed showed why existing production logic should be audited before improvement. The work clarified pitcher base, offense factor, starter expected, team expected, and pitcher tier thresholds, then preserved the current blended baseline while identifying opportunity decomposition as the next research path.

### Pitcher BF

Pitcher BF demonstrated a safe foundation pattern: official source validation first, dry-run-only gate second, duplicate handling third, schema design fourth, and write mode deferred until local stat-line mismatches are understood.

### Reporting Integrity

Reporting Integrity converted a confusing operational section into a platform rule: every reported population must say whether it was actually knowable pregame. Correct arithmetic is not enough if representation is misleading.

### Completed Game Lineage Integrity

Completed Game Lineage Integrity showed that downstream foundations inherit upstream drift. If local completed-game stats differ from current official source data, BF, Environment, PA, and model research all need health checks that can see the discrepancy.

## Future Evolution

This playbook is a living engineering document. It should evolve when Proppadia completes new maturity patterns, promotes research into controlled experiments, or discovers new classes of operational failure.

Future updates should distinguish observed practice from aspirational guidance. The goal is not to create bureaucracy. The goal is to keep future features from skipping the steps that made prior platform capabilities trustworthy.
