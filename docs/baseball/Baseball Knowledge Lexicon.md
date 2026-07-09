# Baseball Knowledge Lexicon

## Purpose

This document defines the canonical baseball language used throughout Proppadia.

Proppadia has evolved from a collection of baseball statistics into a framework of interacting baseball concepts. This lexicon explains how the platform understands baseball: how hitters inherit context, how pitchers create or suppress opportunity, how team environment interacts with individual ability, and how research concepts earn the right to influence production.

Future engineering and research work should reference this document rather than redefining these concepts in new reports.

## How To Use This Document

This document defines concepts.

Engineering documentation explains implementation.

Research documents provide evidence.

The Project Journal explains evolution.

When a future project introduces a baseball idea, it should either map to an existing concept here or explicitly propose a new concept for the lexicon. A concept can be important before it is production-ready.

## Current Baseball Framework

The current historical Hits 1.5 framework emerged from the Starter Expected Hits Allowed and Hitter Context research branches.

**Foundational**

- hitter persistence
- pitcher/environment context

**Bridge**

- team-context ownership

**Confirmatory**

- PA opportunity

**Refinement**

- lineup role quality

**Diagnostic**

- offense-factor movement
- odds context

The major discovery is that team offense is not inherited equally. A strong offensive environment does not lift every hitter the same way. The cleanest O1.5 baseball story is not simply good pitcher environment plus good offense. It is a strong hitter, a supportive pitcher environment, team context the hitter actually owns, enough opportunity, and an appropriate role.

## Relationship Diagrams

```text
Persistence
  ↓
Ownership
  ↓
Opportunity
  ↓
Role Quality
  ↓
Context Confirmation
```

```text
Pitcher Base
  ↓
Pitcher Environment
  ↓
Starter Expected Hits Allowed
  ↓
Hitter Context
  ↓
Prediction Context
```

```text
Historical Evidence
  ↓
Engineering Maturity
  ↓
Research Observation
  ↓
Production Consideration
```

## Research Philosophy

Proppadia prefers discovering baseball concepts from historical evidence rather than inventing them from intuition.

Concepts mature through:

```text
historical evidence
  ↓
engineering maturity
  ↓
research observation
  ↓
production consideration
```

Research is allowed to ask questions. Production follows evidence. A concept can be useful for explaining baseball before it is safe to use for decisions.

---

## Persistence

**Definition**

Persistence is the degree to which a hitter's recent production reflects durable baseball identity rather than a temporary hot streak.

**Plain-English Explanation**

A player can have a good week without being a reliable multi-hit threat. Persistence asks whether the hitter keeps showing the same hit-producing shape across multiple windows.

**Why It Matters**

Persistence is foundational for Hits 1.5. Team context can help a hitter, but it works best when the hitter already has a real individual hit profile.

**How Proppadia Measures It Today**

Primarily through retained recent hit-form fields such as d7, d15, and d30 hits per game, plus related rolling hit-rate and two-hit persistence research where available.

**What It Does NOT Mean**

Persistence does not mean guaranteed hits. It does not mean opportunity volume. It does not mean team strength.

**Depends On**

Completed-game stat lineage, player identity, rolling-window integrity.

**Supports**

Hitter context, ownership interpretation, O1.5/U1.5 research labels, promotion reviews.

**Research Status**

Active and historically supported as a foundational hitter layer.

**Future Research**

Separate one-hit floor from true multi-hit persistence more cleanly.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/hitter_context_hierarchy_2026-07-05/`

**Related Concepts**

Opportunity, Ownership, Context Confirmation.

## Opportunity

**Definition**

Opportunity is the number and quality of chances a hitter has to express a baseball skill in a game.

**Plain-English Explanation**

Even a strong hitter needs enough chances. Opportunity asks how many trips or usable chances the game is likely to give him.

**Why It Matters**

Hits O1.5 requires more than ability. It requires enough chances to collect multiple hits.

**How Proppadia Measures It Today**

Through PA fields where available, rolling PA context, lineup role when available, and historical opportunity-stability research.

**What It Does NOT Mean**

Opportunity is not talent. It is not role quality by itself. It is not the same as official at-bats.

**Depends On**

PA foundation, lineup role, batting order, completed-game lineage.

**Supports**

PA opportunity labels, low-PA research, role-quality research, O1.5 false-upgrade explanation.

**Research Status**

Active research foundation; increasingly platform-supported through PA restoration.

**Future Research**

Opportunity Consumption: how walks, HBP, sacrifices, errors, and fielder's choices spend plate appearances differently.

**Engineering References**

`artifacts/analysis/mlb/pa_foundation/`

**Related Concepts**

Plate Appearances, Role Quality, Expected Opportunity.

## Plate Appearances

**Definition**

Plate appearances are the broad count of hitter trips to the plate, including events that do not count as at-bats.

**Plain-English Explanation**

PA is the platform's main measure of how often a hitter gets a chance to do something. A walk consumes a PA but does not count as a hit opportunity in the same way an at-bat does.

**Why It Matters**

PA gives a better opportunity foundation than AB-only context because it sees more ways a hitter participates in the game.

**How Proppadia Measures It Today**

Through restored PA source fields and rolling d7/d15/d30 PA context in research and diagnostic artifacts.

**What It Does NOT Mean**

PA does not mean talent. PA does not mean hits. PA does not distinguish all opportunity-consumption outcomes by itself.

**Depends On**

PA source freshness, player stats lineage, rolling stat generation.

**Supports**

Opportunity, low-PA research, opportunity stability, passive diagnostic propagation.

**Research Status**

Platform foundation for passive context; not a production decision rule.

**Future Research**

Separate PA from AB-per-PA and outcome-specific opportunity consumption.

**Engineering References**

`artifacts/analysis/mlb/pa_foundation/`

**Related Concepts**

Opportunity, Expected Opportunity, Opportunity Consumption.

## Role Quality

**Definition**

Role Quality describes how a hitter's place in the lineup shapes the kind of opportunity he receives.

**Plain-English Explanation**

Two hitters may both get plate appearances, but batting near the top or middle of the order is different from batting at the bottom. Role affects how a hitter captures team environment.

**Why It Matters**

Role quality helped explain why some high-PA false O1.5 upgrades failed and why complete O1.5 stories were cleaner with top/middle-order context.

**How Proppadia Measures It Today**

Historically through postgame actual lineup slot reconstructed from StatsAPI boxscore `battingOrder`. Pregame capture is now a dry-run research project.

**What It Does NOT Mean**

Role quality is not PA volume. It is not hitter skill. Postgame actual role is not confirmed pregame role.

**Depends On**

Lineup slot source semantics, pregame lineup capture, postgame actual comparison.

**Supports**

Hitter context hierarchy, false-upgrade explanation, future Workbench context consideration.

**Research Status**

Historically supported; prospective pregame capture is in dry-run maturity.

**Future Research**

Measure confirmed pregame lineup availability, stability, and accuracy before first pitch.

**Engineering References**

`artifacts/analysis/mlb/pregame_lineup_capture/`

**Related Concepts**

Lineup Slot, Opportunity, Context Confirmation.

## Lineup Slot

**Definition**

Lineup slot is the batter's position in the batting order, usually 1 through 9.

**Plain-English Explanation**

Lineup slot is the concrete field behind role quality. Top order, middle order, and bottom order are role buckets.

**Why It Matters**

Lineup slot helps explain whether a hitter has a role that can capture team offense and enough opportunities for Hits 1.5.

**How Proppadia Measures It Today**

Postgame actual lineup slot can be reconstructed from StatsAPI boxscore `battingOrder`. A dry-run pregame runner is measuring confirmed pregame availability.

**What It Does NOT Mean**

Lineup slot does not mean projected lineup unless explicitly labeled. Postgame actual lineup slot does not mean the information was known before first pitch.

**Depends On**

StatsAPI boxscore payloads, source timestamps, pregame vs postgame semantics.

**Supports**

Role Quality, Pregame Lineup Capture, Hitter Context.

**Research Status**

Active platform maturity project.

**Future Research**

Compare confirmed pregame lineup to postgame actual lineup and measure late changes.

**Engineering References**

`backend/mlb/scripts/dry_run_capture_pregame_lineups.py`

**Related Concepts**

Role Quality, Historical Reconstruction, Passive Visibility.

## Ownership

**Definition**

Ownership describes how much of a team's offensive environment genuinely belongs to an individual hitter.

**Plain-English Explanation**

A strong offense does not lift every hitter equally. Ownership distinguishes hitters who drive the offense from hitters who merely participate in it.

**Why It Matters**

Ownership became the bridge layer between team context and individual hitter outcomes. It explains why some high-offense environments create true O1.5 upgrades and others create false upgrades.

**How Proppadia Measures It Today**

Through historical team-context ownership research, including hitter share of team hit environment and ownership buckets retained in behavior audits.

**What It Does NOT Mean**

Ownership is not simply team strength. It is not just a hitter's raw hits. It does not mean the hitter controls all team outcomes.

**Depends On**

Hitter persistence, team offense context, completed-game hit lineage.

**Supports**

Context Confirmation, O1.5 false-upgrade analysis, Hitter Context Hierarchy.

**Research Status**

Historically supported as a bridge concept.

**Future Research**

Carry ownership labels prospectively with clean lineage and pregame lineup context.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/team_context_ownership_2026-07-05/`

**Related Concepts**

Team Context, Offense Factor, Persistence.

## Team Context

**Definition**

Team Context is the offensive environment surrounding the hitter.

**Plain-English Explanation**

The hitter belongs to an offense. That offense may be hot, cold, deep, shallow, confirmed by the hitter, or carried by others.

**Why It Matters**

Team context can strengthen or weaken an individual hitter story, but only when interpreted through ownership and opportunity.

**How Proppadia Measures It Today**

Through offense factor, recent team hits/game, Environment v2 profiles, and team-context ownership research.

**What It Does NOT Mean**

Team context is not automatically inherited by every hitter. It is not lineup quality by itself. It is not team runs.

**Depends On**

Completed-game stat lineage, offense factor health, team identity.

**Supports**

Ownership, Offense Factor, Environment, Context Confirmation.

**Research Status**

Active and historically supported, with health/lineage guardrails.

**Future Research**

Separate true offense quality from schedule, opponent, park, lineup, and market effects.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/offense_factor_foundation_2026-07-05/`

**Related Concepts**

Ownership, Offense Factor, Environment v2.

## Offense Factor

**Definition**

Offense Factor is Proppadia's current team-level adjustment for recent offensive hit environment relative to league context.

**Plain-English Explanation**

It asks whether the hitter's team is producing more or fewer hits than league context would suggest, then uses that as part of the starter expected hits allowed environment.

**Why It Matters**

Offense factor is meaningful historically, but it is strongest when team context confirms the hitter's own profile rather than carrying a weak hitter.

**How Proppadia Measures It Today**

Through recent team hits/game blended across multiple windows and compared to league context, with lineage and health diagnostics retained in research artifacts.

**What It Does NOT Mean**

Offense factor is not team runs. It is not confirmed lineup strength. It is not individual hitter ownership.

**Depends On**

Team hits lineage, offense health guards, completed-game stat integrity.

**Supports**

Starter Expected Hits Allowed, Team Context, offense-factor movement diagnostics.

**Research Status**

Mature as a blended platform component; still observed as context rather than a standalone decision rule.

**Future Research**

Decompose when offense factor helps versus when it creates false upgrades.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/offense_factor_performance_stability_2026-07-05/`

**Related Concepts**

Team Context, Ownership, Starter Expected Hits Allowed.

## Pitcher Base

**Definition**

Pitcher Base is the starter-side baseline estimate of hits allowed before team offense adjustment.

**Plain-English Explanation**

It captures the starter's expected hit allowance shape. It blends how vulnerable the pitcher is with how long he tends to stay in the game.

**Why It Matters**

Pitcher Base is foundational to pitcher environment and starter expected hits allowed.

**How Proppadia Measures It Today**

Through the current pitcher expected hits allowed weighted foundation used by starter expected hits allowed.

**What It Does NOT Mean**

Pitcher Base is not pure pitcher skill. It is not pure workload. It is a productive blend of both.

**Depends On**

Pitcher stat lineage, starter identity, workload/vulnerability decomposition.

**Supports**

Pitcher Environment, Starter Expected Hits Allowed, pitcher tier context.

**Research Status**

Architecturally mature; current blend should remain baseline for now.

**Future Research**

Improve passive decomposition using official batters faced and starter opportunity metrics.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/pitcher_base_inventory_2026-07-05/`

**Related Concepts**

Pitcher Vulnerability, Pitcher Workload, Batters Faced.

## Pitcher Vulnerability

**Definition**

Pitcher Vulnerability is how easily a pitcher allows hits per unit of opportunity.

**Plain-English Explanation**

Some pitchers allow hits because they are hittable. Others allow hits because they face many batters. Vulnerability isolates the hittable part.

**Why It Matters**

It helps distinguish true O1.5 support from workload-only support.

**How Proppadia Measures It Today**

Through decomposition research using hits allowed per out/inning and related starter-quality slices.

**What It Does NOT Mean**

Vulnerability is not workload. It is not total hits allowed by itself.

**Depends On**

Pitcher game logs, outs, hits allowed, future BF foundation.

**Supports**

Pitcher Base decomposition, Starter Expected Hits Allowed research.

**Research Status**

Research-supported but not yet a production feature.

**Future Research**

Use official BF to express hits allowed per batter faced.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/pitcher_base_quality_eval_2026-07-05/`

**Related Concepts**

Pitcher Base, Pitcher Workload, Batters Faced.

## Pitcher Workload

**Definition**

Pitcher Workload is how much of the game a starter is expected to cover.

**Plain-English Explanation**

A pitcher who works deep into games creates more hitter opportunities against himself. Workload can raise expected hits allowed even when the pitcher is not especially vulnerable.

**Why It Matters**

Workload is part of why Pitcher Base is productive, but it should not be confused with hit vulnerability.

**How Proppadia Measures It Today**

Through outs per start, innings per start, and workload buckets in pitcher-base decomposition research.

**What It Does NOT Mean**

Workload is not pitcher weakness. It is not hitter quality.

**Depends On**

Outs recorded, starter identity, pitcher game-line lineage.

**Supports**

Pitcher Base, Expected Opportunity, BF foundation.

**Research Status**

Historically evaluated; stronger after BF backfill matures.

**Future Research**

Replace rough workload proxies with official batters faced and PA faced.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/pitcher_side_pa_bf_availability_2026-07-05/`

**Related Concepts**

Pitcher Vulnerability, Batters Faced, Expected Opportunity.

## Pitcher Environment

**Definition**

Pitcher Environment is the hitter-facing context created by the opposing starter and surrounding hit-allowance conditions.

**Plain-English Explanation**

It is the pitcher-side world the hitter enters: how many hits the starter is expected to allow after starter baseline and offense context are considered.

**Why It Matters**

Pitcher/environment context is foundational in the Hitter Context Hierarchy.

**How Proppadia Measures It Today**

Primarily through starter expected hits allowed, pitcher tier, Environment v2 profile families, and related matchup artifacts.

**What It Does NOT Mean**

Pitcher environment is not pure pitcher skill. It does not currently include every possible baseball condition such as weather, park, lineup, or bullpen in the starter tier driver.

**Depends On**

Pitcher Base, Offense Factor, Environment lineage.

**Supports**

Starter Expected Hits Allowed, Hitter Context, O1.5/U1.5 tier research.

**Research Status**

Mature enough as a platform concept; still researched for decomposition.

**Future Research**

Add passive fields for bullpen, park, weather, handedness, lineup, and market context before considering formula changes.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/starter_expected_hits_allowed_foundation_audit_2026-07-03.md`

**Related Concepts**

Pitcher Base, Offense Factor, Environment v2.

## Starter Expected Hits Allowed

**Definition**

Starter Expected Hits Allowed is Proppadia's current starter-level estimate of expected hits allowed in a matchup.

**Plain-English Explanation**

It combines the starter's baseline hit allowance with the opposing team's recent offensive hit environment.

**Why It Matters**

It drives pitcher tier context for Hits 1.5 and anchors the pitcher/environment side of the hitter-context stack.

**How Proppadia Measures It Today**

Through the established current blend of Pitcher Base and Offense Factor.

**What It Does NOT Mean**

It is not a full-game team/staff projection. It does not currently include bullpen, weather, park, handedness, confirmed lineup, BvP, PA, Vegas, or pricing.

**Depends On**

Pitcher Base, Offense Factor, local stat lineage, starter identity.

**Supports**

Pitcher tiers, Environment v2, Hitter Context, review-aid research.

**Research Status**

Architecturally mature; no formula change currently recommended.

**Future Research**

Continue decomposition and passive context retention before any production formula proposal.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/`

**Related Concepts**

Pitcher Environment, Pitcher Base, Offense Factor.

## Batters Faced

**Definition**

Batters Faced is the number of hitters a pitcher faces in a game.

**Plain-English Explanation**

BF is the pitcher-side opportunity count. It is the cleanest way to ask how much hitter exposure a starter actually created.

**Why It Matters**

BF can help separate workload from vulnerability in Pitcher Base.

**How Proppadia Measures It Today**

Official StatsAPI `pitching.battersFaced` has been validated in dry-run starter BF gates. Write mode is intentionally deferred.

**What It Does NOT Mean**

BF is not currently a production input. It is not inferred from aggregate approximations.

**Depends On**

StatsAPI official source, starter reconciliation, local stat-line integrity.

**Supports**

Pitcher Workload, Pitcher Vulnerability, Pitcher Base decomposition.

**Research Status**

Dry-run foundation validated; landing schema designed; write mode deferred.

**Future Research**

Resolve local stat-line mismatches before write-gated backfill.

**Engineering References**

`backend/mlb/scripts/dry_run_starter_bf_write_gate.py`

**Related Concepts**

Pitcher Workload, Expected Opportunity, Lineage.

## Expected Opportunity

**Definition**

Expected Opportunity is the anticipated chance volume available to a player before the game resolves.

**Plain-English Explanation**

For hitters, it asks how many usable plate appearances and role-based chances are likely. For pitchers, it asks how much exposure the starter is likely to create.

**Why It Matters**

Expected opportunity connects player skill to the game context that lets that skill appear.

**How Proppadia Measures It Today**

Through PA context, workload proxies, lineup role research, and starter BF design work.

**What It Does NOT Mean**

It is not the actual final opportunity count. It is not production certainty.

**Depends On**

PA, lineup slot, BF, starter workload, pregame capture maturity.

**Supports**

Opportunity, Role Quality, Pitcher Base decomposition.

**Research Status**

Partly mature; still split across hitter and pitcher-side foundations.

**Future Research**

Unify hitter-side and pitcher-side opportunity into passive research rows.

**Engineering References**

`artifacts/analysis/mlb/pa_foundation/`

**Related Concepts**

Opportunity, Plate Appearances, Batters Faced.

## Context Confirmation

**Definition**

Context Confirmation is the process of checking whether multiple baseball layers tell the same story.

**Plain-English Explanation**

A hitter is more interesting when persistence, pitcher environment, ownership, PA opportunity, and role all agree.

**Why It Matters**

The strongest O1.5 historical story came from a complete context stack rather than any single metric.

**How Proppadia Measures It Today**

Through research labels and behavior audits combining persistence, pitcher environment, offense factor, ownership, PA opportunity, and lineup role.

**What It Does NOT Mean**

It is not a black-box score. It is not automatic production approval.

**Depends On**

Clean concept labels, lineage, passive visibility.

**Supports**

Hitter Context Hierarchy, Workbench consideration, promotion reviews.

**Research Status**

Historically supported as a research framework.

**Future Research**

Evaluate with prospective pregame lineup capture and clean live observations.

**Engineering References**

`artifacts/analysis/mlb/starter_expected_hits_allowed/hitter_context_hierarchy_2026-07-05/`

**Related Concepts**

Persistence, Ownership, Opportunity, Role Quality.

## Research vs Production

**Definition**

Research is where Proppadia studies baseball ideas; production is where approved behavior affects operational decisions or uploads.

**Plain-English Explanation**

An idea can be true, interesting, and useful without being ready to change production.

**Why It Matters**

This separation prevents promising but immature findings from becoming unguarded decision rules.

**How Proppadia Measures It Today**

Through registry statuses, promotion reviews, artifact labels, and production guardrails.

**What It Does NOT Mean**

Research-only does not mean unimportant. Production-ready does not mean perfect.

**Depends On**

Platform maturity, lineage, health, rollback, evidence.

**Supports**

Promotion, Research Confidence, Production Confidence.

**Research Status**

Core operating doctrine.

**Future Research**

Continue sharpening criteria for Workbench observation versus controlled experiments.

**Engineering References**

`docs/engineering/Platform Feature Maturity Playbook.md`

**Related Concepts**

Promotion, Passive Visibility, Research Confidence.

## Historical Reconstruction

**Definition**

Historical Reconstruction is recovering past context from source data after games have occurred.

**Plain-English Explanation**

It lets Proppadia study concepts that were not retained at the time, but it must be labeled as historical and not represented as pregame-known evidence.

**Why It Matters**

Lineup role quality was historically useful through postgame actual reconstruction, but that does not make it pregame operational context.

**How Proppadia Measures It Today**

Through dry-run backfills, accepted/rejected manifests, source provenance, and semantic labels.

**What It Does NOT Mean**

Historical reconstruction does not mean the platform knew the value before first pitch.

**Depends On**

Source availability, exact join keys, lineage, representation integrity.

**Supports**

Behavior audits, concept discovery, future capture design.

**Research Status**

Accepted research method when clearly labeled.

**Future Research**

Compare historical reconstruction with prospective capture to measure operational availability.

**Engineering References**

`backend/mlb/scripts/dry_run_lineup_slot_backfill_prepass.py`

**Related Concepts**

Lineup Slot, Research vs Production, Lineage.

## Passive Visibility

**Definition**

Passive Visibility is exposing a concept beside existing decisions without allowing it to change those decisions.

**Plain-English Explanation**

It lets researchers and operators see context while production remains stable.

**Why It Matters**

Most platform concepts should become visible before they become influential.

**How Proppadia Measures It Today**

Through diagnostic artifacts, companion CSVs, health reports, and research rows.

**What It Does NOT Mean**

Passive visibility is not a recommendation. It is not a selector rule.

**Depends On**

Lineage, health, stable field definitions.

**Supports**

Research observation, promotion review, Workbench readiness.

**Research Status**

Standard platform maturity phase.

**Future Research**

Define which baseball labels deserve Morning Workbench visibility.

**Engineering References**

`docs/engineering/Platform Feature Maturity Playbook.md`

**Related Concepts**

Research Confidence, Promotion, Health.

## Lineage

**Definition**

Lineage is the known path from source data to baseball concept to artifact.

**Plain-English Explanation**

It answers where a value came from, how it was transformed, and whether it still means what the platform says it means.

**Why It Matters**

A row can exist and still be wrong, stale, or semantically unsafe.

**How Proppadia Measures It Today**

Through source maps, formula traces, field inventories, parity checks, and lineage health artifacts.

**What It Does NOT Mean**

Lineage is not coverage. It is not correctness by itself.

**Depends On**

Source inspection, artifact tracing, health checks.

**Supports**

Health, Production Confidence, Representation Integrity.

**Research Status**

Core platform discipline.

**Future Research**

Extend completed-game lineage integrity across hitter and pitcher stat families.

**Engineering References**

`artifacts/analysis/mlb/local_stat_lineage_integrity/`

**Related Concepts**

Health, Historical Reconstruction, Production Confidence.

## Health

**Definition**

Health is the platform's assessment of whether a concept's data is fresh, complete, correct enough, and semantically safe for its current use.

**Plain-English Explanation**

Health checks protect the platform from stale sources, broken lineage, missing artifacts, and misleading representation.

**Why It Matters**

Concepts mature only when their failure modes are visible.

**How Proppadia Measures It Today**

Through WARN/FAIL health reports, freshness checks, parity checks, lineage diagnostics, and preflight gates where appropriate.

**What It Does NOT Mean**

Health does not mean a concept improves outcomes. Health means the data layer is trustworthy for its intended role.

**Depends On**

Lineage, source freshness, coverage, validation policy.

**Supports**

Production Confidence, Promotion, Operational Monitoring.

**Research Status**

Required for platform maturity.

**Future Research**

Differentiate research-only WARNs from production blockers more consistently.

**Engineering References**

`docs/engineering/Platform Feature Maturity Playbook.md`

**Related Concepts**

Lineage, Promotion, Research Confidence.

## Promotion

**Definition**

Promotion is the process of moving a concept from research toward observation, controlled experiment, or production.

**Plain-English Explanation**

Promotion is earned. A concept advances when evidence, lineage, health, and rollback are strong enough for the next stage.

**Why It Matters**

It keeps baseball discovery connected to operational safety.

**How Proppadia Measures It Today**

Through promotion reviews, maturity matrices, research registry status, and explicit non-goals.

**What It Does NOT Mean**

Promotion does not always mean production. A concept can be promoted to Workbench observation or shadow testing first.

**Depends On**

Research confidence, production confidence, health, lineage.

**Supports**

Research vs Production, Platform Feature Maturity.

**Research Status**

Formalized as part of the platform methodology.

**Future Research**

Use the lexicon to make promotion decisions more concept-aware.

**Engineering References**

`artifacts/analysis/mlb/research_promotion/`

**Related Concepts**

Research Confidence, Production Confidence, Passive Visibility.

## Research Confidence

**Definition**

Research Confidence is confidence that a concept explains baseball outcomes under the studied conditions.

**Plain-English Explanation**

It says the idea appears real enough to keep studying or observe live.

**Why It Matters**

Many useful concepts begin as research confidence before becoming production candidates.

**How Proppadia Measures It Today**

Through historical sample size, live observation, ROI/WR stability, sample flags, and lineage quality.

**What It Does NOT Mean**

Research confidence is not production readiness.

**Depends On**

Historical evidence, clean rows, outcome reconciliation.

**Supports**

Promotion, Passive Visibility, future controlled experiments.

**Research Status**

Core evaluation concept.

**Future Research**

Create consistent confidence labels across sports and markets.

**Engineering References**

`docs/engineering/Platform Feature Maturity Playbook.md`

**Related Concepts**

Production Confidence, Promotion, Research vs Production.

## Production Confidence

**Definition**

Production Confidence is confidence that a concept is safe, monitored, reversible, and beneficial enough to influence operational behavior.

**Plain-English Explanation**

It is a higher bar than research confidence because production changes affect real decisions and uploads.

**Why It Matters**

Production must not follow unvalidated or poorly monitored concepts.

**How Proppadia Measures It Today**

Through production guardrails, upload schema checks, selectors, health gates, rollback plans, and promotion reviews.

**What It Does NOT Mean**

Production confidence does not mean the concept will always win. It means it is mature enough to use responsibly.

**Depends On**

Health, lineage, monitoring, rollback, evidence.

**Supports**

Production rollout, controlled experiments, operational monitoring.

**Research Status**

Guardrail concept rather than a baseball signal.

**Future Research**

Define controlled experiment standards for baseball concepts that pass Workbench observation.

**Engineering References**

`docs/engineering/Platform Feature Maturity Playbook.md`

**Related Concepts**

Research Confidence, Promotion, Health.

## Environment v2

**Definition**

Environment v2 is a research framework for categorizing hitter-facing matchup environment profiles.

**Plain-English Explanation**

It groups the conditions around a matchup into profile families, such as aligned high environment or starter-led with bullpen drag.

**Why It Matters**

It helps test whether environmental context survives live observation without replacing production decision logic.

**How Proppadia Measures It Today**

Through daily Environment v2-beta profile rows, reconciliation, ledger artifacts, and live observation reports.

**What It Does NOT Mean**

Environment v2 is not a production selector. It is not the same as pitcher tier.

**Depends On**

Starter expected hits allowed, team/starter context, daily capture, reconciliation.

**Supports**

Pitcher Environment, research observation, context labels.

**Research Status**

Collecting live data and reconciled research evidence.

**Future Research**

Continue live sample accumulation and profile-specific A/A and C/A behavior analysis.

**Engineering References**

`artifacts/analysis/mlb/environment_v2/`

**Related Concepts**

Pitcher Environment, Starter Expected Hits Allowed, Passive Visibility.

## Odds Context

**Definition**

Odds Context is the market price environment around a baseball candidate.

**Plain-English Explanation**

Odds can explain ROI and market difficulty, but they are not a baseball trait by themselves.

**Why It Matters**

The same baseball story can have different value depending on price.

**How Proppadia Measures It Today**

Through odds buckets and market fields retained in candidate/research artifacts where available.

**What It Does NOT Mean**

Odds context is not hitter skill, pitcher vulnerability, or team offense.

**Depends On**

Market data availability, upload/reconcile lineage.

**Supports**

ROI interpretation, diagnostic layers, production review.

**Research Status**

Diagnostic context.

**Future Research**

Study where baseball confidence and price confidence diverge.

**Engineering References**

`artifacts/analysis/mlb/review_aids/`

**Related Concepts**

Research Confidence, Production Confidence, Context Confirmation.

## Frequently Confused Concepts

- Persistence is not Opportunity.
- Opportunity is not Role.
- Ownership is not Team Strength.
- Pitcher Workload is not Pitcher Vulnerability.
- Pitcher Environment is not Pitcher Skill.
- Offense Factor is not Team Runs.
- PA is not Talent.
- Research Confidence is not Production Readiness.
- Historical Reconstruction is not Pregame Availability.
- Passive Visibility is not a Decision Rule.

## Future Concepts

These concepts are intentionally deferred until the platform has stronger evidence or cleaner source lineage:

- Opportunity Consumption
- Confirmed Pregame Lineup Stability
- Park Context
- Weather Context
- Handedness Context
- Bullpen Exposure
- Confirmed Lineup Strength
- Injury/Rest Context
- Market Disagreement

They belong in the lexicon only after the platform can define them consistently and distinguish research semantics from production use.

## Journal Notes

This lexicon should change slowly. Routine audits should not add entries. New entries belong here only when Proppadia's baseball language changes.
