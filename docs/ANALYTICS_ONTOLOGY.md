# Proppadia Analytics Ontology

This document defines the sport-portable vocabulary for Proppadia analytics artifacts. It is intentionally separate from MLB-specific implementation details.

The goal is to make every report answer the same basic question:

What level of analysis is this?

## Canonical Hierarchy

`Sport -> Universe -> Population -> Classification -> Candidate -> Outcome`

### Sport

The league/domain boundary.

Examples:

- MLB
- NHL

### Universe

A broad opportunity source or population boundary. A universe defines what kinds of opportunities can exist before any discretionary subset is applied.

Examples:

- MLB hits O1.5 main production-source universe
- MLB hits O1.5 alternate market universe
- MLB Expanded O1.5 Universe
- NHL shots-on-goal player prop universe

A universe should answer:

- What source feeds it?
- What market/prop lane does it cover?
- Is it production, review-only, research-only, or manual-only?
- What row identity defines one opportunity?

### Population

A saved subset within a universe. Populations are operational or research cuts of a universe.

Examples:

- O1.5 watch candidates
- O1.5 layered candidates
- U1.5 favorite audit rows
- QC uploaded rows
- QC placed rows
- user filter proxy rows
- alternate-only rows

A population should answer:

- What universe does it come from?
- What required filters define membership?
- Is it shown to Jerry, uploaded, reconciled, or only studied?

### Classification

A descriptive grouping applied to candidates or outcomes. Classifications should not imply row membership by themselves unless explicitly promoted into a population definition.

Examples:

- hitter tier A/B/C
- pitcher tier A/B/C/D/U
- combined tier A/A
- price bucket
- time-of-day bucket
- market classification profile
- low-attention score bucket
- BvP bucket

Classification answers:

- How should rows be grouped?
- What context explains performance?
- What warning/boost/veto profile applies?

### Candidate

One player-market opportunity row before outcome.

Minimum candidate identity should include:

- sport
- date
- game id when available
- player id when available
- prop type
- side
- line
- source/provenance

For MLB research market candidates, canonical identity should use:

`date + game_id + player_id + prop_type + side + line`

### Outcome

Resolved performance or quality measurement.

Examples:

- win/loss/push
- WR
- ROI
- units
- average odds
- calibration
- Brier/log loss/AUC
- matched/unmatched/reconciled counts

Outcome reports must clearly name the candidate population they grade.

## Supporting Levels

Some artifacts are not part of the decision hierarchy but are still durable.

### Provenance

Source and lineage facts that explain where a candidate came from.

Examples:

- from main source
- from alternate source
- from both
- book count
- OddsAPI market key
- Quick Card membership
- board/layer source
- canonical identity method

Provenance is not a decision category by default.

### Health

Coverage, freshness, completeness, and regression checks.

Examples:

- identity health
- context health
- feature lineage health
- preflight

### Orchestration

Execution order, automation, runbooks, wrapper traces, LaunchAgent checks.

### Invariant

Machine-checkable doctrine.

Examples:

- canonical identity coverage thresholds
- Daily Index must exist
- research snapshots are immutable

### Snapshot

Immutable research checkpoint: what the project believed at a point in time.

## Terms To Avoid Or Clarify

### Board

Use only for a human-facing candidate artifact. Prefer naming the population when discussing performance.

Better:

- `O1.5 Watch Candidate Population`
- `U1.5 Favorite Audit Population`

### Layer

Use as provenance/qualification metadata unless the report explicitly defines a population by layer.

Better:

- `qualification layer`
- `source layer`
- `population`

### Watch

Use for monitored populations, not for every interesting research cut.

### Filter

Use only when the exact predicate matters. If it becomes a durable saved subset, name it as a population.

### Discovery

Use for source visibility or opportunity discovery. Do not use discovery as a performance label unless the population is clearly defined.

### Shadow

Use only for alternate model/scoring outputs that do not affect production.

### Audit

Use for an evidence report answering a bounded question. If it becomes daily/standing, promote it to a named population, health check, or performance report.

## Naming Standard

New analytics artifacts should declare:

- ontology level;
- sport;
- lane/prop;
- universe;
- population if applicable;
- classification if applicable;
- primary audience;
- cadence;
- production impact.

## Row-Level Ontology Fields

O1.5 row-level artifacts now carry executable ontology metadata. Reports should read these fields instead of inferring meaning from filenames or section names.

| field | allowed values | meaning | example |
|---|---|---|---|
| `universe` | `main`, `alternate`, `expanded` | Broad opportunity boundary. | `expanded` |
| `population` | `simple_filter`, `watch`, `expanded_review`, `alternate_discovery`, `expanded_universe`, `main_only`, `alternate_only`, `shared`, `user_proxy`, `outside_proxy` | Saved subset or source slice inside the universe. | `alternate_only` |
| `classification_type` | `tier`, `price_bucket`, `opportunity`, `context`, `unclassified` | Primary classification family for the row. | `tier` |
| `classification_value` | controlled by `classification_type` | Specific value for the classification. | `A/A`, `C/A`, `201-250` |
| `opportunity_type` | `public_hot`, `context_supported_plus_money`, `quiet_hitter`, `unclassified` | Research-facing opportunity profile. Working values may evolve through ontology migrations. | `quiet_hitter` |
| `provenance_layer` | source/report specific labels | Qualification or source layer. This is provenance by default, not the primary decision category. | `Layer A`, `Layer 4`, `main_source` |
| `board_name` | artifact stem | Human-facing board or research artifact that emitted the row. | `hits_o15_watch_candidates` |
| `research_status` | `operational_research`, `manual_research`, `research_only`, `proxy_research` | How the row should be interpreted operationally. | `research_only` |

### Current O1.5 Population Mapping

| artifact | universe | population | research_status |
|---|---|---|---|
| `hits_o15_simple_filter_<DATE>.csv` | `main` | `simple_filter` | `operational_research` |
| `hits_o15_watch_candidates_<DATE>.csv` | `main` | `watch` | `operational_research` |
| `hits_o15_layered_candidates_<DATE>.csv` | `main` | `expanded_review` | `operational_research` |
| `hits_o15_alternate_discovery_<DATE>.csv` | `alternate` | `alternate_discovery` | `manual_research` |
| `expanded_o15_universe_rows.csv` | `expanded` | `main_only`, `alternate_only`, or `shared` | `research_only` |

### Health Contract

`make mlb-o15-ontology-health DATE=YYYY-MM-DD` checks current O1.5 row-level artifacts for:

- missing ontology fields;
- invalid allowed values;
- invalid universe/population combinations;
- missing provenance labels.

The health output lives at:

- `artifacts/analysis/mlb/ontology/ontology_health.md`
- `artifacts/analysis/mlb/ontology/ontology_health.csv`
- `artifacts/analysis/mlb/ontology/ontology_health.json`

Project invariants and the Daily Index consume the JSON status. A new report that adds candidate rows should either populate these fields or explicitly document why it is outside the ontology scope.

Before creating a new report, answer:

1. Is this a universe, population, classification, candidate list, outcome report, health check, orchestration doc, invariant, or snapshot?
2. What existing report does this overlap?
3. Should it appear in Ops Brief, Daily Index, both, or neither?
4. Is it temporary research or durable?
