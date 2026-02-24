# Supabase Optimization Planning Note

Purpose: capture a return point for database/performance optimization planning before regular-season traffic and workload growth increase read/write pressure.

## Why This Matters (Recorded Intent)

- As games go live and daily activity increases, Supabase load will grow:
  - reads
  - writes
  - inserts
  - upserts
- This will increase over time as row counts and active workflows grow.

This note is a placeholder to revisit the topic at the right time with a focused optimization plan.

## Revisit Triggers (When to Bring This Back)

Revisit this subject when one or more of these begins to show up:

- daily cron/runtime durations begin trending up materially
- API response times noticeably degrade on MLB/NHL pages
- Supabase dashboard shows rising DB CPU / I/O / connection pressure
- write-heavy jobs (upserts/inserts) begin timing out or requiring retries
- regular-season volume starts and workload becomes representative

## Initial Optimization Areas To Evaluate (Draft Ideas)

### 1. Query + Index Audit (Highest Priority)

- Identify hottest read/write queries in active MLB/NHL pipelines and frontend APIs
- Verify indexes for:
  - join keys
  - `ON CONFLICT` targets (upsert paths)
  - frequent date filters (`game_date`, `slate_date`, etc.)
  - common `(sport, date, prop_type)` access patterns
- Review index bloat / redundant indexes

### 2. Write Path Efficiency

- Batch inserts/upserts where safe
- Reduce duplicate writes / unnecessary rewrites
- Confirm conflict keys match actual data model and usage
- Stage-then-merge patterns for heavy ingests (where useful)

### 3. Table Growth / Retention / Archival

- Review growth-heavy tables and retention policy by sport
- Decide what stays hot vs archive/cold
- Use game-type/date scoping consistently (preseason vs regular season where applicable)

### 4. API Read Reduction / Caching

- Cache expensive read endpoints used by dashboards/pages where freshness allows
- Reuse derived artifacts (CSV/materialized outputs) instead of repeated DB recomputation
- Confirm frontend pages avoid duplicate fetches on initial load/navigation

### 5. Operational Visibility (Before Tuning)

- Establish a baseline before broad optimizations:
  - cron durations
  - slowest queries
  - high-frequency queries
  - API latency on key pages
- Track changes after each optimization (avoid unmeasured tuning)

## Guardrails For Future Work

- Optimize based on measured bottlenecks, not assumptions
- Keep product behavior stable while optimizing (especially prediction pipelines)
- Prefer incremental changes with before/after metrics

## Return Point

When regular-season volume is active and "things are humming," return here and create a sport-aware Supabase optimization plan with:

- measured bottlenecks
- prioritized changes
- rollout order
- rollback plan for risky DB changes
