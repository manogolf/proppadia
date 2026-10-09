# MLB Morning Home - 2026-10-08

## Good Morning

- Current Slate: `2026-10-08`
- Completed Slate: `2026-10-07`
- Generated (UTC): `2026-10-08T23:54:13+00:00`
- System Ready? `FAIL`
- Safe to Begin? `NO`
- Reason: One or more BLOCKER issues mean data cannot be trusted.

## Canonical Game Phase Control

- Status: `UNAVAILABLE_OR_BLOCKED`
- Reason: `PHASE_AUTHORITY_STALE:GAME_PHASE_AUTHORITY_STALE:requested=2026-10-08..2026-10-08;supported=2026-02-20..2026-10-05`
- All regular-season, late-season, postseason, close, metric, market, ROI, and agreement totals are fail-closed.
- Model/selector/publication: `NO_QUALIFIED_MLB_MODEL_SELECTOR_RANKING_QUICK_CARD_UNAVAILABLE`


## Moneyline Run-Bound Authority (Separate from Static Index Horizon)

- Latest retained Moneyline run evidence: `VERIFIED_SOURCE_BOUND`
- Run: `moneyline_20261008T233005119355Z_30449_82685602`; completed `2026-10-08T23:35:09.862210Z`
- Source-bound decisions: `749`; phase counts `{"POSTSEASON": 24, "REGULAR_SEASON": 725}`
- Evidence reason when unavailable: `none`
- This verifies only this completed Moneyline run; it does not extend the static shared index horizon or establish cross-lane/global phase authority. Unknown, conflicting, or unverified games remain fail-closed.
- Model readiness remains governed by qualified-model evidence; run-bound phase authority does not qualify a model.
- Scope note: all other counts on this home screen are navigation, availability, or source-health counts unless explicitly bound to an exact-gamePk phase partition.

▶ [Start Morning Review](../../mlb_daily_ops_brief_2026-10-08.md)

[Log Morning Timing](../../morning_timing_template.md)

## Morning Gate

- Operational Gate: `FAIL`
- Morning Workflow Health: `WARN` (`97.22%`)
- Warnings: `6` blocker, `4` major, `1` minor
- [View Ops Brief](../../mlb_daily_ops_brief_2026-10-08.md)
- [View Morning Gate Summary](../../morning_gate_summary.md)
- [View Morning Workflow Audit](../../morning_workflow_audit.md)

## Doctrine

- Home Screen answers: can I begin?
- Ops Brief answers: can I trust the system and what kind of baseball day is today?
- Morning Workbench answers: where should I spend attention?
- Candidate CSV answers: what exists today?
- Pivot answers: what is worth considering?

## Today's Workflow

- [ ] System healthy
- [ ] Ops Brief reviewed
- [ ] Baseball context calibrated
- [ ] Open Morning Workbench
- [ ] Open candidate CSV
- [ ] Pivot
- [ ] Record conclusions
- [ ] Production upload (future)

## Supporting Evidence

| item | open |
|---|---|
| Decision Performance | [open](../../review_aids/performance/review_aid_decision_performance_report.md) |
| Expanded Research | [open](../../expanded_o15_universe/expanded_o15_universe_rows.csv) |
| Analytics Ontology | [open](../../../../../docs/ANALYTICS_ONTOLOGY.md) |
| Research Snapshot | [open](../../../../..) |
| Review Aid Performance | [open](../../review_aids/performance/review_aid_performance_report.md) |
| Morning Timing Plan | [open](../../morning_time_to_first_insight_plan.md) |
| Morning Timing Log | [open](../../morning_timing_log.csv) |

## Operations

| item | open |
|---|---|
| Ops Brief | [open](../../mlb_daily_ops_brief_2026-10-08.md) |
| Preflight | [open](../../orchestration/mlb_daily_preflight_2026-10-08.md) |
| Identity | [open](../../identity/mlb_identity_health.md) |
| Invariants | [open](../../invariants/mlb_project_invariants_2026-10-08.md) |
| Morning Gate | [open](../../morning_gate_summary.md) |
| Morning Workflow Audit | [open](../../morning_workflow_audit.md) |
| Feature Lineage | [open](../../feature_lineage/daily_feature_lineage_health_2026-10-08.md) |

## Research

### Latest Snapshot

Latest research snapshot unavailable. Generate one with `make mlb-research-snapshot DATE=2026-10-08`.

### Active Threads

| research thread | status | current conclusion | next action | link | command |
|---|---|---|---|---|---|
| Tier A Failure / Final Review Vetoes | `active` | Within O1.5 Tier A, raw d7/d15 hit rate is not enough; d7 HRR < 3 is the strongest current veto candidate. | Surface HRR and related context on boards, keep tracking before turning it into any rule. | [Tier A Failure / Final Review Vetoes](../../review_aids/tier_a_failure_audit.md)<br>`artifacts/analysis/mlb/review_aids/tier_a_failure_audit.md` |  |
| Offensive Heat / Same-Lineup Tier A Clustering | `research-only` | Same-team Tier A clustering has been strongly positive but rare; needs sensitivity monitoring before use. | Treat as context/boost signal, not a filter; continue checking concentration by date/team. | [Offensive Heat / Same-Lineup Tier A Clustering](../../review_aids/tier_a_cluster_rarity_audit.md)<br>`artifacts/analysis/mlb/review_aids/tier_a_cluster_rarity_audit.md` |  |
| BvP / PvB Matchup Consensus | `monitoring` | BvP remains useful broadly, especially total_bases, but not a standalone production-change trigger. | Keep BvP/PvB collection and compare against non-BvP baseball features as live samples grow. | [BvP / PvB Matchup Consensus](../../review_aids/tier_a_offensive_heat_audit.md)<br>`artifacts/analysis/mlb/review_aids/tier_a_offensive_heat_audit.md` |  |
| Bullpen Path / Second-Hit Suppression | `active` | Team expected hits allowed and starter context separate 2-hit winners better than raw hits alone; bullpen path remains a likely missing context layer. | Audit whether bullpen/team context explains 1-hit losses in otherwise valid O1.5 candidates. | [Bullpen Path / Second-Hit Suppression](../../review_aids/tier_a_failure_audit.md)<br>`artifacts/analysis/mlb/review_aids/tier_a_failure_audit.md` |  |
| Expanded O1.5 Universe | `ACTIVE` | Expanded O1.5 is the canonical research universe; the active thread is Low-Attention +200s / Hidden Support rather than obvious-hot alternate profiles. | Track whether +200s low-attention candidates need support context to remain positive at best-price and BetOnline views. | [Expanded O1.5 Universe](../../expanded_o15_universe/expanded_o15_low_attention_signpost_audit.md)<br>`artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_low_attention_signpost_audit.md` | `make mlb-expanded-o15-low-attention-signpost-audit` |
| U1.5 Corrected Baseline | `monitoring` | Corrected deduped_union reconcile changed u1.5 accounting materially; use rebuilt performance, not stale pre-fix ROI. | Monitor Layer 4/3/2 and combined tiers using corrected reconcile only. | [U1.5 Corrected Baseline](../../review_aids/performance/review_aid_performance_rebuild_after_reconcile_fix.md)<br>`artifacts/analysis/mlb/review_aids/performance/review_aid_performance_rebuild_after_reconcile_fix.md` | `make mlb-review-aid-performance` |
| Total Bases Shadow Monitoring | `monitoring` | Balanced TB shadow remains too Over-aggressive; unweighted shadow is better calibrated but still research-only. | Keep shadow running and evaluate only after enough corrected-reconcile live outcomes accumulate. | [Total Bases Shadow Monitoring](../../model_quality/total_bases_shadow/reconcile_fix_recheck/total_bases_reconcile_fix_recheck.md)<br>`artifacts/analysis/mlb/model_quality/total_bases_shadow/reconcile_fix_recheck/total_bases_reconcile_fix_recheck.md` | `make mlb-total-bases-shadow-evaluation` |
| Reconcile / Artifact Integrity | `active` | deduped_union fixed silent row loss; policy audit is the reference for stale/duplicate market risk. | Keep preflight/index/link checks in daily flow; do not interpret performance from stale largest_rows reconcile. | [Reconcile / Artifact Integrity](../../reconcile/deduped_union_policy_audit_2026-06-23.md)<br>`artifacts/analysis/mlb/reconcile/deduped_union_policy_audit_2026-06-23.md` | `make mlb-daily-index DATE=2026-06-26` |

Retired/historical audits are intentionally not listed here. Use the MLB artifact map and manifest for archive navigation.

### Invariant Intake

| proposed | accepted not implemented | implemented | open | repo path |
|---:|---:|---:|---|---|
| `n/a` | `n/a` | `n/a` | [Invariant Backlog](../../invariants/invariant_backlog.md) | `artifacts/analysis/mlb/invariants/invariant_backlog.md` |

## Archives

These links preserve the old directory-style index for deeper navigation. They are not the morning workflow.

### Routine Review Board Files

| board | status | rows | mtime UTC | open | repo path |
|---|---|---:|---|---|---|
| O1.5 Simple Filter | `MISSING_INPUT` |  |  | `missing` | `artifacts/analysis/mlb/review_aids/hits_o15_simple_filter_2026-10-08.csv` |
| O1.5 Watch Candidates | `MISSING_INPUT` |  |  | `missing` | `artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_2026-10-08.csv` |
| O1.5 Layered Candidates | `MISSING_INPUT` |  |  | `missing` | `artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-10-08.csv` |
| U1.5 Favorite Audit | `MISSING_INPUT` |  |  | `missing` | `artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-10-08.csv` |
| O1.5 Alternate Discovery | `OPTIONAL_MISSING` |  |  | `missing` | `artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_2026-10-08.csv` |

> Alternate discovery is optional/research. To generate it for this slate:

```bash
make mlb-hits-o15-alternate-discovery-full DATE=2026-10-08
```

### Production / Upload Files

| item | status | rows | open | repo path |
|---|---|---:|---|---|
| Lane Selector | `MISSING_INPUT` |  | `missing` | `backend/mlb/exports/model_v2/lanes/today/2026-10-08/hits_lane_selector_2026-10-08.csv` |
| Ranking Upload Input | `MISSING_INPUT` |  | `missing` | `backend/mlb/exports/model_v2/lanes/today/2026-10-08/hits_lane_selector_2026-10-08_ranking_upload_input.csv` |
| Quick Card | `MISSING_INPUT` |  | `missing` | `backend/mlb/exports/model_v2/lanes/today/2026-10-08/quick_card_hits_2026-10-08.csv` |
| Lane Selector Report | `optional_missing` |  | `missing` | `backend/mlb/exports/model_v2/lanes/today/2026-10-08/hits_lane_selector_2026-10-08_daily_report.md` |
| Book Upload Base | `optional_missing` |  | `missing` | `backend/mlb/data/processed/mlb_uploads/2026-10-08/05_book_upload_base.csv` |
| Upload Manifest | `optional_missing` |  | `missing` | `backend/mlb/data/processed/mlb_uploads/2026-10-08/MANIFEST.md` |

### Completed-Slate Performance Files

| item | status | rows | open | repo path |
|---|---|---:|---|---|
| Reconcile Rows | `available` | 0 | [Reconcile Rows](../../execution_vs_model/2026-10-07/reconcile_rows.csv) | `artifacts/analysis/mlb/execution_vs_model/2026-10-07/reconcile_rows.csv` |
| O1.5 Decision Performance | `available` |  | [O1.5 Decision Performance](../../review_aids/performance/review_aid_decision_performance_report.md) | `artifacts/analysis/mlb/review_aids/performance/review_aid_decision_performance_report.md` |
| Review Aid Performance | `available` |  | [Review Aid Performance](../../review_aids/performance/review_aid_performance_report.md) | `artifacts/analysis/mlb/review_aids/performance/review_aid_performance_report.md` |
| Review Aid Performance JSON | `available` |  | [Review Aid Performance JSON](../../review_aids/performance/review_aid_performance_summary.json) | `artifacts/analysis/mlb/review_aids/performance/review_aid_performance_summary.json` |
| Full Slate Summary | `source-not-ready` |  | `missing` | `artifacts/analysis/mlb/execution_vs_model/2026-10-07/full_slate_summary.md` |
| Total Bases Shadow Evaluation | `available` | 2978 | [Total Bases Shadow Evaluation](../../model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json) | `artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json` |

### Optional / Research Files

| item | status | rows | open | repo path |
|---|---|---:|---|---|
| Expanded O1.5 Context Health | `available` |  | [Expanded O1.5 Context Health](../../expanded_o15_universe/expanded_o15_context_health_2026-10-08.md) | `artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_context_health_2026-10-08.md` |
| Expanded O1.5 Context Health JSON | `available` |  | [Expanded O1.5 Context Health JSON](../../expanded_o15_universe/expanded_o15_context_health_2026-10-08.json) | `artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_context_health_2026-10-08.json` |
| MLB Identity Health | `available` |  | [MLB Identity Health](../../identity/mlb_identity_health.md) | `artifacts/analysis/mlb/identity/mlb_identity_health.md` |
| MLB Identity Health JSON | `available` |  | [MLB Identity Health JSON](../../identity/mlb_identity_health_summary.json) | `artifacts/analysis/mlb/identity/mlb_identity_health_summary.json` |
| O1.5 Ontology Health | `available` |  | [O1.5 Ontology Health](../../ontology/ontology_health.md) | `artifacts/analysis/mlb/ontology/ontology_health.md` |
| O1.5 Ontology Health JSON | `available` |  | [O1.5 Ontology Health JSON](../../ontology/ontology_health.json) | `artifacts/analysis/mlb/ontology/ontology_health.json` |
| MLB Project Invariants | `available` |  | [MLB Project Invariants](../../invariants/mlb_project_invariants_2026-10-08.md) | `artifacts/analysis/mlb/invariants/mlb_project_invariants_2026-10-08.md` |
| MLB Project Invariants JSON | `available` |  | [MLB Project Invariants JSON](../../invariants/mlb_project_invariants_2026-10-08.json) | `artifacts/analysis/mlb/invariants/mlb_project_invariants_2026-10-08.json` |
| MLB Invariant Backlog | `available` |  | [MLB Invariant Backlog](../../invariants/invariant_backlog.md) | `artifacts/analysis/mlb/invariants/invariant_backlog.md` |
| MLB Invariant Backlog CSV | `available` | 1 | [MLB Invariant Backlog CSV](../../invariants/invariant_backlog.csv) | `artifacts/analysis/mlb/invariants/invariant_backlog.csv` |
| Expanded O1.5 Variable Importance | `available` |  | [Expanded O1.5 Variable Importance](../../expanded_o15_universe/expanded_o15_variable_importance.md) | `artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_variable_importance.md` |
| Expanded O1.5 Universe Rows | `available` | 6823 | [Expanded O1.5 Universe Rows](../../expanded_o15_universe/expanded_o15_universe_rows.csv) | `artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv` |
| Alternate Source Rows | `OPTIONAL_MISSING` |  | `missing` | `artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery/2026-10-08/live_alternate_book_level_rows.csv` |
| Alternate Source Report | `OPTIONAL_MISSING` |  | `missing` | `artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery/2026-10-08/live_alternate_discovery_report.md` |
| Alternate Slate Coverage Audit | `OPTIONAL_MISSING` |  | `missing` | `artifacts/analysis/mlb/review_aids/alternate_discovery_slate_coverage_2026-10-08.md` |

### Warnings / Missing Inputs

- Latest research snapshot missing: `artifacts/analysis/mlb/research_snapshots/snapshot_manifest.csv`
- Preflight status is `fail`.
- MLB canonical identity health is `warn`; warning artifacts: `17`.
- O1.5 ontology health is `fail`; invalid rows=0.
- MLB project invariants are `fail`; fail=6, warn=2.
- MLB invariant backlog summary missing: `artifacts/analysis/mlb/invariants/invariant_backlog_summary.json`.
- O1.5 Simple Filter: `MISSING_INPUT`
- O1.5 Watch Candidates: `MISSING_INPUT`
- O1.5 Layered Candidates: `MISSING_INPUT`
- U1.5 Favorite Audit: `MISSING_INPUT`
- O1.5 Alternate Discovery: `OPTIONAL_MISSING`. Run `make mlb-hits-o15-alternate-discovery-full DATE=2026-10-08` to refresh it.
- Broken required link: O1.5 Simple Filter -> `artifacts/analysis/mlb/review_aids/hits_o15_simple_filter_2026-10-08.csv`
- Broken required link: O1.5 Watch Candidates -> `artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_2026-10-08.csv`
- Broken required link: O1.5 Layered Candidates -> `artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-10-08.csv`
- Broken required link: U1.5 Favorite Audit -> `artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-10-08.csv`
- Broken required link: Lane Selector -> `backend/mlb/exports/model_v2/lanes/today/2026-10-08/hits_lane_selector_2026-10-08.csv`
- Broken required link: Ranking Upload Input -> `backend/mlb/exports/model_v2/lanes/today/2026-10-08/hits_lane_selector_2026-10-08_ranking_upload_input.csv`
- Broken required link: Quick Card -> `backend/mlb/exports/model_v2/lanes/today/2026-10-08/quick_card_hits_2026-10-08.csv`
- Optional/research missing links: `8`; see `index_link_check.csv`.

### Navigation / Maps

| item | open | repo path |
|---|---|---|
| MLB Artifact Map | [MLB Artifact Map](../../README.md) | `artifacts/analysis/mlb/README.md` |
| Review Aids Map | [Review Aids Map](../../review_aids/README.md) | `artifacts/analysis/mlb/review_aids/README.md` |
| Review-Aid Performance Map | [Review-Aid Performance Map](../../review_aids/performance/README.md) | `artifacts/analysis/mlb/review_aids/performance/README.md` |
| Orchestration Map | [Orchestration Map](../../orchestration/README.md) | `artifacts/analysis/mlb/orchestration/README.md` |
| Feature Lineage Map | [Feature Lineage Map](../../feature_lineage/README.md) | `artifacts/analysis/mlb/feature_lineage/README.md` |
| Model Quality Map | [Model Quality Map](../../model_quality/README.md) | `artifacts/analysis/mlb/model_quality/README.md` |
| Artifact Manifest | [Artifact Manifest](../../artifact_manifest.csv) | `artifacts/analysis/mlb/artifact_manifest.csv` |
| Phase 2 Move Candidates | [Phase 2 Move Candidates](../../artifact_cleanup_phase2_candidates.csv) | `artifacts/analysis/mlb/artifact_cleanup_phase2_candidates.csv` |
| Cleanup Plan | [Cleanup Plan](../../artifact_cleanup_plan.md) | `artifacts/analysis/mlb/artifact_cleanup_plan.md` |
| Index Link Check | [Index Link Check](index_link_check.csv) | `artifacts/analysis/mlb/daily/2026-10-08/index_link_check.csv` |

## Link Check

- Broken required links: `7`

## MLB Phase Reporting Classifications

- Regular-season close readiness: `UNAVAILABLE_OR_BLOCKED`
- Late-season regular-season completeness: `UNAVAILABLE_OR_BLOCKED`
- Postseason collection status: `UNAVAILABLE_OR_BLOCKED`
- Postseason evaluation status: `UNAVAILABLE_OR_BLOCKED`
- Game-type integrity: `FAIL_CLOSED`
- Outstanding games: `UNAVAILABLE_OR_BLOCKED`
- Requests and credits: `REPORTING_USED_ZERO_REQUESTS_ZERO_PAID_CREDITS`
- Agreement-study status: `UNAVAILABLE_OR_BLOCKED`
- Model/selector/publication status: `NO_QUALIFIED_MLB_MODEL_SELECTOR_RANKING_QUICK_CARD_UNAVAILABLE`
- Offseason readiness: `UNAVAILABLE_OR_BLOCKED`
