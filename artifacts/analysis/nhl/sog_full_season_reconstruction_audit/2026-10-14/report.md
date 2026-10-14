# NHL SOG full-season reconstruction feasibility audit

Audit date: 2026-10-14. Scope is repository season 2025, canonical regular season only. Local artifacts only; no provider/network calls, paid credits, or database writes.

## Finding

Canonical phase filtering uses repository authority `backend/nhl/game_phase.py` (types 1/2/3 = preseason/regular/postseason); the retained 2025 schedule snapshot contains only type 2. See `phase_boundary_inventory.csv`. The retained canonical schedule contains **1,312 games**, from **2025-10-07** through **2026-04-16**. It spans 167 game dates. Nonempty canonical-key feature rows exist on 163 dates; scheduled dates with empty exports are: 2025-10-07, 2025-10-08, 2025-10-09, 2025-10-11. December is not a wholesale archive outage: 28 December game dates are present and 28 have feature exports; Dec. 24–26 have no canonical games. The Feb. 6–24 Olympic break has no canonical game rows in the retained schedule.

The feature exports and official history can support a retrospective computation, but the non-certified daily feature archives lack immutable per-game prediction/input-cutoff evidence. Therefore strict-prior chronology cannot be proven for dates outside the certified replay. Those records are TIER 3 and are excluded from structural metrics. No new TIER 2 rows are admitted.

## Coverage and usable population

| Measure | Result |
|---|---:|
| Canonical regular-season games | 1,312 |
| Certified player-games (TIER 1) | 13,389 |
| Certified line rows | 40,167 |
| Additional validated player-games (TIER 2) | 0 |
| Combined usable player-games | 13,389 |
| Combined usable line rows | 40,167 |
| Non-certified feature player-games retained as unsafe (TIER 3) | 39,701 |
| Usable / retained official appeared-player-game rows (reference only) | 28.7% |

This percentage uses the 46,713 official appearances in the retained history snapshot as a reference denominator; it is not a full expected-skater denominator.

See `monthly_coverage.csv`, `canonical_season_coverage.csv`, `source_availability_matrix.csv`, `reconstruction_eligibility.csv`, `blocked_row_reasons.csv`, and `trust_tier_assignments.csv`. Dates/months outside the certified late-season window are not usable for full-season accuracy or structural evaluation. Team-specific rows are descriptive archive counts only; roster/call-up/transaction causes are not inferred.

## December map

December has 28 canonical game dates, feature archives on 28 such dates, and 9,371 archived participant feature rows, with zero TIER 1/2 usable rows. Exact eligible-player-game loss is **not identifiable** because the full-season pregame expected-skater denominator was not retained. The 9,371 December player-game feature rows are TIER 3 and excluded; this is the exact excluded artifact count, not a claim about every expected skater. No missing date is interpolated. Each date and classification is in `december_gap_map.csv`.

## Frozen baseline contract

Rate priority: D10, D20, D5. Exposure priority: D10 TOI, D20 TOI, D5 TOI, season 5v5 + PP, season 5v5 + 5v4. Lambda is selected rate × exposure minutes / 60. Poisson survival probabilities use count thresholds 2, 3, 4 for lines 1.5, 2.5, 3.5. The production scorer is `backend/nhl/scripts/score_sog_poisson_baseline.py`; exact SHA-256 is in `lineage_manifest.json`. A replay is valid only with these choices and strict-prior inputs.

## Certified bridge and offline diagnostics

The repository frozen reproduction utility loaded prepared inputs for all 40,167 certified line rows: 1,376 bit-exact probability rows, 40,167 within 1e-12, 0 material mismatches, and 40,167 side matches. Maximum absolute probability difference: 5.83e-16. The matched source coverage validates only the TIER 1 bridge.

Frozen corrections can be recomputed for the 13,347 TIER 1 player-games with retained raw rate/exposure fields (40,041 line rows). The other 42 certified player-games still have valid predictions included in the 40,167-row parity bridge but lack enough raw inputs for correction re-derivation. On the paired subset:

| Line | Production Brier | Midpoint Brier | Midpoint + soft Brier | Production MAE | Midpoint MAE | Midpoint + soft MAE |
|---:|------:|------:|---:|---:|---:|---:|
| 1.5 | 0.218443 | 0.214490 | 0.214135 | 1.0354 | 1.0211 | 1.0169 |
| 2.5 | 0.149225 | 0.146856 | 0.146294 | 1.0354 | 1.0211 | 1.0169 |
| 3.5 | 0.081676 | 0.080215 | 0.079611 | 1.0354 | 1.0211 | 1.0169 |

See `certified_frozen_correction_metrics.csv`, `month_level_metrics.csv`, `divergence_replication_metrics.csv`, and `high_rate_replication_metrics.csv`. These are certified-window-only diagnostics. Expanded-sample divergence/high-D10 assessments, month stability outside TIER 1, and gap sensitivity cannot be evaluated because TIER 2 is empty. Findings A–D are `CANNOT_EVALUATE` for the broader season; this audit neither confirms nor reverses a full-season effect.

## Shadow and backlog decision

Continue divergence-shadow collection: **supported**, based on the existing certified/prospective rationale; this audit adds no expanded historical confirmation and makes no promotion recommendation. High-D10 shadow implementation: **not advanced by this audit**; hold implementation pending strict-prior historical reconstruction or prospective evidence. Preserve backlog: historical official-TOI binding (raised priority because it blocks expansion); prospective divergence-shadow grading; fixed-blend collection; Puck Line deferred until tomorrow; reusable cross-prop sanity/explanation audit beginning with Points. No backlog items were executed.

## Reproducibility and safety

Provider calls: 0. Paid credits: 0. Production database mutations: 0. Production and shadow definitions were not modified. `certified_bridge/` contains the independent certified replay outputs.
