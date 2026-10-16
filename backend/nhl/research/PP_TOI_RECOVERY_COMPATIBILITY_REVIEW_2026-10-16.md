# 2023–24 PP-TOI Recovery Compatibility Review — 2026-10-16

## Historical recovery compatibility

Replayed the corrected period-bounded manpower calculation and exact duplicate-shift-interval deduplication against all **71,627** previously accepted player-game recovery rows across **2,039 games**, using retained official NHL PBP, boxscore, and shiftchart responses. All required responses were present. Recalculation of the prior method reproduced all 71,627 original accepted values exactly before the recent calculation changes.

After applying the fixes, **32 values across 29 games** differ from the previous recovery candidate. All 32 are caused by duplicate official shift intervals that the earlier recovery summed more than once. Revised values are lower by **0.05–0.93 minutes**. An independent period-bounded interval implementation agrees with all 32 revised values. Deduplicated player shift totals remain within the accepted official boxscore TOI reconciliation bound for every affected row (maximum difference: 2 seconds).

The first replay also showed 22 small apparent increases (0.01–0.02 minutes). These were not source corrections: the active segment merger was bridging a one-second gap between noncontiguous PP intervals. The segment code now merges only overlapping or exactly adjacent intervals. The 22 values return to their prior accepted measurements and are excluded from the correction proposal. A regression test preserves a genuine one-second gap.

Read-only database comparison found all **71,627** recovery targets still present and matching the previous accepted values, including the 32 affected rows. No database values or historical recovery records were rewritten. The separate 32-row CSV is a bounded correction proposal only; execution requires separate authorization and fresh current-value guards.

The proposal is [pp_toi_recovery_compatibility_candidates_2026-10-16.csv](pp_toi_recovery_compatibility_candidates_2026-10-16.csv). It records numeric game/player/official-team IDs, old and recomputed values, delta, reason, and official PBP/boxscore/shiftchart source hashes.

## October 16 correction preparation

The prepared forward and rollback SQL are [repair_nhl_oct16_official_source_conflicts.sql](../sql/repairs/repair_nhl_oct16_official_source_conflicts.sql) and [rollback_nhl_oct16_official_source_conflicts.sql](../sql/repairs/rollback_nhl_oct16_official_source_conflicts.sql). They target exactly 8 shot-attempt fields and 2 PP-TOI fields. The guards pin the original and replacement values, expected SOG, numeric identities, unique official-to-database team crosswalks, source validation statuses and hashes, and the five unresolved PP-TOI values. The unresolved rows are guard-only and excluded from updates. Both procedures are idempotent and abort if current values or mappings have changed. Neither SQL file was executed.

Read-only validation confirmed all 10 candidate player-game rows still match captured original values; official boxscore player/team identities match; all seven involved team mappings are unique and agree with the official team IDs and abbreviations. The five unresolved PP-TOI values were read and remain outside the candidate set.

## Normal processing and source integration

The central daily CLI calls `backfill_game_manpower_segments.py` and then `fill_pp_toi_minutes_for_date.py`; postgame reconciliation calls the same fill script. The daily and postgame promoters separately preserve populated raw PP-TOI values and emit conflict evidence. The corrected fill extraction deduplicates exact shift intervals, and conflict comparison uses `numeric(5,2)` persistence precision.

A read-only replay of the active fill extraction SQL for the 214 retained October 16 PP-conflict keys recomputed all 214 rows and reported exactly the two source discrepancies: `(2026020111, 8480995)` 2.18 → 1.60 and `(2026020116, 8474567)` 1.62 → 1.47. The 207 independently reconciled matches no longer report precision-only conflicts. Five additional rows remain unresolved because their deduplicated shifts do not reconcile to official boxscore TOI; they remain unchanged and are not correction candidates.

## Evidence checksums

- Historical recovery candidates: `0bd6953c4152e3b29b0bc1af324816f5916d86066d494559a58f492eed4dcfef`
- Historical official acquisition manifest: `625f59f6d29fd7a54a2b4a5df7a630c2968f6e8bab36ad2f0b6bf88c6b7e22f2`
- 32-row compatibility proposal CSV: `4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631`
- October 16 official-source manifest: `628c0d7df5a8a0097b4f463690f8d85469dd60df4863058a1fbd17cd003672be`
- October 16 correction candidate CSV: `cf663c4ec6fa1a5759b9e450cc00341984d34ff2bd27425c598a677a8a49a048`
- Prepared forward SQL: `134ff42989d5f731e5c747a060ca482de1998901329a1d7122e694e055408a32`
- Prepared rollback SQL: `186b0d6dfa851785a55e2005dc91d94809217ab89e4cae724cd757ccff8c0bbb`
