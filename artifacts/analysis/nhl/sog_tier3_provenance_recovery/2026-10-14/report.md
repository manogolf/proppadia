# NHL SOG Tier-3 provenance recovery

Audit date 2026-10-14. Local repository and retained artifacts only. No providers, paid credits, database writes, model-performance use of Tier 3, or user override.

## Decision

**SUBSTANTIAL_TIER2_RECOVERY_AVAILABLE**: 26,084 of 39,701 archived Tier-3 player-games (65.7%) have all current frozen baseline rate/exposure inputs rebuildable from retained final skater logs using an explicit strict-prior game-date filter. These are `TIER2_RETROSPECTIVE_STRICT_PRIOR_RECONSTRUCTION`, not proof that the original archived rows were created pregame. Original-pregame-proven rows: 0. Remaining Tier 3: 13,617.

The archived features bind deterministically to `(game_id, player_id, slate_date)` via unique row keys and canonical schedule date. Missing expected-skater roster data means this remains an archived-population result, not full slate eligibility coverage.

## Generation path and code history

Daily archived files are `artifacts/archive/generated_daily/nhl/YYYY-MM-DD/sog_features/sog_features_YYYY-MM-DD_denali.csv`, exported from the SOG feature pipeline. The current SQL feature builders include `seed_sog_features_for_slate.sql`, `fill_sog_toi_features_for_slate.sql`, `fill_sog_season_toi_features_for_slate.sql`, and `export_sog_denali_pregame.sql`; their Git commits and hashes are in `git_provenance_timeline.csv`. Historic Git revisions show explicit `game_date < slate_date` in the SOG log rollup, and explicit `ROWS BETWEEN N PRECEDING AND 1 PRECEDING` for rolling windows. This demonstrates the checked-in code contract at those revisions, but there is no original run/commit receipt that binds each archive to a particular script revision.

The daily orchestration in `backend/nhl/cli.py` seeds and fills SOG counts, PP role, rolling TOI, season situation TOI and pairings (lines 2628–2646), then exports Denali features (around 2697–2699). The historical seed file already has a strict-prior predicate in commit `2bd8671e` (2025-10-14); the completed Denali builder in `0e6be589` (2025-11-22) also uses a preceding-row window. Situation TOI shift logic appears in `848aaa71` (2025-12-31) and subsequent revisions retain strict date filtering when the casted `game_date::date < slate_date` predicate is parsed. The timeline records current hashes and per-revision predicates. These code facts show historical-era strict-prior intent, but no run receipt binds a particular export to those revisions.

The production scorer prioritizes D10/D20/D5 per-60 rate, and D10/D20/D5 TOI average then season situation-TOI fallbacks; lambda is rate × minutes / 60. Rate is rebuilt as `SUM(SOG)/SUM(TOI)*60` over the last N strictly earlier appearances. Rolling total-TOI exposure comes from retained logs; season EV/PP exposure is independently reconstructed from retrospective shiftchart and manpower data. Exact Tier 2 requires both selected inputs to match the archive within 1e-12.

## Evidence and limits

The retained 2025 schedule has scheduled UTC starts. The retained skater snapshot has game date, game ID, player ID, SOG, TOI, and `log_created_at`. Many October logs have a December 14–15 `log_created_at`; they are bulk history, not contemporaneous captures. Read-only database exports add 38,908 player-game situation-TOI rows reconstructed from shifts/manpower, but their source timestamps are later than the games. There are no date-matched original SOG run receipts/manifests binding feature SHA, cutoff, player-game rows, and pregame start. Archive mtimes are weak evidence only. Therefore original-pregame evidence class A is zero. Rows not at exact B input parity remain C where strict-prior history and historical code support exist, otherwise E; see reason and chronology ledgers.

Evidence class B is assigned only where the retained final history contains a strict-prior rate and exposure sufficient to rebuild lambda. Rows missing this history stay Tier 3 with specific reason codes. No Tier-3 outcomes or metrics were evaluated.

## Tier 1 method validation

The reconstruction is checked against certified late-season predictions at all three lines. Of 13,389 Tier-1 identities, 13,143 have exact archived selected-input parity; their 39,429 threshold comparisons all match within 1e-12 (maximum absolute difference 8.88e-16). The remaining identities do not pass the selected-input parity gate. This is feature/prediction method validation only, not a performance study.

## Month-level recoverability

See `month_level_recoverability.csv` for TIER 3 separate from TIER 1. Tier 1: 13,389. Tier 2 retrospective reconstruction: 26,084. Combined usable archived identities: 39,473 player-games and 118,419 line rows. Percent of 46,713 retained official appeared-player-games represented is 84.5%; this is not an expected-skater denominator.

Tier-3 monthly ledger: 2025-10: 3,343/4,342 (77.0%); 2025-11: 5,836/8,188 (71.3%); 2025-12: 5,991/9,371 (63.9%); 2026-01: 5,640/10,697 (52.7%); 2026-02: 1,403/2,857 (49.1%); 2026-03: 2,255/2,497 (90.3%); 2026-04: 1,616/1,749 (92.4%). Archived exports exist on 167 of 167 canonical game dates; Oct–Jan rows and missing/empty dates are enumerated in the inventory.

## December

December has 28 canonical game dates with archived rows tabulated in `december_recovery_map.csv`; the Tier-3 population is 9,371, with 5,991 retrospective strict-prior candidates. No evidence supports original-pregame promotion. This points to **lineage loss, with source-history reconstruction available for a substantial share**, rather than a blanket loss of December feature data.

## Structural work and backlog

The recovered Tier 2 population is sufficient to justify later structural studies as research-only retrospective reconstruction, carrying the original-vs-reconstructed distinction into evaluation. No structural study was executed in this task. It does not authorize any Tier-3 override. Backlog preserved without execution: historical official-TOI binding; prospective grading of `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`; continued fixed-blend collection; high-D10 soft-compression shadow pending broader evidence; Puck Line deferred until tomorrow; reusable cross-prop sanity/explanation audit starting with Points; possible Tier-3 override remains pending.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Pushed: NO.
