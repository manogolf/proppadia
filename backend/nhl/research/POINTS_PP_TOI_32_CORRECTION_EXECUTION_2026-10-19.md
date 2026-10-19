# NHL PP-TOI 32-row correction — execution receipt, 2026-10-19

## Execution

- Authorized and run by the operator on 2026-10-19 in the Supabase SQL editor
  (production project), session user `postgres`.
- Block: `backend/nhl/sql/repairs/operator_sql_editor_pp_toi_32_correction.sql`,
  SHA-256 `a7ff4909cf059d0956b09d84aa79a96445801fc02673d8d0ea471121ac025608`. It granted `proppadia_nhl_source_correction`, verified
  `current_user`, ran the reviewed guarded UPDATE
  (`repair_nhl_pp_toi_32_duplicate_shift_intervals.sql`), reset and revoked
  the role, and verified the revoke in one transaction.
- Proposal: `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/pp_toi_recovery_compatibility_differences_20261016.csv`,
  SHA-256 `4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631`;
  32 player-games in 29 games, reason `DUPLICATE_SHIFT_INTERVAL`.

## Receipts (operator, `postgres`)

| Receipt | checked_at_utc | original | corrected | missing | other |
| --- | --- | --- | --- | --- | --- |
| Before | 2026-10-19 16:09:03.9978+00 | 32 | 0 | 0 | 0 |
| After | 2026-10-19 16:10:02.469233+00 | 0 | 32 | 0 | 0 |

## Independent verification

A separate read-only check as `proppadia_nhl_research_login` after execution
found original 0, corrected 32, missing 0, other 0, and zero members of
`proppadia_nhl_source_correction`, so the temporary grant was removed.

## Committed values (`nhl.skater_game_logs_raw.pp_toi_minutes`)

| game_id | player_id | pp_toi_minutes |
| --- | --- | --- |
| 2023020012 | 8482684 | 4.38 |
| 2023020204 | 8480839 | 5.38 |
| 2023020268 | 8483460 | 0.50 |
| 2023020329 | 8475714 | 4.20 |
| 2023020352 | 8477979 | 0.88 |
| 2023020428 | 8473449 | 1.53 |
| 2023020650 | 8480003 | 0.67 |
| 2023020692 | 8478028 | 0.23 |
| 2023020693 | 8476875 | 4.42 |
| 2023020748 | 8478115 | 2.43 |
| 2023020835 | 8480113 | 0.28 |
| 2023020890 | 8477952 | 2.70 |
| 2023020927 | 8475958 | 0.85 |
| 2023020961 | 8478466 | 3.95 |
| 2023020961 | 8479314 | 6.33 |
| 2023020979 | 8483466 | 3.18 |
| 2023020998 | 8474590 | 3.28 |
| 2023021058 | 8479400 | 1.58 |
| 2023021058 | 8481606 | 1.00 |
| 2023021063 | 8477839 | 0.63 |
| 2023021081 | 8479977 | 3.93 |
| 2023021116 | 8480748 | 0.12 |
| 2023021188 | 8482133 | 2.52 |
| 2023021277 | 8477503 | 1.23 |
| 2024020123 | 8477903 | 0.05 |
| 2024020141 | 8474600 | 4.87 |
| 2024020331 | 8480043 | 5.65 |
| 2024020557 | 8480806 | 0.82 |
| 2024020557 | 8482745 | 3.60 |
| 2024020644 | 8476461 | 0.80 |
| 2024020843 | 8475172 | 5.75 |
| 2024021138 | 8476468 | 1.22 |

Rollback, if ever required:
`backend/nhl/sql/repairs/rollback_nhl_pp_toi_32_duplicate_shift_intervals.sql`
(guarded the same way against the corrected values).
