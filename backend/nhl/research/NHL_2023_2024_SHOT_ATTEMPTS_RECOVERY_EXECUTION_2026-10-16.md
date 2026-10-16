# NHL 2023–24 Shot-Attempt Recovery Execution

**Database status: COMMITTED AND INDEPENDENTLY VERIFIED**

**Run ID:** `shot_attempts_recovery_20261016T163636Z`
**Preparation commit:** `659bb7ed6fd7f7bd0b0d66de9758711609a2e1f6`

## Transaction

The existing guarded transaction in `backend/nhl/sql/repairs/repair_nhl_2023_2024_shot_attempts_from_official_pbp.sql` committed. Its receipt reported `shot_attempt_rows_changed=46379`. The SQL file SHA-256 is `e89625667428a9833d79c2632bd00aecb73bc7b2112ea4bf5e03499e8abeb09c`.

The 46,379 changes comprise 40,907 positive-to-positive corrections, 12 positive-to-verified-zero corrections, and 5,460 false zeros corrected to verified positive counts. Thus 46,367 replacements are positive and 12 are numeric zero. The 5,460 false zeros were all replaced with positive source counts.

The preflight matched the prepared population exactly: 37,040 independently verified matches were excluded; 7,829 unresolved raw rows in 218 games and 228 official identities without raw rows were excluded. No target original value, key, or mapped team identity differed from prepared evidence. Source evidence covered 2,324 reconciled games; all 4,648 referenced PBP and boxscore response bodies matched their retained hashes. All target identities passed the explicit historical-team crosswalk check.

## Independent verification

Read-only full-table snapshots contained 153,260 rows both before and after. Comparing every key and column found exactly 46,379 changed identities, all in `shot_attempts`; every replacement equals its official candidate. The 7,829 unresolved rows and all 37,040 verified matches remained unchanged. No identities were inserted or removed.

The prior recovered populations were checked against their official candidates and the before/after snapshots: all 34,108 recovered 2025 rows and all 3,204 recovered 2026 rows remained unchanged. No SOG, PP TOI, total TOI, blocks, player/team/game identity, or other raw field changed. The transaction writes only `nhl.skater_game_logs_raw.shot_attempts`; PostgreSQL reported no non-internal triggers on that table, and the transaction does not write predictions or model artifacts.

The existing guarded forward SQL was then executed a second time. It reported `shot_attempt_rows_changed=0`; the complete retry snapshot is byte-identical to the committed after snapshot. The rollback SQL was validated as the exact 46,379-row inverse candidate set, guarded by official/current values and restoring only original `shot_attempts`. It was not executed.

## Routine-ingestion durability

Daily promotion in `backend/nhl/cli.py` and postgame promotion in `backend/nhl/scripts/run_nhl_postgame_reconciliation.py` use `preserve_existing_attempts_conflict_assignment` from `backend/nhl/points_attempts_source.py`. The assignment keeps a populated raw attempt value (including zero) and fills only a raw NULL. The skater-log importer replaces its staging snapshot, but that staging behavior does not replace populated raw attempts through these promoters. The separate historical recovery remains an explicit guarded correction path.

## Evidence and tests

Run-bound snapshots, preflight, SQL logs, changed-row evidence, trigger/constraint query, retry receipt, and execution receipt are retained under:

`artifacts/operational/nhl/shot_attempts_recovery_execution/run_id=shot_attempts_recovery_20261016T163636Z/`

The compact execution receipt is also committed beside this report as `NHL_2023_2024_SHOT_ATTEMPTS_RECOVERY_EXECUTION_2026-10-16.json`.

- Before full raw snapshot SHA-256: `a02157e6cd449a8163e55bd2261a0bc8fece30119a1af89d4297be76618d9476`
- After full raw snapshot SHA-256: `88cda7128b22387ba14bdf26276a15e7c2fe9a5c2eac584e63446b0aef1f4dc8`
- Changed-row evidence SHA-256: `7876a58bb402edc1000c249aadba4d27a44c6782220b99d5f317f98d2e1cf955`
- Retry snapshot SHA-256: `88cda7128b22387ba14bdf26276a15e7c2fe9a5c2eac584e63446b0aef1f4dc8`
- Official candidate SHA-256: `6bf0944d5c0e31e4302902ef466fc20ad79c0071bcd65b0e410970a4d3e7d091`
- Official acquisition manifest declared SHA-256: `e7f9117810e6d40e0a9b791a3936f04cd827c57e6fcb54c11c0b1d3a1e694034`
- Execution receipt SHA-256: `bf6eb2a44c71e06d88089d078a02dc04a9962c99f3302b553a8d5de531299627`

Focused tests passed: `./bin/python -m pytest backend/nhl/tests/test_points_attempts_source.py backend/nhl/tests/test_prepare_nhl_2025_attempts_repair.py backend/nhl/tests/test_points_hgb_official_team_shots_capture.py` — **16 passed**. One existing joblib warning reported that physical-core detection returned an empty value; it did not affect the test results.

The rollback file SHA-256 is `97d05ce01280a344642f198029f7862206f074f1c8be97dd671d0221ebb77653`. No model fitting, scoring, prediction, authority, or unrelated database operation was performed.
