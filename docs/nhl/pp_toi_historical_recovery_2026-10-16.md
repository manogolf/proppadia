# NHL Historical PP-TOI Recovery — 2026-10-16

## Result and scope

This is a read-only official-source reconstruction for the `202302` and `202402` game-ID prefixes. It prepared correction candidates and guarded forward/rollback SQL; it did not change the database, model artifacts, or predictions.

The inventory contains 2,542 games, 91,248 player-game rows in the current raw snapshot, and 91,493 rows in the October 10, 2025 checkpoint. The candidate includes those checkpoint identities plus one additional official-source identity with no current or checkpoint raw row: 91,494 identities total.

| Result | Count |
|---|---:|
| Games with complete final-source, shift, TOI, and dual-method reconciliation | 2,040 |
| Games unresolved | 502 |
| Reconciled raw player-game rows | 73,232 |
| Verified incorrect populated PP-TOI values | 71,627 |
| Verified correct populated values | 1,605 |
| Unresolved raw rows in rejected games | 18,016 |
| Officially reconstructed player identities with no current raw row | 195 |
| Checkpoint identities absent from both current raw and reconciled official roster | 51 |

The 502 rejected games consist of 449 `SHIFT_TOI_RECONCILIATION_FAILED` games and 53 `SHIFTCHARTS_EMPTY` games. The former fail because at least one player's shift duration differs from official boxscore TOI by more than the one-minute acceptance tolerance. All their player-game observations remain unresolved. No missing shift source was interpreted as zero.

Among the 73,232 reconciled raw rows, 51,030 official values are positive and 22,202 are verified zero. Of the 71,627 required corrections, 50,724 replace one positive value with a different positive value, and 20,903 replace a positive value with a verified zero. No accepted current zero needs changing to a positive value, and no accepted current PP-TOI value is NULL. The 1,605 verified-correct rows include 306 positives and 1,299 zeros.

By prefix:

| Game ID prefix | Reconciled raw rows | Incorrect | Correct | Unresolved raw rows | Verified zeros | Verified positives |
|---|---:|---:|---:|---:|---:|---:|
| `202302` | 35,424 | 34,823 | 601 | 11,759 | 10,219 | 25,205 |
| `202402` | 37,808 | 36,804 | 1,004 | 6,452 | 11,983 | 25,825 |

There were 245 checkpoint identities absent from the current raw snapshot. Of these, 194 have an official source identity in a reconciled game but no current raw row; one additional official identity has no checkpoint or current row; the remaining 51 checkpoint-only identities have no reconciled source identity. They are retained as unresolved and excluded from SQL.

Across the 91,248 current raw identities, 89,625 PP-TOI values differ from the October 10 checkpoint. That is a change count only; neither the checkpoint nor current values are assumed correct without official shift evidence.

A fresh read-only transaction exported the live `nhl.skater_game_logs_raw` rows for these prefixes at `2026-10-16T15:09:41Z`. All 91,248 live keys are unique and match the retained current snapshot's keys and PP-TOI values exactly. Every candidate before-value for a live key also matches; there are zero changed or missing live candidate values. The remaining 246 candidate identities have no current raw row and remain excluded from correction. The retained live export is `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/live_raw_pp_toi_snapshot_readonly.csv` (SHA-256 `452cff37a3741b780c575d337d938da6f4144455fa3fdada97c97ec084a05fe2`), with comparison receipt `live_raw_pp_toi_snapshot_comparison.json` (SHA-256 `c6a18ee0fef8ee1ef81bb5c1e2d0367b5fed41105d8a48febd949c61ea9695fb`).

## Team identity

The sampled numeric disagreements were an identifier namespace difference, not a team-side disagreement. The historical importer explicitly defines a stable internal team ID map keyed by abbreviation in [`fetch_nhl_to_csv.py`](../../backend/_legacy/nhl/scripts/archive/fetch_nhl_to_csv.py:31). Official boxscores use NHL team IDs. For example, the historical internal IDs for CHI, MTL, COL, OTT, and TBL are 8, 16, 9, 21, and 27, while the official NHL IDs differ.

The checkpoint `nhl.teams` table maps official NHL IDs, so using it to decode historical raw IDs incorrectly made 54 of the 108 sampled rows look like wrong teams. Using the original stable mapping, all 73,232 reconciled rows present in the raw table map to the same team abbreviation as the boxscore, and all 73,232 raw `is_home` flags agree with the official side. Numeric raw and official IDs differ for 68,648 rows with both identities available; this is not evidence to rewrite team IDs. No team identity change is included in the prepared correction.

## Corrupting calculation evidence

For game `2023021169`, official PBP PP wall-clock time is 591 seconds for the home team and 484 seconds for the away team. Each team has 18 skaters. Dividing each team's PP wall-clock seconds by all 18 skaters and rounding to hundredths gives 0.55 and 0.45 minutes respectively. Those are exactly the stored values assigned uniformly to every skater on each side, while individual shift-overlap reconstructions differ.

The same signature appears across the accepted population: 4,046 of 4,080 team-game groups have a uniform current PP-TOI value, and 2,509 groups exactly match team PP wall-clock minutes divided by the number of team skaters and rounded to hundredths. This is a demonstrated data signature consistent with an equal-team allocation calculation. The retained archived per-player shift-overlap writers do not implement that allocation, and no retained writer reproduces it directly. The data establish the calculation signature, but not the exact process or operator that wrote every affected row.

## HGB feature exposure

The frozen feature contract computes `mean_pp_toi_last10` over strict-prior player games within 120 days, taking the latest 10 rows. The exact retained training source frame is `validation_input_2023_2025.csv.gz`, SHA-256 `45605af7f472481af4d5a33a103b679ab4284db5d043f82842757297a3534892`; its 2023–24 target population has 90,486 rows.

Using its frozen outcome identities and current raw records, the replay covered 90,281 training targets (205 target identities are absent from the current raw snapshot). The current raw replay matches the retained feature value on 88,122 of 88,448 non-NULL comparable rows. Substituting only independently verified official values changes `mean_pp_toi_last10` for 88,096 replayed targets. Across replayed rows, mean PP-TOI feature value changes from 0.3344 to 1.2183 minutes; among changed targets the mean absolute change is 0.9908 minutes, with a 7.11-minute maximum. This demonstrates direct exposure in the frozen 2023–24 HGB training feature population. It does not establish the effect on model predictions, and no frozen prediction was changed or recomputed.

For the retained `evaluation_season=2025` HGB rolling target artifact (47,239 target rows), 44,158 identities could be replayed. None has a verified `202302`/`202402` source row in its strict 120-day feature window, so this recovery changes zero of those evaluation feature values. This does not undo the demonstrated training-feature exposure. Phoenix has no identified direct PP-TOI feature dependency in the inspected feature contract; no Phoenix artifact was modified.

## Candidate and correction controls

The candidate at `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/pp_toi_recovery_candidates.csv` includes before values, official reconstructed values, raw and official team identities, source-response hashes, shift/boxscore reconciliation evidence, and a status for every considered identity. Its SHA-256 is `0bd6953c4152e3b29b0bc1af324816f5916d86066d494559a58f492eed4dcfef`.

The prepared files are:

- `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/repair_nhl_2023_2024_pp_toi_from_official_shiftcharts.sql`
- `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/rollback_nhl_2023_2024_pp_toi_from_official_shiftcharts.sql`

They contain exactly 71,627 validated rows. The forward transaction asserts the count, confines game IDs to `202302`/`202402`, checks every current value against the candidate before-value or already-correct official value, updates only the PP-TOI column, validates post-update values, and fails transactionally on a mismatch. A rerun would update zero rows. Rollback is similarly guarded and restores only the recorded before-values. Neither file was executed. Rejected games, missing raw identities, and checkpoint-only rows are excluded.

Prepared SQL hashes: forward `6b7eea80d7347869ee9b9f46d2948c653c60c61b7c9f0a53a3e800bda976177c`; rollback `be5781ebd992b29343becd547bcbf79e90bce94c59b67f2300fda44ba0c90064`. The HGB training feature replay CSV SHA-256 is `8031bd94ca86804a8584cc47ca07439ff5d247282e0dfb4c0c46911c6559eaa7`; the 2025 evaluation replay CSV SHA-256 is `e1f10d66cbe066077a4b55220e965195fe79cb8e4d46137d6340c550797923b1`.

Official PBP, boxscore, and shiftcharts responses, per-game receipts, reconstruction outputs, and acquisition manifest are retained locally under `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/`. The manifest covers all 2,542 games and records source timestamps and raw/compressed response hashes for 7,626 source responses. Its self-excluding SHA-256 is `e7f9117810e6d40e0a9b791a3936f04cd827c57e6fcb54c11c0b1d3a1e694034`; the manifest file SHA-256 is `625f59f6d29fd7a54a2b4a5df7a630c2968f6e8bab36ad2f0b6bf88c6b7e22f2`. Current-vs-checkpoint changes are not treated as proof that either value is correct.

## Verification

Focused tests use retained official records for three games and reconcile 108 skaters with both existing shift-overlap implementations, including 20 verified zeros. They also verify the team-ID namespace mapping, the `2023021169` equal-allocation signature, rejection of empty shift evidence, and strict-prior HGB feature substitution. The full acquisition replay reused captured responses and made no database connection. A separate read-only export compared live before-values. No historical DB update, model fit, scoring run, or prediction rewrite occurred.

The 502 unreconciled games and 51 checkpoint-only identities remain unresolved. The prepared correction requires separate authorization and must not be executed as part of this investigation.
