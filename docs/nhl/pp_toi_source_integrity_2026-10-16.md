# NHL PP-TOI source-integrity findings — 2026-10-16

Status: **confirmed post-checkpoint changes and code paths that erase or infer zero; correctness of the 98,292 positive replacements is not established from retained row-level source evidence.** Read-only investigation. No database, model, prediction, or authority changes were made. The Points foundation remains open.

## Scope and evidence

The comparison uses the Oct. 10, 2025 database dump `backups/nhl_schema_and_data.sql` (SHA256 `87cb47909de97ec3717e087acff344cf9514d5a01c6c42e812c5294a78010ee5`) and the read-only current table export `artifacts/analysis/nhl/raw_foundation_integrity_2026-10-16/current_raw_snapshot.csv` (SHA256 `fadf53ffaaefe518affe06b58f6ba7dd709dd070a3d025d1f3dc0fa2115fcc68`). The checkpoint has 100,386 rows. The current export has 99,987 rows; all 99,987 keys are shared. Of those shared keys, **98,292 PP-TOI values changed, and every one changed from numeric zero to a positive value**. No shared row changed from positive to zero, positive to NULL, or NULL to positive in this field comparison. This confirms later modification of the checkpoint values; it does not independently establish which side is correct.

Representative changed rows include `(game_id=2024020146, player_id=8470613, 0.00 -> 0.31, 2024-10-28)`, `(2023021169, 8479398, 0.00 -> 0.55, 2024-03-30)`, and `(2024020146, 8480835, 0.00 -> 0.31, 2024-10-28)`. These are examples of the database delta, **not** independently source-reconciled examples. The raw table has no `updated_at`, source endpoint, source-response hash, validation state, or write audit. `created_at` therefore cannot identify the process that changed these values.

The read-only live database inspection found historical `shiftcharts_raw` and `game_manpower_segments` coverage begins in the 2025 game-ID prefix; it does not provide retained individual shift rows for the 2023/24 or 2024/25 sample above. The six retained October 2025 official PBP/boxscore samples contain no PP shift evidence. Current rows, a database checkpoint, team-level PP windows, and non-NULL values cannot independently prove a particular player's PP seconds. Consequently, the requested representative source reconciliation of the 98,292 changed historical rows is **not possible from retained official-source artifacts**. Their source correctness remains unresolved; no new official requests or database writes were made.

## Confirmed transformations and write paths

| Path | Observed behavior | Integrity consequence |
| --- | --- | --- |
| Daily source parser, [`seed_skater_logs_for_date.py`](../../backend/nhl/scripts/seed_skater_logs_for_date.py#L145) | Selects candidate keys with Python `or`. A numeric `0` (and empty string) is false and may fall through to another key, ultimately parsing as `None`; text `"0:00"` is truthy and parses as zero. | A provider's numeric verified zero can become unknown before staging. The accepted boxscore shapes are not accompanied by a PP-specific completeness proof. |
| Daily staging UPSERT, same file [around line 537](../../backend/nhl/scripts/seed_skater_logs_for_date.py#L536) | `COALESCE(NULLIF(EXCLUDED.pp_toi_minutes, 0), stage.pp_toi_minutes)`. | Incoming zero is deliberately treated like missing and cannot replace a staged value. A true zero and an unavailable value are conflated. |
| Daily stage-to-raw promoter, [`cli.py`](../../backend/nhl/cli.py#L2564) | New rows use `NULLIF(pp_toi_minutes, 0)`; conflict updates use `COALESCE(NULLIF(EXCLUDED.pp_toi_minutes,0), raw.pp_toi_minutes)`. | A zero is written as NULL for a new raw row; on conflict it preserves the previous raw value. A positive incoming value can replace a populated raw value. |
| Postgame stage refresh, [`staging_sync.py`](../../backend/nhl/postgame_reconcile/staging_sync.py#L318) and lines 567–568 | Gamecenter rows set PP TOI to `None`; stage UPSERT coalesces with the prior staged value. | This source does not establish PP zero. A prior staged number can remain even when the refreshed record has no PP field. |
| Postgame raw promoter, [`run_nhl_postgame_reconciliation.py`](../../backend/nhl/scripts/run_nhl_postgame_reconciliation.py#L1190) | Inserts `nullif(s.pp_toi_minutes,0)` and on conflict coalesces the incoming result with current raw. | Explicit zero becomes NULL on insert; on conflict missing/zero preserves an older raw value. Positive staged values can replace raw. |
| Daily PP fill, [`fill_pp_toi_minutes_for_date.py`](../../backend/nhl/scripts/fill_pp_toi_minutes_for_date.py#L142) | Computes player PP seconds from `game_manpower_segments` × `shiftcharts_raw`; query returns only players with `pp_seconds > 0`. UPDATE writes positive map entries only where raw is NULL or zero. | It can fill a NULL/zero with a positive result, but does not establish zero for players absent from the overlap. The team aggregate invariant is not a player-level completeness check. |
| Archived PP reconstruction/backfill scripts | Archived scripts include date-range PP calculations from PBP/shift charts and boxscore; some target raw NULL/zero rows, while `rebuild_pp_toi_from_shiftcharts_and_pbp.py` documents a replacement and updates raw rows from its calculated map. | These are capable of revisiting historical dates. Git proves capability, not execution against the 98,292 keys. No matching run receipt or SQL statement log was found. |
| Archived `20260222_runtime_cleanup/ingest_boxscore.py` | When a PP-window map exists but a player has no map entry, it assigns `0.0`, then UPSERTs PP TOI directly. | Potential false-zero path if shift-chart/player coverage is incomplete. This file is archived; no execution evidence ties it to current values. |

The earliest current active daily PP calculation was added in `fbf133e1` (2026-01-13); historical backfill code existed earlier in the archived script history. The active calculator's source tables do not cover 2023/24, so it cannot alone explain the old-season deltas. The archived date-range backfills are plausible writers for earlier seasons, but available Git and database evidence does not identify which process produced the 98,292 changes. The Oct. 10 checkpoint predates the large delta and is the earliest retained whole-table state, not independent proof that its zeroes are all correct.

## Source semantics and model exposure

PP TOI is a duration. A verified zero means the player had no overlap with complete, correctly bound PP windows while fully covered by official shift data. Missing player shifts, absent PP windows, incomplete game coverage, or an absent provider field mean unknown; they do not establish zero. The existing overlap query omits zero-overlap players, so its output alone cannot distinguish a genuine zero from incomplete source coverage.

HGB V1's 10-feature scoring contract includes `mean_pp_toi_last10` (see [`score_nhl_points_hgb_shadow.py`](../../backend/nhl/scripts/score_nhl_points_hgb_shadow.py#L25)). Its feature exporter converts the source field to numeric, then averages available prior rows; pandas skips NULL and includes zero. The HGB scorer later fills an unavailable model input with zero. Therefore source changes can alter HGB features, and NULL-only windows can converge to the same model input as a measured zero. The 98,292 source deltas are **potential HGB feature exposure**; no before/after HGB feature replay was run, so changed feature and prediction counts are not established.

Frozen Phoenix points metadata lists 15 features and contains no PP-TOI feature ([`feature_metadata.json`](../../backend/nhl/models/points/points_phoenix/0_5/feature_metadata.json#L4)); the Phoenix `export_points.sql` path likewise does not select or aggregate `pp_toi_minutes`. Thus this issue has **no demonstrated direct Phoenix feature dependency** in the retained Phoenix contract. This conclusion is specific to PP TOI; it does not resolve Phoenix's separate shot-attempt exposure.

## Conclusion and correction proposal

- **Confirmed defect:** ingestion/promotion code treats explicit zero as missing in multiple PP-TOI paths. A legacy archived ingestion path can also infer zero from absence in a player shift-overlap map.
- **Confirmed historical change:** 98,292 checkpoint zeros are now positive, across shared historical keys. The altered values may be legitimate later corrections, but their per-player source provenance is absent. It is not justified to label all as defects or as verified corrections.
- **Not established:** the specific writer/run, whether any of the 98,292 values is right or wrong, how many were genuine zero participation, exact HGB feature changes, or any model prediction effect. No historical PP-TOI rewrite is justified from the current evidence.
- **Smallest justified code correction:** preserve numeric zero by replacing truthiness-based PP key selection with first-present/non-`None` selection; remove PP-specific `NULLIF(...,0)` and zero-suppressing `COALESCE` behavior from staging/promoters so the stored value preserves source zero versus NULL. Before allowing any computed zero, require explicit per-game completeness for both manpower windows and shift-chart identities; emit zero only for covered players with verified zero overlap. Keep positive-only fill behavior from converting absence into zero, and add PP-specific source/promotion/conflict counts to the receipt. This proposal was not implemented because this task is investigation-only.
- **Historical correction prerequisite:** acquire/retain official final-game PBP and shift charts for a bounded representative set (then expand only if warranted), bind source records to canonical player/game IDs, validate complete game/player coverage, and retain raw hashes and before/after candidate evidence. Do not bulk-fill the 98,292 rows or the retained October 2025 HGB's 1,004 NULL PP-TOI observations without that evidence.

At the initial investigation stage no DB writes, daily runs, model changes, or source requests were made. The follow-up below records nine authorized official NHL GET requests; no PP-TOI row was changed.

## Follow-up: official shift-level reconstruction and zero-handling repair

This follow-up supersedes the earlier statement above that representative reconstruction was unavailable. Three bounded games from affected historical seasons were independently reconstructed using retained final NHL PBP, boxscore, and official shiftcharts responses. No database values were changed.

### Distribution and plausibility

Read-only counts from nhl.skater_game_logs_raw:

| Game-ID season prefix | Player-games | NULL | Zero | Positive | Positive PP/TOI median (mean) | Positive skaters per team-game, median (mean) | Team-game PP sum median (p90, max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2023 | 50,252 | 0 | 714 | 49,538 | 0.310 (0.342) min | 100% (98.6%) | 5.51 (9.72, 72.75) min |
| 2024 | 47,260 | 0 | 981 | 46,279 | 0.290 (0.312) min | 100% (97.9%) | 5.04 (9.00, 64.86) min |
| 2025 | 46,713 | 9,953 | 4,274 | 32,486 | 0.780 (1.468) min | 77.8% (69.6%) | 19.90 (39.20, 94.57) min |
| 2026 | 6,560 | 3,400 | 0 | 3,160 | 1.700 (2.136) min | 61.1% (48.0%) | 27.41 (45.03, 73.33) min |

No current record had PP TOI exceeding total TOI. Among rows with positive PP and total TOI, the PP/TOI ratio median was 1.9% in 2023 and 1.8% in 2024, versus 5.1% in 2025 and 10.5% in 2026. The 2023 and 2024 distributions are suspicious: nearly every skater is positive, yet player values and team-game totals are much lower than later seasons. Distribution alone does not prove which values are wrong; it is consistent with a broad systematic transformation or flawed historical calculation.

The 98,292 changed rows are the exact Oct. 10, 2025 checkpoint zeros later found positive in the current table. The checkpoint establishes the before-values, not that they were correct.

### Independent official reconstruction

For game IDs 2023020024, 2023021169, and 2024020211, official PBP and boxscore both reported final state OFF. Each boxscore contained 36 skaters. Shiftcharts had 659, 772, and 794 rows; all 108 boxscore skaters had shifts. Each skater's total shift duration agreed with boxscore TOI within 60 seconds.

Two existing repository algorithms were used: rebuild_pp_toi_from_shiftcharts_and_pbp.py and 20260222_runtime_cleanup/approx_pp_toi_from_shiftcharts.py. They produced the same two-decimal PP-TOI result for all 36 skaters in each game. PBP manpower windows were joined to official player shift intervals; absent overlap was interpreted as zero only when the player's shift coverage was present.

| Game | Checkpoint values | Current sum | Reconstructed sum | Reconstructed positive / zero | Current positive where reconstruction is zero | Current within 0.05 min | Mean absolute delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2023020024 | 36 zero | 96.85 | 86.03 | 30 / 6 | 6 | 9/36 | 0.566 min |
| 2023021169 | 36 zero | 18.00 | 88.87 | 26 / 10 | 10 | 0/36 | 2.316 min |
| 2024020211 | 36 zero | 106.94 | 93.73 | 32 / 4 | 4 | 14/36 | 0.648 min |
| Total | 108 zero | 221.79 | 268.63 | 88 / 20 | 20 | 23/108 | 1.177 min |

Game 2023021169 is especially diagnostic: the current values are exactly 0.55 minutes for each skater on one team and 0.45 for each on the other, totaling 18 player-minutes. Independent reconstruction gives 26 positive values and 10 verified zeros, totaling 88.87 player-minutes. This confirms corruption among the sampled records, including false positive values on all ten reconstructed-zero skaters. Across the three games, 85 current rows differ from the reconstruction by more than 0.05 minute. Matching rows do not establish provenance for unsampled records.

Raw team_id disagrees with official boxscore team identity for 54/108 sampled rows (36 in 2023020024, 18 in 2023021169). Reconstruction used official boxscore identity and shiftchart team assignment. This separate identity inconsistency is a caveat, not repaired here.

Raw PBP, boxscore, and shiftchart responses, acquisition times, endpoints, byte counts, hashes, the 108-row comparison, and per-game summaries are retained under artifacts/analysis/nhl/pp_toi_source_integrity_2026-10-16/official_sources/. See acquisition_manifest.json. The inputs and derived comparisons are reproducible.

### Historical cause and HGB feature exposure

The bounded sample establishes systematic historical source-value corruption, but not the process that wrote all 98,292 rows. Archived approx_pp_toi_from_shiftcharts.py has broad historical replacement capability and is a plausible mechanism. Git history proves the code existed, but no retained run receipt or database statement log ties it to these records. No single writer is attributed as the confirmed cause. The checkpoint and current snapshot are the earliest retained before/after states, not proof of the original values' correctness.

HGB's frozen feature exporter calculates mean_pp_toi_last10 from up to ten strict-prior player games in the 120-day history window using the mean of observed PP-TOI values. The retained bounded counterfactual file contains 965 target player-games with one or more sampled source records in their histories. After substituting independently reconstructed values for those 108 sample rows, the feature changed for 805 targets; mean absolute change across the 965 rows was 0.126 minutes, maximum 1.360 minutes. This demonstrates source-to-feature exposure, not a prediction or performance effect. No model or prediction changed. Phoenix's frozen feature contract has no direct PP-TOI feature.

### Confirmed zero-loss fixes

- seed_skater_logs_for_date.py now selects the first recognized PP-TOI key that is not None, instead of Python or truthiness. Numeric zero no longer falls through and becomes None.
- The staging UPSERT represents the latest source snapshot: explicit zero stays zero; a missing field clears stale staged state to NULL.
- The daily and postgame stage-to-raw promoters no longer apply NULLIF(pp_toi_minutes, 0). A non-NULL incoming source value, including zero, is applied; incoming NULL preserves an existing raw value. New unknown rows remain NULL.

These changes do not claim undocumented Gamecenter fields are complete or that absent values imply zero. The manpower/shiftcharts fill remains positive-only because it lacks player-completeness proof to emit zero. Archived reconstruction scripts were not changed. No historical data rewrite was executed.

Focused integration tests exercise the actual parser, stage SQL, and shared raw-promoter assignment with explicit numeric zero, 0:00, positive, missing/unknown, latest-source clearing of stale stage state, zero promotion, preserving raw values on NULL, and repeated updates. The command ./bin/python -m unittest backend.nhl.tests.test_points_pp_toi_zero_semantics backend.nhl.tests.test_points_attempts_source passed 15 tests.

Classification: historical corruption is confirmed in this 108-row sample and the 2023/24 distribution is strongly suspicious; exact cause and full-population affected count remain unresolved. Further recovery should require game-specific official shift evidence and explicit game/player coverage, not the checkpoint or current non-NULL values.
