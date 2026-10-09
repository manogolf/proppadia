# NHL 2025–26 Official Points Outcome Restoration

## Population and source audit

The target season is canonical season 2025 (2025–26 regular season), October 7, 2025 through April 16, 2026: 1,312 scheduled games. The database source `nhl.skater_game_logs_raw` has primary key `(player_id, game_id)`, 46,713 rows over 1,306 games, and 46,713 unique keys. Goals present: 0; assists present: 0; both present: 0; either missing: 46,713; both missing: 46,713. `nhl.skater_points_raw`, also keyed by `(player_id, game_id)`, has 0 regular-season rows for canonical season 2025.

The local official source inventory found 1,312 retained NHL Gamecenter boxscore responses at `artifacts/operational/nhl/moneyline_team_history/raw/season=2025/game=*/boxscore.json`. They are regular-season final boxscores and contain player IDs, skater goals/assists, team IDs, game dates, and official final scores. Existing response bodies were reused; the prior Moneyline research acquisition code uses atomic write-once retention and refuses a conflicting body at an existing path. The acquisition timestamp for these earlier responses was not recorded, so it is left unknown. Their exact hashes and paths are in `source_manifest.json`.

Those boxscores yielded 47,231 official player-game rows. Eight database log keys were absent from the boxscore player lists: five Elias Pettersson rows, two Andre Lee rows, and one Jaccob Slavin row. All eight had logged playing time. To resolve them, eight official Gamecenter play-by-play requests were made, one per affected game. These responses and their request ledger are retained under `../pbp_recheck_raw/`. No name matching was used. Each player ID had no goal or assist credit in the complete official goal-event sequence for that game, so their restored goals/assists are exactly zero. Goal event counts reconcile to both teams' final scores for all eight games after excluding shootout attempts from player goals.

## Recovery and integrity results

- All 46,713 source log keys exactly match one restored outcome: 100.000% exact identity coverage.
- The restored outcome dataset has 47,239 unique `(game_id, player_id)` rows: 47,231 boxscore rows plus 8 event-level supplements.
- All restored rows have goals and assists. Realized Points equals goals plus assists. No negative targets, duplicates, unresolved keys, or ambiguous keys remain.
- The 526 additional boxscore player-game rows not present in the log table are retained as outcomes with their official identity and target. Their log-derived feature fields remain missing or zero according to the existing bakeoff export semantics.
- All 2,624 team-game goal checks pass. There are 119 shootout score adjustments; the official player-goal sum excludes the shootout-deciding attempt. No other team score mismatch remains.
- There were no pre-existing partial goals/assists in the database target population to compare. Thus the retained-target comparison count is zero, not a claim of agreement with a second target source.
- No database rows were inserted or updated. No paid credits were consumed. Provider calls: 8 planned, 8 executed.

## Validation readiness

The 2025 `120_DAY_LEGACY_BOUND` feature frame was constructed using the unchanged `build_frame` function from the frozen architecture bakeoff. It contains 47,239 unique player-games, excludes same-day outcomes from player and team history (`game_date < target date`), and joins the 90,486 unchanged 2023–24 prior training rows into `validation_input_2023_2025.csv.gz`. No 2025 data was used to tune the model. The exact input and parent-frame hashes are in `validation_input_manifest.json`.

Status: **READY_FOR_FROZEN_HGB_SECOND_SEASON_VALIDATION**. The optional validation was not run: the existing runner filters its evaluation set to `canonical_season == 2024`, so it cannot evaluate 2025 without changing runner behavior. No runner change or model search was made. The artifact prepares the exact target and frozen feature semantics for a later dedicated frozen validation.

The canonical outcomes, coverage/mismatch/unresolved reports, game-level scoring checks, response manifests, and SHA256 checksums are retained in this package. The eight newly acquired raw play-by-play bodies are separately retained and hash-bound in `pbp_source_manifest.json`.
