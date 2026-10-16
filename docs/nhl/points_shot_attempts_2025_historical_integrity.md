# NHL 2025 Shot-Attempt Historical Integrity Sample

Status: **bounded sample confirms systemic legacy-fallback contamination; full 2025 source reconciliation is justified and remains outstanding**.

## Scope and method

This was a read-only sample investigation. Six games from the `2025` game-ID-prefix population were selected across the retained date range, including recorded positive values, recorded zeros, NULL periods, and the highest sampled stored count:

| Game | Game date | Why selected |
| --- | --- | --- |
| `2025020001` | 2025-10-07 | early-season positive and zero values |
| `2025020326` | 2025-11-20 | high attempt values; sampled maximum 13 |
| `2025020519` | 2025-12-16 | mixed positive and NULL records |
| `2025020632` | 2026-01-01 | mixed positive and NULL records |
| `2025020947` | 2026-03-01 | all 36 database values NULL |
| `2025021188` | 2026-04-01 | all 35 available database rows NULL |

The existing `backend/nhl/points_attempts_source.py::derive_attempts_from_final_pbp` validator independently reconstructed all 216 official skater counts from final NHL Gamecenter play-by-play. All six games passed final-state, event identity, team/player binding, unique-event, and exact player SOG reconciliation checks against official boxscores. Twelve official source responses (one boxscore and one PBP response per game) returned HTTP 200. Raw response bodies, request/capture timestamps, HTTP dates, hashes, the database snapshot, comparisons, and per-game results are retained at:

`artifacts/analysis/nhl/points_attempts_2025_integrity/sample_2026-10-16/`

`SHA256SUMS.json` binds every file in that directory. The retained September 2026 Gamecenter boxscores used to reproduce the old expression are in `artifacts/operational/nhl/moneyline_team_history/raw/season=2025/game=<game_id>/boxscore.json`; their exact paths and hashes are in the comparison CSV. No database values were changed.

## Sample comparison

The denominator is 216 official player-game records across six validated games. There were 214 database log rows across those games; three official player identities could not be bound uniquely to a canonical database identity (two absent log rows and one missing/unusable crosswalk).

| Result | Player-game rows |
| --- | ---: |
| Database value exactly matched official PBP | 32 |
| Database value differed from official PBP | 89 |
| Uniquely bound database row was NULL; official PBP supplied a count | 92 |
| Canonical database identity unresolved | 3 |
| Total official rows | 216 |

For 70 of the 89 discrepancies, the retained boxscore fields reproduce the database value exactly using the historical fallback expression `SOG + (missedShots or 0) + (blockedShots or 0)`. The retained responses have no `missedShots` or explicit attempt field, so the expression reduces to SOG plus the player's defensive `blockedShots`. In nine of the eleven sampled database zeros, official PBP records one or more attempts; only two sampled database zeros are verified official zeros. This confirms that the fallback can turn legitimate attempts into zero or undercount them. The sampled maximum of 13 (game `2025020326`) matches official PBP and the fallback by coincidence; a matching non-NULL value alone is not proof of source authenticity.

Nineteen discrepancies are not reproduced by that expression and remain unexplained. Those may reflect other source versions, a different ingest, or corrections to the official record; this sample does not assign a cause. Three unresolved identities and the 92 uniquely bound NULLs were not treated as matches or zeros.

## Git history and affected population

The fallback is present in the importer history as follows:

- Commit `7b978031` (2025-10-12) first adds the daily skater importer with `shotsAttempted` as the preferred field and a fallback to SOG + missed shots + blocked shots when that field is absent. That version reads a nested `stats` shape.
- Commit `8f15b81a` (2025-10-23) introduces the top-level Gamecenter boxscore parser matching the retained payload shape and preserves the same three-term fallback when `shotAttempts` is absent. This is the first Git version whose parser directly matches the retained boxscore records used in this sample.
- Commit `9cad5d9b` (2026-03-02) removes `blockedShots` from attempts. It leaves attempts unknown unless an explicit field or `missedShots` is present; the retained boxscore schema has neither. The upsert uses `COALESCE(incoming, existing)`, so NULL refreshes do not clear a previously stored fallback value.
- Commit `99284ff0` (2026-10-15) adds validated final-PBP acquisition and uses it when the boxscore has no explicit `shotAttempts` field.

Git establishes when the code was available, not which historical invocation wrote an individual row. The 70 formula-signature discrepancies therefore demonstrate contamination consistent with the historical expression, but do not by themselves prove the exact importer invocation. Of those 70, 47 are from games dated after the first importer commit; the other 23 are from 2025-10-07, before this importer existed, and require a later backfill or another source path to explain. No retained 2025 daily receipts were found that bind individual database writes to importer executions.

The database-wide monthly read-only count shows a source-coverage break that aligns with the code history:

| Game-date month | Rows | Non-NULL attempts | Stored zero | Stored positive | NULL |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2025-10 | 6,426 | 6,426 | 793 | 5,633 | 0 |
| 2025-11 | 7,971 | 7,971 | 1,114 | 6,857 | 0 |
| 2025-12 | 8,049 | 6,960 | 484 | 6,476 | 1,089 |
| 2026-01 | 8,642 | 6,374 | 70 | 6,304 | 2,268 |
| 2026-02 | 2,659 | 2,659 | 357 | 2,302 | 0 |
| 2026-03 | 8,694 | 0 | 0 | 0 | 8,694 |
| 2026-04 | 4,272 | 0 | 0 | 0 | 4,272 |
| **Total** | **46,713** | **30,390** | **2,818** | **27,572** | **16,323** |

This profile and the sample justify expanding to a full 2025-prefix source reconciliation and a bounded correction candidate. They do not justify updating all retained non-NULL values from code history alone. The March–April NULL rows should remain unknown until each game's official PBP is independently validated; the unexplained discrepancies and identity gaps also need source-bound dispositions.

## HGB and Phoenix exposure

The retained 2025 HGB evaluation source export, `points_official_outcome_restoration/2026-10-09/restored_v3/skater_log_feature_source.csv.gz`, includes 214 of the sampled database player-game rows. Under its frozen `120_DAY_LEGACY_BOUND` last-ten feature builder, all 214 source rows appear in at least one later 2025 evaluation history. All 70 formula-signature discrepancy rows enter at least one such history; 682 retained 2025 target feature rows include at least one of those discrepant records in their prior history. This establishes feature-path exposure, not the effect on fitted HGB predictions or evaluation metrics. No model scoring or sensitivity replay was performed.

The sampled 2025 records are outside the frozen Phoenix training window, which its retained feature metadata identifies as 2023–24. Phoenix's operational SQL feature path selects the latest ten strict-prior regular-season skater-log rows without a date cutoff and consumes `shot_attempts` in `attempts_d10_per60` and season/window count features. A read-only reconstruction against the 100 current Points target identities found zero of these six sampled games in their latest-ten histories. The actual Phoenix same-run feature input was not retained, so this is a target-history check, not evidence that Phoenix predictions were produced from these rows on that slate. Other 2025 source rows can still enter Phoenix histories on later slates.

The specific 2025 fallback code did not exist when 2023–24 game records were originally played, so it cannot be attributed to those rows through the ordinary daily path. A later bulk backfill using the same importer remains possible but is unverified. Both frozen HGB and Phoenix use attempts-derived features and both train on 2023–24 history; that training source provenance remains open and neither model is exempt from a separate historical source check.

## Next ordinary daily run

Commit `be389750` adds `NHL_SHOT_ATTEMPTS_SOURCE_SUMMARY_V1` to the normal `seed_skater_logs_for_date.py` child output. It reports explicit-boxscore, validated-PBP, unknown, positive, zero, and per-game reconciliation-failure counts. `backend/nhl/cli.py::_structured_child_summary` parses `NHL_CHILD_SUMMARY_JSON`, and `run()` records the parsed fields in the daily child summary. The next ordinary daily run will therefore capture the status in its receipt; no special run was initiated. The October 16 receipt predates `be389750` and is not evidence of this newer summary.

No production code, database values, model artifacts, predictions, authority, or Funnel settings were changed. No paid odds or other provider requests were made; only the 12 official NHL source requests above were issued. No unit tests were run because this task made no implementation changes; the existing validator executed against all six real source pairs.

## Recommendation and open work

Proceed with a complete game-by-game 2025 official-PBP reconciliation candidate before any database correction. Preserve verified zeros, keep unresolved source values NULL, and separately classify each discrepancy as fallback-matched, official-match, identity-unresolved, or unexplained. Do not infer prediction effects from source exposure. Investigate 2023–24 model training inputs as a separate source-provenance question.

The broader Points foundation investigation remains open.
