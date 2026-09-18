# MLB BvP canonical slate identity contract V1

Authorized September 18, 2026. Contract: **`BVP_CANONICAL_SLATE_IDENTITY_V1`**. This is a forward acquisition/certified-reader safety boundary, not permission to rerun, repair or regrade historical data. No model, feature formula, schedule, power, market, publication or wagering authority changes.

## Proven root cause

The retained legacy mapper is reproduced offline from commit `f74293b1`: today's MIN–LAA official gamePk **823977** becomes **823978** using only `game_info.game_date='2026-09-18'` and teams 142/108. The local row actually denotes September 17, starting September 18 01:38 UTC / September 17 18:38 PT. Today's game starts September 19 01:38 UTC / September 18 18:38 PT. Only that matchup matches the two local candidates, producing mapped=1/unmapped=14. `insert_mlb_stat_derived._upsert_game_info_min` parses the Z timestamp then stores `parsed.date()` as `game_date`, reproducing the UTC/local-date mismatch. Local enrichment then overwrites the official ID; it is not a Mac timezone or DNS defect. No fix to that shared historical producer is made here.

The earlier retained 05:30 official source and immutable slate identify all 15 intended September 18 games. Original manual raw schedule/pairings are not retained. The primary ID transformation occurs after source parsing, before DB starter fallback and tuple construction; the date range does not cause it. No source/cache-origin claim is substituted for missing original bytes.

## Identity and failure policy

- Primary game identity is official MLB gamePk, never a date/team-name substitution or optional local key.
- Independent source authority requires requested `America/Los_Angeles` slate date, source schedule-day date, `officialDate`, distinct positive home/away team IDs and an aware scheduled-start UTC timestamp whose PT date matches. Distinct official IDs keep doubleheaders distinct. A rescheduled game needs consistent current authority; suspended/postponed/cancelled or carried-date entries remain unresolved/excluded rather than guessed.
- Before a feature row is admitted, require its gamePk in that independent verified set, requested date, positive batter ID, tags and unique canonical tuple key. Opposing pitcher ID, team/opponent, successful request identity/hash and actual source observation/acquisition times are retained in the private journal. DB `computed_at` remains its actual write time.
- An unverifiable/malformed authoritative slate fails the whole acquisition before feature write. Known individual off-date/unverifiable-state entries and foreign prepared tuples are excluded while valid rows continue, classified **`PARTIAL_IDENTITY_REJECTIONS`**. This preserves the collector's existing valid-population/exclusion behavior without admitting a wrong identity. Duplicated or malformed canonical keys fail closed. A rejected game excluded before acquisition has zero prepared rejected rows; its hypothetical row count is not invented.
- No optional local match is necessary. `local_game_id_mapped` is retained for telemetry compatibility but is always zero; `local_game_id_already_aligned` counts exact official-ID/team coverage, and `local_game_id_unmapped` means optional coverage absent. No official ID is changed. Starter fallback uses exact preserved official IDs, not another same-team game.

Reason distinctions: **`CANONICAL_SLATE_IDENTITY_VALID`**, **`OPTIONAL_LOCAL_ID_UNMAPPED`**, **`OPPOSING_STARTER_UNRESOLVED`**, **`OFF_DATE_GAME_REJECTED`**, **`CANONICAL_IDENTITY_UNRESOLVED`**. Request/data errors are separate from these identity classifications and remain visible in original summary counters. Final summary includes intended/verified/unmapped/starter-unresolved games, rejected/unresolved games and rows, prepared/written rows and identity certification status. Initial schedule retry behavior and downstream no-qualified-model semantics are unchanged.

## Auditable request and exclusion provenance

Every nonempty acquisition uses an exclusively created, mode-0600 per-run journal beneath a mode-0700 slate directory in `artifacts/ops/bvp_identity_v1/<date>/`. Records are append-only and fsynced before feature admission. No reusable latest receipt overwrites prior evidence. They retain actual times, official game/date/start, teams, batter/pitcher IDs when applicable, feature/model tags, source/fallback/cache branch, reason and response/feature hashes. Missing starters retain exact affected team side and NULL pitcher; failed fallback and request states are explicit. Metadata-only logging does not add feature keys or change values.

Successful empty stats/splits retain the successful request identity **and parsed response payload/hash** in this private journal. `EMPTY_BVP_RESPONSE` is not a claim of certified zero history and creates no zero-valued row. Unknown empty shape is `EMPTY_BVP_RESPONSE_UNVERIFIABLE`; failed requests are not successful empty responses. Cached reuse retains the original request evidence. No credential, private network identifier or paid market call is added.

After unchanged database commit, the journal gains a durable `DATABASE_WRITE_COMMITTED` marker. New-source certified readers require that marker plus matching batter/pitcher/tag/BvP payload hash, independent game/date/teams, actual timestamps and pre-start source/write timing. A missing or contradictory journal fails closed. Journal size is only cache invalidation, not an outcome/source timestamp. Forward source boundary is September 18 17:41:34 UTC; no BvP acquisition was invoked by this implementation.

Legacy rows retain their original bytes/hashes. They may be **game-identity-eligible**, but absent original pitcher/as-of provenance is never upgraded to full source certification. Canonical membership is verified through retained official schedule archives, designated immutable Moneyline identities, or a committed new source journal; `game_info.game_date` and raw feature `game_date` alone are insufficient. Conflicting/unknown authority fails closed. Optional internal ID absence does not remove otherwise proven official rows.

## September 18 non-destructive quarantine

All **1,937 original rows / 149 batters / 14 stored game IDs** remain exactly stored, including computed_at `2026-09-18T17:13:42.094683Z`. The frozen exclusion receipt lists **91 canonical keys and complete-row SHA-256 hashes** for prior-night game 823978. Its bytes are pinned by the reader and integrity manifest. Add later reconciliation receipts as new versions; do not modify this one. The boundary excludes those exact retained rows from certified evidence, including a prior-day exact-ID training join, without deleting/updating/relabeling any row.

The read-only validator proves the original entire row-stream hash unchanged, exact exclusion membership and **1,846 remaining game-identity-eligible rows / 142 batters / 13 games**. It does not certify missing legacy pitcher lineage or reconstruct 03:30. Today's MIN–LAA correctly keyed population is absent because of the wrong key; DET–CHW also has no rows, but that does not identify either starter exclusion.

The two manual `skip_no_opp_sp` cases remain **unrecoverable at the actual manual timestamp**. Earlier 05:30 source lacks BAL/COL home starters, but it is not the manual source and is not substituted for it. The 243 prior empty responses likewise lack individual retained request proof. Future occurrences now retain exact identities and payload evidence.

## Historical scope and downstream boundaries

Read-only retained audit: **270,582 rows**, April 7–September 18, across 121 slate/acquisition-date cohorts. **262,561** match retained official identity; **8,021 rows / 43 game groups / 18 slate dates / 17 acquisition dates** are off-date; zero unknown identities and duplicate tuple keys. This is retained mutable row state, not 121 immutable acquisition runs. **5,954 rows / 29 games** have the preceding-local-date/UTC-date fingerprint; other 2,067 involve different date changes, including postponement/rescheduling. Do not equate all off-date retained rows with proven erroneous original acquisitions. The scheduled April 15 log reports four remaps and 741 retained prior-night rows, proving the pattern is not manual-only. Earliest retained off-date slate is April 7 (acquisition April 8); latest is September 18.

Protected BvP-bearing readers: `prop_workflow._load_latest_pfp_features` (including wide/parent/shadow callers), `model_trainer._fetch_pfp_feature_rows`, and `v2_write_training_from_pfp._pfp_rows_for_date`. They now enforce the shared identity/exclusion boundary while retaining valid fallback order and unrelated non-BvP rolling payloads. Diagnostic SQL readers use exact label/game joins or lineup-only fields; they remain explicitly raw/research, not certified admission paths. Their inventory is in the manifest. Existing snapshots are not rewritten or rescored.

Historical **actual consumption remains unresolved**: legacy loading discarded source-row identity; 53 matching September 17 training keys existed before the manual write, but the table has no copied feature/provenance column proving consumption of those later 91 rows. No claim that all historical models/datasets were unaffected is justified. Current authority is `NO_QUALIFIED_MLB_MODEL`, automated retired-model retraining suspended; no qualified production model consumes BvP. Moneyline/Totals/market/study models and all ledgers receive no task writes.

## Validation, manual runbook and rollback

No BvP rerun or live HTTP/API call is authorized by this repair. The existing explicit-user-authorization runbook in [acquisition/late-admission contract](MLB%20BvP%20Acquisition%20Retry%20and%20Late-Admission%20Contract%20V1.md) remains mandatory: current PT date, processes/window, both locks, distinct actual-time identity and acquisition error handling; no full pipeline, automatic recovery or predictive application. The forward safety implementation removes the defective substitution, not the need for these checks.

Read-only retained validator (no acquisition):

```sh
cd /Users/jerrystrain/Projects/proppadia
.venv/bin/python -m backend.mlb.scripts.validate_mlb_bvp_identity_v1
```

Offline tests cover local/UTC boundaries, West Coast midnight, doubleheaders, postponement/reschedule/suspension, optional local absence, cross-date remap, unresolved authority, foreign/mixed candidates, duplicates, missing starters, empty and failed requests, receipt failure, new committed source receipt and the September 18 actual identity-key fixture. Wake-retry tests and retry/feature/upsert function source remain unchanged. Tests and final hashes: `artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_identity_forward_correction_manifest.json`.

Rollback requires separate approval: revert only this forward-correction commit, preserving all data/logs and the immutable exclusion receipt as historical evidence. Reverting a reader guard re-exposes known raw identity risk and requires review. No database rekey/delete, reacquisition, schedule/power/service reset or publication rollback is part of this task. Push none.
