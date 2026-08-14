# MLB Hits Aug 4–13 prediction-stream retention audit v1

- Hits scorer execution: **YES on every date and both lines**.
- Exact retained rows: 13,179 run-level observations (2,931 date-scoped earliest unique identities). Hits 0.5: 11,787/2,579; Hits 1.5: 1,392/352.
- All discovered rows are strict pregame and lineage-certified under `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb` / `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`.
- Persistence surface: `backend/mlb/exports/prospective_lineage/<date>/prediction_lineage_ledger.csv` (append-only, with exact feature, market-source, and model hashes).
- First August 3/4 divergence: downstream `production_slate_generation`; scoring and lineage persistence completed, then candidate/routing/upload/public artifacts were blocked.
- `NO_QUALIFIED_MLB_PROP_MODEL` effect: prevented candidate/ranking, upload, and publication; it did **not** prevent scorer execution or raw probability retention.
- Mutable processed wide/slate outputs were overwritten or suppressed (`MUTABLE_OUTPUT_RETENTION_RISK = YES`), but the certified lineage artifacts survived.
- Recovery: `DIRECT_ARTIFACT_RECOVERY_POSSIBLE`. This audit does not create a continuity ledger.
- Root cause: `HITS_PROBABILITIES_EXIST_AND_WERE_MISSED_BY_PRIOR_SEARCH`.

## Date classifications

- 2026-08-04: `GENERATED_AND_RECOVERED` — H0.5 1391 rows/276 unique; H1.5 181 rows/42 unique.
- 2026-08-05: `GENERATED_AND_RECOVERED` — H0.5 1413 rows/288 unique; H1.5 152 rows/38 unique.
- 2026-08-06: `GENERATED_AND_RECOVERED` — H0.5 846 rows/216 unique; H1.5 132 rows/34 unique.
- 2026-08-07: `GENERATED_AND_RECOVERED` — H0.5 1441 rows/288 unique; H1.5 163 rows/35 unique.
- 2026-08-08: `GENERATED_AND_RECOVERED` — H0.5 1319 rows/281 unique; H1.5 148 rows/34 unique.
- 2026-08-09: `GENERATED_AND_RECOVERED` — H0.5 1004 rows/284 unique; H1.5 167 rows/57 unique.
- 2026-08-10: `GENERATED_AND_RECOVERED` — H0.5 991 rows/190 unique; H1.5 119 rows/26 unique.
- 2026-08-11: `GENERATED_AND_RECOVERED` — H0.5 1431 rows/279 unique; H1.5 160 rows/34 unique.
- 2026-08-12: `GENERATED_AND_RECOVERED` — H0.5 1255 rows/288 unique; H1.5 135 rows/39 unique.
- 2026-08-13: `GENERATED_AND_RECOVERED` — H0.5 696 rows/189 unique; H1.5 35 rows/13 unique.

## Human decision

Review whether to authorize a separate, immutable continuity-ledger import from these certified original rows and whether to add an explicit stored `P(Under)` field to make the future retention invariant exact. No replay, grading, certification, or pipeline change is authorized here.
