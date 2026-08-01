# Clean-room lineup temporal-admissibility audit

## Failed ingestion

All 468 lineup observations from ingestion
`14951a25-57cb-49f1-88c1-15424cac4f94` are authentic source evidence and
classified `POST_FIRST_PITCH_OBSERVATION`. The canonical prospective view
admits zero of them.

| Metric | Result |
| --- | ---: |
| Total rows | 468 |
| July 29 rows | 288 |
| July 30 rows | 180 |
| Valid pregame | 0 |
| Post-first-pitch | 468 |
| Minimum minutes after first pitch | 1,322.76 |
| Median minutes after first pitch | 2,851.30 |
| Maximum minutes after first pitch | 3,362.76 |

The source rows, raw payloads, failed ingestion metadata, and identity-pilot
evidence remain unchanged. No cleanup was performed.

## Active temporal contract

Only `LINEUP_VALID_PREGAME` may populate confirmed-lineup or batting-order
fields. Selection now requires exact game/player identity, official confirmed
order, observation strictly before first pitch, observation no later than the
governing market, and same-run or completed strict-prior ingestion visibility.

The database keeps all authentic rows in `lineup_snapshots`. The source-only
`lineup_temporal_observation_audit` view classifies all rows, while
`valid_pregame_lineup_observations` exposes only confirmed eligible rows.

## Historical artifact finding

The failed ingestion itself was referenced by no immutable snapshot or frozen
population. A separate legacy materialization defect was found: the old query
selected `latest_lineups` without the governing-market boundary.

| Artifact | Defect |
| --- | ---: |
| July 29 frozen final population | 201 lineup-after-market rows |
| July 30 frozen Under population | 76 lineup-after-market rows |
| July 29 H1 top-order members affected | 93 |
| July 30 H1 top-order members affected | 36 |

These immutable artifacts were not rewritten. Consequently, the historical H1
top-order replication evidence is not temporally certifiable under the frozen
strict-prior contract.

Across legacy immutable snapshots, the audit also found post-first-pitch or
after-market attachments. Exact identities and governing run tags are preserved
in `prior_frozen_population_temporal_audit.csv`.

## Hardened-path validation

Snapshot `cleanroom_20260801T004509Z` was generated after hardening. It attached
63 valid strict-prior lineup rows and emitted no batting order for four rows
classified `LINEUP_NOT_RUN_VISIBLE`. All 36 bounded regression tests passed.

No model, prediction, wager, immutable source row, frozen population, or
historical closeout was modified.
