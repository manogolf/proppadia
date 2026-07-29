# UBO-5 TB 1.5 decommission tests

| Check | Result | Evidence |
|---|---|---|
| daily run never loads UBO-5 artifact | PASS | zero reachable scorer in mlb-predictions-wide |
| daily run never invokes UBO-5 scorer | PASS | True |
| old enable flag cannot reactivate UBO-5 | PASS | compatibility route ignores enabled and tests pass |
| future TB1.5 probability is incumbent | PASS | scoring route removed |
| future UBO probability remains null | PASS | compatibility test |
| no new UBO snapshot directory | PASS | no writer reachable |
| current aliases are notices | PASS | candidate names removed |
| neutral BOL board builds | PASS | 112 rows |
| installed wrapper UBO scoring/board commands | PASS | True |
| Daily Ops UBO sections | PASS | True |
| historical evidence preserved | PASS | no dated UBO artifacts deleted |

Smoke metrics:

- UBO-5 artifacts loaded: `0`
- UBO-5 scoring calls: `0`
- TB 1.5 incumbent rows in current wide file: `133`
- Neutral BOL market rows: `112`
- New UBO-5 prediction snapshots: `0`
- Active UBO-5 routed rows: `0`
- Neutral BOL files written: `4`
- Scheduled UBO scoring/board references remaining: `0`
- Daily Ops UBO sections remaining: `0`
- Active upload UBO route references remaining: `0`
