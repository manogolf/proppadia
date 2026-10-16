# NHL Points HGB live binding recovery and foundation review

## Recovery result

The blocking run was `nhldaily_20261016T003210071223Z_d9962266` for slate `2026-10-15`. Its Points production lane failed before feature export completed with `HGB_HISTORY_TEAM_BINDING_INVALID`. The guard built a supposed canonical physical team from the skater log's `is_home` flag and then compared that result with the official player team ID. This made a descriptive log flag part of the authoritative team binding.

The exporter now binds `canonical_team_id` from the official outcome player team, falling back to the raw log team only when the outcome team is absent, and accepts it only when it equals exactly one of that game's canonical home/away IDs. If both official and log team IDs exist and disagree, it raises `HGB_HISTORY_TEAM_SOURCE_CONFLICT` with game/player keys. Missing, out-of-game, or ambiguous team bindings still fail closed and include the first affected keys. The frozen HGB feature list, history window, model, scaler, scoring path, and Phoenix implementation are unchanged. The frozen artifact remains SHA `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`; its `StandardScaler` joblib identity remains `d1727493246a03a3cd12e3fa7c3b3b4a`.

The failed run's exact HGB input CSVs were held in a temporary directory and deleted by the daily runner's `finally` cleanup. The run-bound `points_feature_evidence` package is a distinct SQL/Phoenix source capture: it retains 2,664 prior player-game rows and target bindings, but omits both `is_home` and `pp_toi_minutes`. Therefore the retained package cannot identify the exact original offending key or reproduce every HGB feature. No retrospective operational prediction was created.

Independent checks on those retained rows found 2,664/2,664 matching immutable official outcome identities, zero outcome-vs-log team-ID disagreements, and 2,664/2,664 log team IDs matching exactly one canonical home/away side. These checks establish that the retained team's identity and the official game sides are coherent; they do not establish what the deleted HGB snapshot's `is_home` values were.

The same slate had an earlier, separate successful capture under run `nhldaily_20261015T140453183604Z_bfa01e59`: 547 identities / 1,641 line predictions, with the same frozen HGB artifact SHA `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`. That capture predates the binding guard that failed in `d9962266`; it is not a replay or replacement for the failed run. The failed run has no HGB prediction artifact and no same-run Phoenix control. A live retry is needed to confirm this code fix against a fresh authoritative input snapshot.

## Validation performed

- `./bin/python -m unittest backend.nhl.tests.test_points_team_shots_evidence backend.nhl.tests.test_points_hgb_official_team_shots_capture backend.nhl.tests.test_points_hgb_mixed_start_production`: 26 tests passed. Regression coverage proves a stale `is_home` value no longer changes the independently bound team, while an actual team-source conflict still fails closed.
- `./bin/python -m py_compile backend/nhl/scripts/export_nhl_points_hgb_features.py backend/nhl/tests/test_points_team_shots_evidence.py`: passed.
- Offline same-run retained-source audit: all 2,664 log rows joined to immutable outcomes and mapped to exactly one canonical game side. A full feature/scoring replay was not possible because retained inputs lack PP TOI and the HGB-specific source files were cleaned up.
- The retained same-run team-shot evidence reports 550/550 target rows exact against its SQL export, with maximum absolute difference `4.27e-14`. Earlier official team-shot lineage repair evidence validated 1,800/1,800 rows. Neither is evidence of a new HGB capture for the failed run.
- The separate `research_integrity` lane reported `COMPLETE_WITH_BOUNDED_LIMITS`; its SOG integrity child exited 2. This is nonblocking and was not repaired here.

## Pre-HGB foundation review

| Lineage | Frozen identity and evidence | Foundation status before HGB | Present disposition |
|---|---|---|---|
| Phoenix incumbent | `points_phoenix_lr`, `phoenix_v2`; fitted identity `648e1b4d7a8e4e96c35f15ff614d234ca935ee0c6e2c80df1084b7b35051e9a5`; `POINTS_PLAYER_HISTORY_CROSS_SEASON_V2` / `export_points.sql` | Frozen training frame was not retained, so training-feature boundary is unproven. Effective policy config was missing; surviving operator surface is research ranking only. Earlier Oct. 9 daily input did not contain the HGB CORE fields, so no HGB-comparable shadow could be certified then. | Phoenix remains production incumbent. It ran as a same-run shadow in the earlier Oct. 15 run, but no Phoenix control was available in failed run `d9962266`. No Phoenix code/model changes here. |
| HGB V1 | `NHL_POINTS_COUNT_HGB_V1`; artifact SHA `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`; fitted identity `a53f43d5caea9a64ee8e6d48daf8072e0332b629998242639b2ba15be743e5c2`; 10 CORE features; `120_DAY_LEGACY_BOUND` | Bakeoff and 2025 independent holdout selection were completed Oct. 9. The same-slate model captured successfully earlier Oct. 15. | Research authority only; Phoenix production authority unchanged. The HGB was frozen before the Oct. 15 attempts/team-shot source audits and canonical team-shot repair. |

The foundation order matters: the HGB model was selected before the later operational source audits found `attempts_d10_per60` support collapse in 2026 and a team-shot lineage defect. The Oct. 15 scoring-history and confidence studies reproduce predictions and several history features, but classify performance cause as provisional/multifactor; the missing-attempts issue is confirmed data quality with causal status insufficient. Team-shot lineage was separately repaired and validated. Those findings concern input foundation/performance, not the live binding exception itself, and do not justify retraining or calibration.

## Final classification

- Live recovery: `CODE_REPAIR_VALIDATED; FRESH_LIVE_RETRY_PENDING`. The guard repair passes focused regressions and retained team IDs are coherent, but the failed run's exact `is_home` values and full PP-TOI inputs are unavailable for an exact replay.
- Predecessor foundation: `HGB_BUILT_BEFORE_OPERATIONAL_FOUNDATION_DIAGNOSES; PERFORMANCE_CAUSE_PROVISIONAL_MULTIFACTOR`. Phoenix's historical training boundary remains unproven; HGB's own 2023–24 training and 2025 holdout evidence remain distinct from the later diagnosed 2026 input defects.

No full daily run, provider call, database write, retraining, calibration, authority change, or SOG repair was performed.
