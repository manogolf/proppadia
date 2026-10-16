# NHL Points PP-TOI 32-Row Correction and Feature Replay — 2026-10-16

## Correction disposition

The 32-row proposal was independently revalidated, but **not applied**. The
read-only Supabase session authenticated as `proppadia_nhl_research_login`; it
cannot assume the historical source-correction role. No approved operator
administrative session was available through the documented secure path, and
the application environment has no dedicated correction connection. The
shared owner URL was not used as a workaround. No canonical database field was
changed.

The proposal SHA-256 is
`4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631`. It has
exactly 32 distinct player-game changes across 29 games. All 96 PBP, boxscore,
and shiftchart hashes match the retained per-game receipts. The independently
period-bounded reconstruction agrees with each replacement. A live read-only
check found all 32 canonical keys with their expected original values. It also
found all 71,627 previously corrected PP-TOI values still equal their committed
official values. The 502 unresolved games were not targeted.

## Offline training-feature replay

Because the database correction is blocked, the replay is explicitly a
**candidate counterfactual**. It overlays the 32 proposed values on the
previously source-verified recovery candidate and reuses the frozen feature
builder's 120-day strict-prior, latest-ten player-game arithmetic mean. It does
not use team PP-TOI, include target-game outcomes, score a model, or modify
predictions.

Across the frozen 90,486-row 2023–24 HGB training population:

| PP-TOI history classification | Rows |
| --- | ---: |
| All retained contributors source-verified | 21,599 |
| Some contributors verified, some not verified in recovery scope | 66,577 |
| No verified contributor among existing history rows | 272 |
| No retained history rows in the window | 1,833 |
| Target player-game absent from current raw snapshot | 205 |

The categories sum to 90,486. “Not verified in recovery scope” includes both
explicitly unresolved observations and observations outside the retained
2023–24/2024–25 recovery population; it is not a claim that every such row is
known to be corrupt. Only the 21,599 fully verified-history rows are presently
eligible for a PP-TOI source-correct training feature without additional
history evidence. No-history rows remain missing under the existing feature
semantics and are not presumed to be measured zero.

The counterfactual replay changes `mean_pp_toi_last10` on 88,101 rows. This is
feature-input sensitivity, not evidence of improved predictions. The replay
baseline reproduces the frozen feature for 88,122 of 88,448 non-NULL comparable
rows. The other 326 are unresolved baseline exceptions. All 326 have ten
current, non-NULL historical rows in the replay window, so they are not caused
by absent contributors or NULL/zero handling. Eighty have ten source-verified
contributors; 246 have seven to nine. Retained evidence does not include the
original row-bound contributor values needed to determine why those frozen
feature values differ from the current raw-history replay.

Replay outputs are retained locally under
`artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/`:

- `pp_toi_recovery_candidates_plus_32_official_overlay_COUNTERFACTUAL.csv`
  SHA-256 `6d37ed9793931e4b4615ef0b60bdb3b561ca9dfd1012c8341d97b2845259f2ce`
- `hgb_pp_toi_feature_exposure_training_plus_32_COUNTERFACTUAL.csv`
  SHA-256 `e6e7a71e7b71981f290586c0ffb2f2a49e0187c8118d228fecb9a1671294dd25`
- `hgb_pp_toi_feature_exposure_training_plus_32_COUNTERFACTUAL_summary.json`
  SHA-256 `610efb2ba6323747421f11c6a046a9633910c5325cf0980c14c915dce365a85f`

## Verification and next step

Focused tests:
`./bin/python -m pytest backend/nhl/tests/test_nhl_historical_pp_toi_recovery.py backend/nhl/tests/test_points_pp_toi_zero_semantics.py`
— 15 passed. The replay reports no prediction changes. HGB V1, Phoenix, all
other NHL models, daily processing, and historical predictions remain
unchanged.

The correction can proceed only in the existing operator-controlled session:
connect through the approved secure administrative path, `SET ROLE
proppadia_nhl_source_correction`, verify `current_user`, then execute a guarded
32-row transaction with exact original-value assertions and retain its
receipt. Do not use a research, ingestion, operations, or owner-effective
session for the write.
