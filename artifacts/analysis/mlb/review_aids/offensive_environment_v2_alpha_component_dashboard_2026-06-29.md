# Environment v2-alpha Component Dashboard

- Generated at: `2026-06-29T23:00:12.965451+00:00`
- Scope: research-only Hits O1.5 component dashboard.
- v2-alpha is not a new formula.
- v2-alpha is not a replacement tier.
- v2-alpha is not an optimized score.
- Current pitcher tier remains unchanged.
- Production behavior changed: `no`
- Selectors/uploads/grading/thresholds/model predictions changed: `no`
- Morning Workbench/Ops Brief behavior changed: `no`

## Purpose

Environment v2-alpha exposes the four retained environment components side by side so research can study agreement and disagreement patterns without collapsing them into a score.

## Component Dashboard

| dashboard area | retained fields | descriptive bucket | role |
|---|---|---|---|
| Offense strength | `offense_factor_vs_league_clamped`, `offense_hits_form_blended` | low / mid / high | independent offensive-strength signal |
| Starter matchup | `pitcher_expected_hits_allowed_weighted`, `pitcher_base`, `starter_expected_hits_allowed` | low / mid / high | current pitcher-tier anchor, unchanged |
| Bullpen support | `bullpen_hits_allowed_form_blended` | low / mid / high | full-game continuation context |
| Full-game rollup | `team_expected_hits_allowed` | low / mid / high | disagreement detector / rollup |

## Research-Only Bucket Definitions

These are transparent fixed bins for inspection. They are not tuned to ROI and are not production thresholds.

| field | low | mid | high |
|---|---|---|---|
| `offense_factor_vs_league_clamped` | `< 0.95` | `0.95 to < 1.05` | `>= 1.05` |
| `offense_hits_form_blended` | `< 8.0` | `8.0 to < 9.0` | `>= 9.0` |
| `pitcher_expected_hits_allowed_weighted` / `pitcher_base` | `< 4.5` | `4.5 to < 5.5` | `>= 5.5` |
| `starter_expected_hits_allowed` | `< 4.5` | `4.5 to < 5.5` | `>= 5.5` |
| `bullpen_hits_allowed_form_blended` | `< 3.5` | `3.5 to < 4.5` | `>= 4.5` |
| `team_expected_hits_allowed` | `< 8.0` | `8.0 to < 9.0` | `>= 9.0` |

## Source Coverage

- Dashboard rows: `1777`
- Resolved dashboard rows: `1703`
- Dashboard health: `PASS`

## Agreement / Disagreement Profiles

| profile | rows | resolved | W-L-P | WR | ROI | units | avg odds | sample |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `component_missing` | `328` | `289` | `88-201-0` | `30.45%` | `-20.06%` | `-57.96` | `158.97` | `ok` |
| `team_expected_high_starter_mediocre` | `185` | `181` | `47-134-0` | `25.97%` | `-32.88%` | `-59.50` | `153.32` | `ok` |
| `offense_low_starter_low_bullpen_low` | `160` | `157` | `30-127-0` | `19.11%` | `-51.28%` | `-80.52` | `154.20` | `ok` |
| `offense_low_starter_mid_bullpen_low` | `137` | `134` | `30-104-0` | `22.39%` | `-41.75%` | `-55.94` | `159.48` | `ok` |
| `offense_mid_starter_mid_bullpen_low` | `119` | `117` | `31-86-0` | `26.50%` | `-31.13%` | `-36.42` | `159.73` | `ok` |
| `offense_low_starter_low_bullpen_mid` | `104` | `101` | `29-72-0` | `28.71%` | `-29.80%` | `-30.09` | `139.57` | `ok` |
| `offense_high_starter_high_bullpen_high` | `88` | `85` | `52-33-0` | `61.18%` | `55.94%` | `47.55` | `153.98` | `ok` |
| `offense_high_starter_high_bullpen_mid` | `81` | `81` | `30-51-0` | `37.04%` | `-4.59%` | `-3.72` | `153.16` | `ok` |
| `offense_high_starter_high_bullpen_low` | `75` | `70` | `30-40-0` | `42.86%` | `13.36%` | `9.35` | `153.47` | `ok` |
| `starter_high_team_expected_mediocre` | `66` | `61` | `22-39-0` | `36.07%` | `-4.83%` | `-2.94` | `153.82` | `ok` |
| `offense_mid_starter_mid_bullpen_mid` | `53` | `53` | `18-35-0` | `33.96%` | `-17.03%` | `-9.02` | `142.85` | `ok` |
| `offense_high_starter_mid_bullpen_low` | `52` | `52` | `24-28-0` | `46.15%` | `20.37%` | `10.59` | `158.90` | `ok` |
| `offense_mid_starter_high_bullpen_mid` | `54` | `52` | `19-33-0` | `36.54%` | `-4.39%` | `-2.28` | `144.73` | `ok` |
| `offense_mid_starter_low_bullpen_low` | `44` | `43` | `12-31-0` | `27.91%` | `-22.12%` | `-9.51` | `170.79` | `thin_lt_50` |
| `offense_low_starter_mid_bullpen_mid` | `42` | `41` | `12-29-0` | `29.27%` | `-19.59%` | `-8.03` | `173.78` | `thin_lt_50` |
| `offense_high_starter_low_bullpen_low` | `29` | `28` | `14-14-0` | `50.00%` | `33.54%` | `9.39` | `155.25` | `thin_lt_50` |
| `offense_low_starter_high_bullpen_mid` | `22` | `21` | `6-15-0` | `28.57%` | `-35.44%` | `-7.44` | `107.00` | `small_lt_25` |
| `offense_mid_starter_high_bullpen_low` | `21` | `21` | `4-17-0` | `19.05%` | `-49.05%` | `-10.30` | `177.86` | `small_lt_25` |
| `offense_high_starter_low_bullpen_mid` | `19` | `19` | `5-14-0` | `26.32%` | `-34.89%` | `-6.63` | `158.26` | `small_lt_25` |
| `offense_low_starter_high_bullpen_high` | `18` | `18` | `5-13-0` | `27.78%` | `-27.00%` | `-4.86` | `162.78` | `small_lt_25` |
| `offense_mid_starter_low_bullpen_mid` | `17` | `17` | `5-12-0` | `29.41%` | `-24.24%` | `-4.12` | `131.18` | `small_lt_25` |
| `offense_high_starter_mid_bullpen_mid` | `16` | `16` | `5-11-0` | `31.25%` | `-13.44%` | `-2.15` | `180.44` | `small_lt_25` |
| `offense_low_starter_low_bullpen_high` | `17` | `16` | `3-13-0` | `18.75%` | `-52.37%` | `-8.38` | `154.56` | `small_lt_25` |
| `offense_mid_starter_high_bullpen_high` | `16` | `16` | `7-9-0` | `43.75%` | `18.69%` | `2.99` | `169.38` | `small_lt_25` |
| `offense_high_starter_low_bullpen_high` | `9` | `9` | `5-4-0` | `55.56%` | `55.44%` | `4.99` | `173.67` | `small_lt_25` |
| `offense_low_starter_high_bullpen_low` | `3` | `3` | `0-3-0` | `0.00%` | `-100.00%` | `-3.00` | `183.00` | `small_lt_25` |
| `offense_mid_starter_low_bullpen_high` | `2` | `2` | `1-1-0` | `50.00%` | `-20.24%` | `-0.40` | `9.00` | `small_lt_25` |

## Guardrails

- The dashboard preserves all component values beside every bucket and flag.
- `starter_expected_hits_allowed` remains the current second-letter input.
- No tier labels are recalculated here.
- No candidate inclusion, ranking, upload, or grading path consumes these fields.

## Next Research Use

Use `offensive_environment_v2_alpha_dashboard_rows_2026-06-29.csv` to study whether agreement profiles remain stable across future completed slates. Do not promote any profile into a rule without a separate bakeoff and doctrine checklist.
