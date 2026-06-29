# Environment v2-alpha Dashboard Health

- Generated at: `2026-06-29T23:00:12.964679+00:00`
- Scope: research-only dashboard coverage.
- Production behavior changed: `no`
- Tier assignment changed: `no`

## Summary

- Dashboard rows: `1777`
- Checks PASS: `24`
- Checks WARN: `0`

## Field Coverage

| group | field | nonblank | rows | coverage | status |
|---|---|---:|---:|---:|---|
| `component` | `offense_factor_vs_league_clamped` | `1756` | `1777` | `98.82%` | `PASS` |
| `component` | `offense_hits_form_blended` | `1756` | `1777` | `98.82%` | `PASS` |
| `component` | `pitcher_expected_hits_allowed_weighted` | `1449` | `1777` | `81.54%` | `PASS` |
| `component` | `pitcher_base` | `1449` | `1777` | `81.54%` | `PASS` |
| `component` | `starter_expected_hits_allowed` | `1449` | `1777` | `81.54%` | `PASS` |
| `component` | `bullpen_hits_allowed_form_blended` | `1756` | `1777` | `98.82%` | `PASS` |
| `component` | `team_expected_hits_allowed` | `1449` | `1777` | `81.54%` | `PASS` |
| `dashboard` | `env_v2_alpha_research_status` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_bucket_schema` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_offense_strength_bucket` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_offense_form_bucket` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_pitcher_base_bucket` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_starter_matchup_bucket` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_bullpen_support_bucket` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_team_rollup_bucket` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_agreement_profile` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_offense_high_starter_high_bullpen_high` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_offense_high_starter_high_bullpen_low` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_offense_high_starter_low_bullpen_high` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_offense_low_starter_high_bullpen_high` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_team_expected_high_starter_mediocre` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_starter_high_team_expected_mediocre` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_component_missing_count` | `1777` | `1777` | `100.00%` | `PASS` |
| `dashboard` | `env_v2_alpha_component_coverage` | `1777` | `1777` | `100.00%` | `PASS` |

## Agreement Profiles

| profile | rows |
|---|---:|
| `component_missing` | `328` |
| `team_expected_high_starter_mediocre` | `185` |
| `offense_low_starter_low_bullpen_low` | `160` |
| `offense_low_starter_mid_bullpen_low` | `137` |
| `offense_mid_starter_mid_bullpen_low` | `119` |
| `offense_low_starter_low_bullpen_mid` | `104` |
| `offense_high_starter_high_bullpen_high` | `88` |
| `offense_high_starter_high_bullpen_mid` | `81` |
| `offense_high_starter_high_bullpen_low` | `75` |
| `starter_high_team_expected_mediocre` | `66` |
| `offense_mid_starter_high_bullpen_mid` | `54` |
| `offense_mid_starter_mid_bullpen_mid` | `53` |
| `offense_high_starter_mid_bullpen_low` | `52` |
| `offense_mid_starter_low_bullpen_low` | `44` |
| `offense_low_starter_mid_bullpen_mid` | `42` |
| `offense_high_starter_low_bullpen_low` | `29` |
| `offense_low_starter_high_bullpen_mid` | `22` |
| `offense_mid_starter_high_bullpen_low` | `21` |
| `offense_high_starter_low_bullpen_mid` | `19` |
| `offense_low_starter_high_bullpen_high` | `18` |
| `offense_mid_starter_low_bullpen_mid` | `17` |
| `offense_low_starter_low_bullpen_high` | `17` |
| `offense_high_starter_mid_bullpen_mid` | `16` |
| `offense_mid_starter_high_bullpen_high` | `16` |
| `offense_high_starter_low_bullpen_high` | `9` |
| `offense_low_starter_high_bullpen_low` | `3` |
| `offense_mid_starter_low_bullpen_high` | `2` |
