# RAW_V1 forecast construction map

`RAW = exp(2.196539796387852 + Σ coefficient_i × ((feature_i - training_mean_i) / training_scale_i))`

The model emits one direct full-game total. The negative-binomial layer (`alpha=0.129444799770130`) supplies uncertainty and line probabilities but does not change the point mean.

| Feature | Governed role | Coefficient | Representation |
|---|---|---:|---|
| `league_total` | strict-prior league run environment | -0.012652505 | Explicit input |
| `home_offense` | home-team strict-prior runs scored state | +0.014383005 | Explicit input |
| `home_prevention` | home-team strict-prior runs allowed state | -0.013582352 | Explicit input |
| `away_offense` | away-team strict-prior runs scored state | +0.013564476 | Explicit input |
| `away_prevention` | away-team strict-prior runs allowed state | -0.013586090 | Explicit input |
| `home_starter_ra9` | home probable starter strict-prior RA9 or governed fallback | +0.013753976 | Explicit input |
| `away_starter_ra9` | away probable starter strict-prior RA9 or governed fallback | +0.006799765 | Explicit input |
| `home_starter_prior_starts` | home probable starter history depth | +0.012677234 | Explicit input |
| `away_starter_prior_starts` | away probable starter history depth | +0.000826836 | Explicit input |
| `home_expected_outs` | home probable starter expected workload | -0.011065970 | Explicit input |
| `away_expected_outs` | away probable starter expected workload | +0.007419497 | Explicit input |
| `home_workload_uncertainty_outs` | home starter workload uncertainty | +0.010362819 | Explicit input |
| `away_workload_uncertainty_outs` | away starter workload uncertainty | +0.001432380 | Explicit input |
| `home_bullpen_ra9` | home bullpen strict-prior RA9 | +0.015070550 | Explicit input |
| `away_bullpen_ra9` | away bullpen strict-prior RA9 | +0.010854281 | Explicit input |
| `home_bullpen_likely_available_reliever_count` | home likely-available reliever count | -0.010844449 | Explicit input |
| `away_bullpen_likely_available_reliever_count` | away likely-available reliever count | +0.003558828 | Explicit input |
| `home_bullpen_recent_innings_burden` | home recent bullpen innings burden | -0.000834537 | Explicit input |
| `away_bullpen_recent_innings_burden` | away recent bullpen innings burden | +0.007154814 | Explicit input |
| `strict_prior_total_run_factor` | regressed strict-prior venue run factor | +0.043471585 | Explicit input |
| `park_history_depth` | number of prior venue games supporting the park state | -0.026813038 | Explicit input |
| `game_number` | doubleheader/game-number state | +0.003839050 | Explicit input |

Explicitly modeled: league level, team offense/prevention, probable-starter quality/history/workload, bullpen quality/availability/burden, park factor/history depth, and game number. Home/away distinctions exist only as input features to one total equation.

Implicitly represented: interactions can only arise through the common exponential link; there are no explicit interaction terms.

Unavailable for decomposition: governed home/away expected-run outputs, lineups, handedness/platoon, weather, ABS, travel/rest, and an explicit early/late-inning forecast. No such component was invented.
