# NHL SOG TOI/exposure characterization

**Classification:** `EXPOSURE_REPLAY_CONFIRMED_PROSPECTIVE_TOI_ERROR_AVAILABLE_HISTORICAL_TOI_GAP`

## Coverage and replay

- Historical certified post-cutover population: **13,389 player-games** (Feb 28–Apr 15). Replay matched within 1e-9 on 13,347; 0 numerical mismatches; 42 rows lack retained exposure/rate inputs. Maximum absolute lambda error among replayable rows: 1.33e-15.
- Prospective Oct 8–13 certified production population: **1,608 player-games**; retained feature-bound subset: **1,107**, official TOI joined for 1,107.
- Historical official TOI is absent from the certified replay rows. Historical actual-exposure comparisons therefore remain unavailable; no estimates are imputed.

## Exact hierarchy and source usage

See [production_exposure_decision_tree.md](production_exposure_decision_tree.md). The hierarchy is D10 → D20 → D5 → season 5v5+PP → season 5v5+5v4. Lambda is selected shot rate × selected exposure / 60. No exposure default, floor, cap, roster/role-derived substitute, or position-specific scorer behavior exists.

Historical retained source usage is D10 **13,347**, unavailable **42**; prospective feature-bound source usage is D10 **1,107/1,107**. The season fallbacks cannot be evaluated for performance because they did not supply exposure in these feature-bound captures. PP TOI participates only in season 5v5+PP fallback. PK TOI is not included.

## Prospective exposure findings

- TOI error (actual minus expected): mean **-0.154 min**, median **-0.113**, MAE **1.972**, RMSE **2.642**. Negative values mean expected exposure was too high. Date breakdown is in `date_metrics.csv`; error bins and threshold calibration/proper scores are in `toi_error_bins.csv`.
- TOI error vs SOG residual: Pearson r **0.174**, Spearman rho **0.145**, slope **0.089 SOG/min**, R² **0.030**. Direction aligns modestly, but TOI error explains only about 3% of linear residual variance in this capture.
- D10 was the only selected exposure source in prospective feature-bound rows. Production-selected and D10-only are identical there. D20-only, D5-only, and season exposure ablations are descriptive unselected alternatives; no tuning or correction was done. See `exposure_ablation_metrics.csv`.
- Recent vs season TOI (n=1,104): MAE recent **1.970**, season **2.449**; recent closer on **677**, season closer on **427**, ties 0. Groups use a fixed ±1-minute recent-minus-season definition. Rising/declining group metrics are in `recent_season_divergence_metrics.csv`.
- The small-sample oracle replaces expected TOI with realized TOI while holding production rate fixed. **POSTGAME_ORACLE_DIAGNOSTIC_ONLY**: production SOG count MAE 1.039; oracle MAE 1.010; apparent MAE improvement **0.030**. This modest ceiling does not suggest exposure is the dominant count-error source. Rate/exposure/interaction identity is exact within 1.2e-15 and is not causal attribution.
- Home/away, PP TOI group, player persistence, team support (minimum 20 in this short feature sample), and prior-error sequence tables are included. Position-role comparison is unavailable because canonical position was not retained in the feature-bound rows. Team/player short-window patterns are exploratory.

## Decision

Historical actual TOI is missing, exposure hierarchy rarely leaves D10 on the measured population, TOI error has a modest residual association, and actual-TOI oracle improvement is small. The data do **not** establish a systematic correctable exposure weakness or justify a bounded prospective exposure correction study yet. First bind official TOI to certified historical rows and re-evaluate stability. The apparent differences across PP groups, teams, and players are short-window exploratory and should be treated as noise absent replication.

No new provider calls, paid credits, production DB mutations, or production changes were made. Pushed: **NO**. Detailed rows, subgroup tables, source hashes, and constraints are in this package.


## Subgroup reads

In fixed actual-minus-expected exposure-error bins, mean SOG residual moves from -0.493 (expected TOI > actual by over 3 minutes), to -0.269 (1–3 minutes over), approximately zero (within 1 minute), and +0.195/+0.196 (actual TOI 1–3/>3 minutes higher). The direction is coherent, but the tail bins are small and proper scores are not strictly monotonic; see `toi_error_bins.csv`.

Recent-minus-season usage groups use a fixed ±1-minute boundary. Rising usage has mean exposure error -0.301 minutes, falling usage +0.155, and near-agreement approximately zero. This is compatible with mild overreaction to rising usage and lag on falling usage, but the feature-bound window has only four dates. Prior TOI error has modest within-window persistence; the player/date sequences are in `prior_error_sequence.csv` and `prior_error_persistence.csv`.

High PP usage (>1.5 season PP minutes/game) has exposure MAE 1.877 minutes, compared with 1.965 at ≤0.5 and 2.243 at 0.5–1.5. PP TOI does not affect the selected D10 exposure; it appears in the season fallback only. The season 5v5+PP exposure has MAE 2.449 minutes, while 5v5-only MAE is 3.537, both worse than recent TOI in this captured population. The historical certified rows do not retain official TOI, so none of these subgroup patterns has a historical robustness check. Canonical positions are also absent from the feature-bound subset; position comparisons are unavailable.
