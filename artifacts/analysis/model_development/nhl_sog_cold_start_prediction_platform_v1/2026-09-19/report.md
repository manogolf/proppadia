# NHL SOG cold-start prediction platform V1

## Decision

The odds-independent feature and prediction platform is ready. `D_PLAYER_ROLE_HIERARCHICAL` is the selected coverage-capable arm. A/B/C, team-aware E, and the gradual ten-appearance G transition are shadow comparators; E's aggregate proper-score change is negligible and G is mixed, so neither can replace D. F is implemented as explicitly unqualified until real strictly-prior preseason evidence accumulates. Sportsbook data is not an input.

## Historical replay

| Variant | Rows | Player-games | Accuracy | Brier | Log loss | ECE | AUC |
|---|---:|---:|---:|---:|---:|---:|---:|
| A_PRIOR_SEASON_CARRY_FORWARD | 34803 | 11601 | 0.779617 | 0.151605 | 0.472964 | 0.016197 | 0.794926 |
| B_PRIOR_SEASON_RECENCY_WEIGHTED | 34803 | 11601 | 0.770853 | 0.157380 | 0.491338 | 0.032008 | 0.778215 |
| CONTROL_FROZEN_ZERO_DEFAULT | 36660 | 12220 | 0.741053 | 0.258947 | 7.154972 | 0.258947 | 0.500000 |
| CONTROL_POSITION_PRIOR | 36660 | 12220 | 0.733197 | 0.171172 | 0.516643 | 0.021338 | 0.714986 |
| C_MULTISEASON_SHRUNK_PLAYER | 35373 | 11791 | 0.779465 | 0.151989 | 0.463552 | 0.021452 | 0.794277 |
| D_PLAYER_ROLE_HIERARCHICAL | 36660 | 12220 | 0.776950 | 0.151736 | 0.462877 | 0.024499 | 0.792364 |
| E_TEAM_CHANGE_AWARE | 36660 | 12220 | 0.777823 | 0.151714 | 0.462819 | 0.025147 | 0.792674 |
| G_COLD_START_TO_CURRENT_SEASON_BLEND | 36660 | 12220 | 0.777987 | 0.152488 | 0.465623 | 0.018247 | 0.789168 |

The replay covers seasons 2024 and 2025, 12,220 participation-conditioned player-games through ten prior team games, and the canonical 1.5/2.5/3.5 ladder. Identical-row, season, line, team, player-class, and exposure-depth slices are in `variant_comparison.csv`. Historical preseason logs do not exist locally, so F is implementation-ready but not historically evaluated. Raw pre-2025 skater team IDs failed plausibility, so the replay deterministically reconstructs canonical team/opponent from official game home/away IDs and each retained `is_home` flag. Position is a current retained reference without historical as-of versions. No September 2026 outcome selected a weight or threshold.

## Exact TOI semantics

`szn_toi_per_game_5on5`, `szn_toi_per_game_pp`, and `szn_toi_per_game_pk` are season-to-date, strictly-prior averages in minutes derived from shift/manpower overlap. `season_5on5_icetime_per_game`, `season_5on4_icetime_per_game`, and `season_4on5_icetime_per_game` are the same average quantities in seconds before division by 60. Rolling `d5/d10/d20_toi_min_avg` fields are prior-game all-situations minutes per game and precede season situation fallbacks in the Poisson scorer. The production Poisson path defaulted a missing rate/TOI chain to lambda zero; legacy Denali training and scoring filled numeric nulls with 0.0. This contract does neither.

## Operational boundary

September 19 remains blocked prospective evidence. The earliest eligible date is September 20, contingent on a retained canonical nonempty pregame slate and active-roster identity. MIDDAY and FINAL_PREGAME predictions are immutable, market-free, and separately gradeable. Market columns remain explicitly unavailable. No wagering, upload, public publication or model promotion is authorized.
