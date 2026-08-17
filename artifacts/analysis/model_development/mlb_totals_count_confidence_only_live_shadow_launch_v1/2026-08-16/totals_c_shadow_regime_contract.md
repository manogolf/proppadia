# C shadow evidence regimes

The experiment begins in `C_SHADOW_PRIMARY_2026_REGIME` on 2026-08-17, with a separate daily operational classification:

- `NORMAL_COMPETITIVE_REGIME`: eligible for the primary 8/12 checkpoints.
- `LATE_SEASON_TRANSITION_WATCH`: retain and grade, but human review is required before primary inclusion.
- `LATE_SEASON_DISTINCT_REGIME`: retain and grade under `C_SHADOW_LATE_SEASON_REGIME`; never automatically pool.

Performance cannot determine regime boundaries. Until exact objective roster/elimination classification is supportable, the implementation records `LATE_SEASON_TRANSITION_WATCH` instead of inventing certainty.
