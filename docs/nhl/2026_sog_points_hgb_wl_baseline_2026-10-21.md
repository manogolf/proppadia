# NHL SOG and Points HGB W/L baseline (frozen 2026-10-21)

A fixed reference for comparing future NHL prop performance. It records the
selected-side win/loss of the two production player-prop models over the
season to date, by line, side and probability band.

## Scope

- **Window:** the agreed anchor is 2026-10-01. Proven SOG starts 2026-10-08
  (13 slates through 2026-10-20), because the fitted model identity was first
  bound to predictions on 2026-10-08 (commit `b6ed889c`). SOG for 2026-10-01 →
  2026-10-07 is a separate section with the model identity unproven (below).
  Points HGB starts 2026-10-10 (11 slates): HGB made no predictions before
  2026-10-09 and was still a shadow model that day. Regular season only.
- **Models:** SOG production reference `poisson_baseline / baseline_v1` (D10).
  Points `NHL_POINTS_COUNT_HGB_V1`, not the frozen Phoenix control.
- **Population:** each player's latest pregame prediction, chosen by the
  resilient selector (`select_latest_atomic_sources`, NHL Backlog P2.27), the
  same rows the restated performance summaries grade. Each day's totals were
  checked against the governed grade and the stored summary; all match.
- **Side rule:** OVER when P(over) ≥ 0.5, otherwise UNDER. Pushes cannot occur
  on these half-point lines. Non-participants and unresolved rows are excluded.
- **Bands:** P(over) edges from `backend/nhl/scripts/analyze_sog_1_5_deep.py`.
- **Not ROI:** these are side-pick win rates, not results against prices. Most
  picks are UNDERs that books price as heavy favorites; ROI and CLV need the
  retained odds snapshots.

## Totals

| Model | W-L | Win rate |
|---|---|---|
| SOG (3 lines) | 7,934-2,260 | 77.8% |
| Points HGB (3 lines) | 7,251-1,227 | 85.5% |

### SOG by line and side

| Line | All | OVER picks | UNDER picks |
|---|---|---|---|
| 1.5 | 2,215-1,183 (65.2%) | 715-479 (59.9%) | 1,500-704 (68.1%) |
| 2.5 | 2,667-731 (78.5%) | 160-176 (47.6%) | 2,507-555 (81.9%) |
| 3.5 | 3,052-346 (89.8%) | 24-34 (41.4%) | 3,028-312 (90.7%) |

### Points HGB by line and side

| Line | All | OVER picks | UNDER picks |
|---|---|---|---|
| 0.5 | 1,895-931 (67.1%) | 166-117 (58.7%) | 1,729-814 (68.0%) |
| 1.5 | 2,581-245 (91.3%) | none | 2,581-245 (91.3%) |
| 2.5 | 2,775-51 (98.2%) | none | 2,775-51 (98.2%) |

### SOG by probability band (P(over))

| Band | 1.5 | 2.5 | 3.5 |
|---|---|---|---|
| .00-.30 | 745-220 (77.2%) | 2,181-370 (85.5%) | 2,875-246 (92.1%) |
| .30-.40 | 407-222 (64.7%) | 176-81 (68.5%) | 111-44 (71.6%) |
| .40-.45 | 220-182 (54.7%) | 80-55 (59.3%) | 23-10 (69.7%) |
| .45-.50 | 128-80 (61.5%) | 70-49 (58.8%) | 19-12 (61.3%) |
| .50-.55 | 118-127 (48.2%) | 38-55 (40.9%) | 8-15 (34.8%) |
| .55-.60 | 113-87 (56.5%) | 49-58 (45.8%) | 5-4 (55.6%) |
| .60-.70 | 165-125 (56.9%) | 37-41 (47.4%) | 11-15 (42.3%) |
| .70-1.00 | 319-140 (69.5%) | 36-22 (62.1%) | none |

### Points HGB by probability band (P(over))

| Band | 0.5 | 1.5 | 2.5 |
|---|---|---|---|
| .00-.30 | 1,219-412 (74.7%) | 2,566-234 (91.6%) | 2,775-51 (98.2%) |
| .30-.40 | 352-270 (56.6%) | 15-10 (60.0%) | none |
| .40-.45 | 80-61 (56.7%) | 0-1 | none |
| .45-.50 | 78-71 (52.3%) | none | none |
| .50-.55 | 60-56 (51.7%) | none | none |
| .55-.60 | 47-36 (56.6%) | none | none |
| .60-.70 | 52-21 (71.2%) | none | none |
| .70-1.00 | 7-4 (63.6%) | none | none |

The confident bands at both ends win most often. Near 0.5 the picks are close
to a coin flip, and SOG OVER picks at 2.5 and 3.5 lose more than they win.

## SOG 2026-10-01 → 2026-10-07 (model identity unproven)

Kept separate from the totals above. These daily runs bind the prediction file
by hash in their receipts but do not record which fitted model produced it, so
the model identity is recorded as `UNPROVEN_BEFORE_2026_10_08`. Everything else
matches the proven section: receipt package and prediction-file hashes,
prior-day certificate and roster re-verification, the same selector and
pregame checks, and the same grader. On 2026-10-01 the runs recorded no
prior-day link, so the certificate was re-verified from the one retained
2026-09-30 reconciliation package (marked `PRIOR_DAY_LINK_NOT_RECORDED`).

| Line | All | OVER picks | UNDER picks |
|---|---|---|---|
| 1.5 | 1,124-567 (66.5%) | 372-241 (60.7%) | 752-326 (69.8%) |
| 2.5 | 1,347-344 (79.7%) | 89-78 (53.3%) | 1,258-266 (82.6%) |
| 3.5 | 1,533-158 (90.7%) | 14-11 (56.0%) | 1,519-147 (91.2%) |
| **Total** | **4,004-1,069 (78.9%)** | | |

Per day: 10-01 678-183, 10-02 413-127, 10-03 1,094-310, 10-04 428-112,
10-05 351-81, 10-06 784-188, 10-07 256-68. Band tables are in
`sog_unproven_by_line_band.csv`; they follow the same pattern as the proven
section (strong at the extremes, near a coin flip around .50).

## Package and regeneration

- Local package (git-ignored):
  `artifacts/analysis/nhl/sog_points_wl_baseline/2026-10-21/`, holding
  row-level grades, the tables above, the `sog_unproven_*` section, per-day totals with summary identities,
  `manifest.json` and `SHA256SUMS` (sha256 of `SHA256SUMS`:
  `d5417ff35423f02b8d1476b474d24cc45b4bf1bbdf8d2a04d280e3166e506525`).
- Regenerate from the main checkout (read-only; writes a new dated folder):
  `./bin/python backend/nhl/scripts/freeze_nhl_sog_points_wl_baseline.py --start 2026-10-08 --end 2026-10-20`
  (the unproven 2026-10-01 → 2026-10-07 SOG section is included by default).
  Run the same command with a later window to compare.
