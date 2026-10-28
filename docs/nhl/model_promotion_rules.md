# NHL model promotion and retirement rules

Status: **adopted by the operator 2026-10-25.** Applied by review; not yet enforced by code
(see "Not yet automated").
These rules decide when a candidate replaces a production model, when a shadow
is retired, and how an improvement-log entry closes. The operator approves
every production change; the rules make the evidence for that decision fixed
in advance, so it does not depend on one good day.

## Evidence that counts

- **Governed grades only:** the same graders as the daily performance summary
  and the weekly evaluation (each player-game graded once, from its latest
  pregame prediction).
- **Paired comparison:** candidate and production scored on the same
  player-games and lines. Unpaired records (different populations) do not
  count toward promotion.
- **Probability quality first:** Brier score and log loss on P(over) decide;
  W/L is reported but never decides on its own. A model can win more picks and
  still be worse calibrated.
- **Uncertainty:** paired differences use a bootstrap clustered by slate date
  (5,000 resamples); "better" means the 95% interval excludes zero.
- **Data health:** the player performance pulse and integrity checks must PASS
  for the window. A window with a known data defect is excluded and noted.

## Routes to production

### A. New model or changed recipe (challenger)
All of these must hold:
1. Runs as a shadow for at least **4 weekly evaluations** and at least
   **2,000 paired settled lines** (SOG, Points) or **100 paired games**
   (Moneyline, Puck Line).
2. Paired Brier **and** log loss better than production, both intervals
   excluding zero, over the whole window.
3. Better or equal in at least 3 of the last 4 weeks (no single-week fluke).
4. No line-and-side segment (for example SOG 2.5 OVER) worse than production
   by more than 2 Brier points with n >= 200.
5. Reproducible fit (two refits identical), frozen artifact with its own model
   identity, and a one-step rollback to the current production artifact.
6. Live parity check on a current slate: no invalid predictions, no ladder
   crossings, and a review of side flips.

### B. Source-data correction (same recipe, corrected inputs)
Used when training or live data were wrong, as with the PP-TOI refit
(2026-10-24). No shadow period:
1. Same features, recipe and hyperparameters; only the corrected data differ.
2. Holdout and retrospective scores no worse than production, and better on
   the corrected feature's segments.
3. Reproducibility, new model identity, rollback, and live parity as in A5–A6.

### C. Calibration layer (post-processing an existing model)
1. Fitted only on graded rows strictly before the evaluation window.
2. Runs as a shadow for at least 2 weekly evaluations; paired Brier and log
   loss better than the uncalibrated model with intervals excluding zero.
3. Does not change which side is picked for more than 10% of lines without a
   review of those flips.

## Retirement
- A shadow that is not better than production on paired Brier in any of
  **8 consecutive weekly evaluations** is proposed for retirement.
- A shadow that cannot be paired with production (missing captures,
  identities) for 4 weeks is proposed for retirement or repair.
- Retired shadows keep their history; only daily scoring stops.

## Improvement-log entries
- Each entry (`docs/nhl/model_improvement_log.md`) moves OPEN -> TESTING when
  a specific fix is running as a shadow, and to PROMOTED or REJECTED when
  route A, B or C decides.
- An entry unflagged for 3 weekly evaluations in a row is marked
  `OPEN_CLEAR_3_WEEKS`; the operator closes it as RESOLVED or keeps it on WATCH.
- One fix may close several entries; record the fix id on each.
- Segments are flagged at two levels (2026-10-28): all predictions, and
  two-sided lines only (picks whose line had the same book quoting Over and
  Under before the prediction; from 2026-10-10). Two-sided entries have
  `level: two_sided`, key prefix `two_sided|`, and show "(two-sided)" in the
  log. A two-sided entry is where the model meets a real market; weigh it
  first when choosing what to fix. Entries without `level` are all-predictions.

## Not yet automated
- Paired shadow-vs-production proper scores (the weekly report shows W/L for
  shadows today).
- Closing-line value. (The weekly report shows W/L, Brier and log loss on
  two-sided lines, and model vs market by gap band since 2026-10-28.)
- A promotion check that applies these rules and reports which candidates
  qualify.

## Operator decisions outside these routes
- **2026-10-25, SOG: `NHL_SOG_COUNT_HGB_V1` promoted to production without
  route A's 4-week shadow.** Operator decision: a new model that beats
  production on held-out and live data becomes the baseline once tested.
  Evidence (NHL Backlog P2.33): out of sample 2026-01..04 (24,213 rows) paired
  log loss vs the formula -0.051 (95% interval -0.056 to -0.046), Brier better
  at all three lines; 2026 slates 10-08..10-24 (4,395 rows) -0.039 (-0.050 to
  -0.028). A5 and A6 hold: reproducible refit, frozen artifact with its own
  identity, one-step rollback (`NHL_SOG_SCORER=poisson_baseline`), and a live
  parity dry run on 10-24 (no invalid probabilities or ladder crossings; side
  flips vs the formula 77/26/7 of 548 at 1.5/2.5/3.5, mostly to UNDER).
  A1-A4 were not run. The formula and SHRUNK_180 continue as shadows; the
  weekly evaluation tracks them against production.

