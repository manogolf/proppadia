# NHL model improvement loop — plan

Status (2026-10-25): steps 1–3 built and on; the rest is planned, not
scheduled. **Operator priority: raw model output first.** Calibration is
deferred until the production models' inputs and training have had the same
scrutiny as Points HGB (P2.31); SOG is next.

## The loop
1. The weekly evaluation finds where results and model confidence disagree.
2. Each weak spot becomes an improvement-log entry with a hypothesis.
3. A candidate fix runs as a shadow and is scored against production on the
   same player-games.
4. The adopted promotion rules decide promotion, rejection or retirement.
5. Market and data checks guard against chasing noise or bad data.
6. The next weekly evaluation shows whether the weak spot closed.

## Parts

| # | Part | Purpose | Done when | Status |
|---|---|---|---|---|
| 1 | Weekly evaluation, built on Sundays by the daily run | See every model and shadow each week | Report builds unattended; daily status line | Done, on (2026-10-25) |
| 2 | Improvement log | Turn weak spots into tracked hypotheses | Weekly run adds and tracks entries | Done (`docs/nhl/model_improvement_log.md`) |
| 3 | Promotion, retirement and log-closing rules | Fix the evidence needed before any change | Adopted | Done, applied by review (`docs/nhl/model_promotion_rules.md`) |
| 4 | Paired shadow-vs-production probability scores | Let the weekly report check rule 3 | Weekly report shows paired Brier/log loss with date-clustered intervals per shadow | Planned |
| 5 | Calibration-layer shadow | Correct systematic over/under-confidence | Shadow scored under rule C | **Deferred** by operator until raw output is sound |
| 6 | Scheduled refits on recent data | Keep models current as the season grows | Monthly refit challengers scored under rule A | Planned, after the SOG review |
| 7 | Market, closing-line, feature-drift and data-invariant checks | Catch noise and bad data early | Weekly checks with PASS/FAIL lines | Planned |

## Order
SOG model review (inputs, training data, prediction mechanics) → 4 → 7 → 6.
Calibration (5) only after the raw models are trusted.
