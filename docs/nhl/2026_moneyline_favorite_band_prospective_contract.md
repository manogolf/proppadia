# NHL Moneyline favorite-band prospective contract

**Policy frozen:** 2026-10-17. **Prospective evaluation starts:** 2026-10-18 slate date.
This is an offline evaluation definition. It does not change scoring, odds
acquisition, production authority, or wagering behavior.

## Population and fixed bands

Retain every paired V3/V2 Moneyline candidate from the existing full-window
candidate export and the ordinary daily prediction and postgame reconciliation
artifacts. Keep one candidate row per model/game, including games outside a
favorite band, pick'ems, unresolved outcomes, and games without qualified
prices. Do not create another acquisition path. Pair by game ID and immutable
prediction identity; retain prediction source path/hash and the historical
model role.

Band the **BetOnline market favorite** using the two-sided no-vig implied
probability, calculated from decimal odds as `1/decimal_home` and
`1/decimal_away`, then normalized to sum to one:

| Band | Favorite no-vig probability |
|---|---:|
| 50 to under 55 | `[0.50, 0.55)` |
| 55 to under 60 | `[0.55, 0.60)` |
| 60 to under 70 | `[0.60, 0.70)` |
| 70 or higher | `[0.70, 1.00]` |

Exact 50/50 is `PICKEM`; missing or unpaired prices are `MISSING_PRICE`.
These boundaries are fixed for the prospective period and were selected as
interpretable probability tiers, not from historical outcome performance.
Retain raw American prices, both sides' implied probabilities, no-vig
probabilities, favorite side, and band. Keep model pick orientation separate:
home probability `>= 0.5` selects HOME, preserving the current deterministic
tie rule. Record whether each model picked the market favorite or underdog,
plus signed and absolute home-probability gap. Those fields are descriptive;
they are not filters.

## BetOnline price and chronology policy

Use only `betonlineag` and require both HOME and AWAY prices from the same
source capture, source update timestamp, and observation timestamp. Among
complete pairs, take the latest observation satisfying
`source_update <= observation <= prediction_timestamp < scheduled_start`.
No alternate-book substitution, best-price selection, or stale-price carry
forward is allowed. Keep every game row when the pair is absent and mark its
price status `MISSING_PRICE`.

Convert American prices to decimal odds with `1 + price/100` for positive
prices and `1 + 100/abs(price)` for negative prices. Predictive measures use
settled official game outcomes only. Price-return measures use one hypothetical
unit risked on each model's probability-selected side, at the selected
BetOnline price; wins net `decimal_odds - 1`, losses net `-1`, and ROI is net
units divided by eligible wagers. Pushes/voids, if present in official grading,
are excluded from wins/losses and reported separately. Missing prices are
excluded from price denominators, never treated as zero or losses. Report
candidate games, settled games, priced settled games, favorite/underdog picks,
and wagers as separate denominators.

## Model roles, reporting, and flow

Keep V3 and V2 as paired immutable identities. For predictions before
`2026-10-14T15:27:11Z`, label V3 challenger and V2 reference; at/after that
timestamp label V3 production and V2 shadow. Do not pool role labels away:
report the full paired comparison and pre/post-cutover slices separately.
Report the four bands, all-candidate/settlement/price coverage, favorite-side
and model-side accuracy, Brier/log loss and calibration, one-unit ROI, date
stability, and disagreement direction/magnitude. Do not optimize a threshold
or claim a winner from the exploratory baseline.

The standard NHL daily command already acquires the shared odds observation
and emits the parent receipt and cross-market Moneyline capture. Existing
prediction artifacts identify V3/V2; ordinary postgame reconciliation supplies
settled outcomes. The retained all-candidate export joins these artifacts and
the two-sided quote lineage. This study consumes those retained outputs only;
it adds no acquisition or daily orchestration.

The compact historical baseline is
[`2026_moneyline_favorite_band_baseline.json`](2026_moneyline_favorite_band_baseline.json).
It is exploratory, covers 120 settled games through October 16, and is not used
to pick the frozen bands. Generated candidate/quote CSVs remain local under
`artifacts/analysis/` and are intentionally not committed.
