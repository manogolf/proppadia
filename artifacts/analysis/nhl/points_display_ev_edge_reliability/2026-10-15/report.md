# NHL Points displayed EV / Edge reliability surface

## Scope and formula contract

This is a descriptive study only. Oct. 10–13 are `DISCOVERY_CONTEXT`; Oct. 14 is `FIRST_PROSPECTIVE_CONTEXT`; Oct. 15 is outcome-pending. Inspection of `nhl/site/index.html` shows that the visible Points table uses HGB `p_over` and the displayed Over price. It does not compute display values from the HGB selected side. Exact UI Edge is `p_over - implied(price_over)`; EV per unit is `p_over * (decimal_odds - 1) - (1 - p_over)`. American implied probability is `100/(A+100)` for plus prices and `-A/(-A+100)` for minus prices. Fair odds invert p_over and are rounded to an integer. Attached provider prices are medians. Historical values were joined to per-date retained `points_with_market.csv` files.

## Primary population and results

Settled population: 714 Points 0.5 rows across Oct. 10–14. HGB-selected outcomes are 416–298; mean selected-side probability 70.1%, realized rate 58.3%, excess-loss rate 11.9%. HGB Brier/log loss 0.252/0.701; market 0.227/0.648.

The displayed Over Edge and EV bands against selected-side outcomes:

- Edge bands: < -15%: N=571, W-L=335-236, acc=58.7%; -15 to -10: N=297, W-L=224-73, acc=75.4%; -10 to -5: N=287, W-L=231-56, acc=80.5%; -5 to -3: N=53, W-L=46-7, acc=86.8%; -3 to -1: N=28, W-L=27-1, acc=96.4%; -1 to 0: N=12, W-L=8-4, acc=66.7%; 0 to +1: N=8, W-L=7-1, acc=87.5%; +1 to +3: N=8, W-L=4-4, acc=50.0%; +3 to +5: N=6, W-L=4-2, acc=66.7%; +5 to +10: N=10, W-L=10-0, acc=100.0%; +10 to +15: N=0, W-L=0-0, acc=0.0%; +15 to +20: N=0, W-L=0-0, acc=0.0%; +20%+: N=2, W-L=1-1, acc=50.0%
- EV bands: < -30%: N=1013, W-L=718-295, acc=70.9%; -30 to -20: N=129, W-L=82-47, acc=63.6%; -20 to -10: N=81, W-L=52-29, acc=64.2%; -10 to -5: N=11, W-L=10-1, acc=90.9%; -5 to 0: N=14, W-L=9-5, acc=64.3%; 0 to +5: N=7, W-L=6-1, acc=85.7%; +5 to +10: N=9, W-L=5-4, acc=55.6%; +10 to +20: N=2, W-L=2-0, acc=100.0%; +20 to +30: N=8, W-L=6-2, acc=75.0%; +30 to +40: N=3, W-L=3-0, acc=100.0%; +40%+: N=5, W-L=4-1, acc=80.0%

Across all settled lines, Pearson/Spearman Edge-EV correlation is 0.282/0.287. This is low because the canonical display population pairs Over-side green metrics with selected-side outcomes and because offered prices vary widely; mathematically for an Over probability `p` and break-even `q`, `EV=(p-q)/q=Edge/q`. They are related transformations, not independent model evidence, though sorting can diverge with price. Exact formula reproduction: 1284 rows, max Edge error 0, max EV error 0, fair-price mismatches 0.

Positive display regions are too sparse for monotonic claims: Edge >=10%, >=15%, and >=20% each have 2 settled rows (1–1); EV >=20% has 16 rows (13–3), EV >=30% 8 (7–1), EV >=40% 5 (4–1). These do not establish that more green improves accuracy. Across the Edge bands, performance is plainly non-monotonic; the positive extreme is especially sparse. EV also lacks stable monotonic evidence. Near-zero bands contain single-digit or low-double-digit samples. Line and date tables keep 0.5, 1.5, 2.5 separate and Oct. 14 separate.

Selected-side score comparisons, Under/Over splits, price bands, EV×Edge matrix, same/opposite-side breakouts, sort prefixes, and overlap are provided as fixed descriptive tables. Read `canonical_rows.csv` to distinguish Over display metrics from HGB-selected outcome probability. No sparse cell is promoted into a rule. Current Oct. 15 attachment contributes 490 quote-complete rows marked `PENDING_OUTCOME`.

## Workflow interpretation

1. More displayed Edge: not shown to be better; historical accuracy is non-monotonic and high-positive Edge has only 2 rows.
2. More displayed EV: not shown to be better; the apparent positive tail has small samples and is price-sensitive.
3. Where green stops helping: no stable breakpoint can be supported; do not infer one from these sparse tails.
4. HGB overconfidence: aggregate 0.5 selected-side probability exceeds realized rate by 11.9%; interpret by fixed Edge/EV bands and date, not as a display cutoff.
5. Under/Over and agreement: separated in `edge_by_side.csv` and `agreement_disagreement.csv`; the visible display itself is always Over-based.
6. Price materially changes EV for a fixed Edge through `EV=Edge/q`.
7. Both columns are related transformations, with price-dependent ranking differences.
8. Edge/EV sorting results and top-set overlap are in `sort_prefix_metrics.csv` and `ranking_overlap.csv`.
9. Near-zero has limited N and cannot be called more credible than extreme positive from this sample.
10. Green should be read as model-versus-price difference under this display formula, not as validated strength.
11. Next: continue frozen prospective collection and grade Oct. 15 after final reconciliation; do not alter UI or Funnel V1.

Decision classifications: displayed Edge `INSUFFICIENT_EVIDENCE`; displayed EV `INSUFFICIENT_EVIDENCE`; relationship `PARTIALLY_REDUNDANT` (same underlying probability/price pair, with price-driven rank changes). No maximum Edge/EV, preferred range, negative-edge inclusion, price requirement, confidence cap, or operational filter is created.

## Guardrails and backlog

Funnel V1, Points production, UI behavior and filtering remain unchanged. Preserve Funnel V1 prospective validation, Oct. 15 funnel grading after final reconciliation, SOG structural-shadow grading, fixed-blend collection, official-TOI binding and deferred Puck Line work. Tier3 override remains unnecessary. Provider calls: 0; paid credits: 0; production DB mutations: 0.
