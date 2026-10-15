# NHL Points near-zero edge / model-market complementarity

Dates: 2026-10-10, 2026-10-11, 2026-10-12, 2026-10-13, 2026-10-14. Oct. 10–13 are discovery context; Oct. 14 is the sole prospective finalized date. Oct. 15 is pending and excluded.

Primary line 0.5: 716 market rows, 714 settled, 2 unresolved.

Production edge is selected-side HGB probability minus the raw implied probability from that side’s separately aggregated median American price; raw probability includes vig. The signed no-vig probability gap is retained separately. Edge and gap are probabilities (0.01 = 1 percentage point). Bands were fixed before outcome analysis.

## Primary comparison

| Segment | N | HGB W-L | HGB Brier / log loss | Market Brier / log loss | HGB excess losses | HGB Brier advantage | HGB calibration gap |
|---|---:|---:|---:|---:|---:|---:|---:|
| All settled | 714 | 416-298 | 0.2518 / 0.7012 | 0.2270 / 0.6477 | 84.6502 | -0.0248 | -0.1186 |
| Negative near zero [-3%,0%) | 47 | 36-11 | 0.1766 / 0.5353 | 0.1787 / 0.5405 | -1.5289 | 0.0020 | 0.0325 |
| Positive near zero [0%,+3%) | 56 | 33-23 | 0.2450 / 0.6868 | 0.2530 / 0.7343 | 5.4195 | 0.0080 | -0.0968 |
| Large positive >=+10% | 345 | 178-167 | 0.2806 / 0.7660 | 0.2371 / 0.6663 | 64.3338 | -0.0435 | -0.1865 |

Positive HGB Brier advantage means lower HGB Brier loss. HGB selected-side calibration is realized selected-side accuracy minus mean selected-side model probability; market calibration is reported both selected-side and over-probability in the tables. The band CSVs provide the full predeclared detail and date splits.

## Complementarity and prices

In negative near-zero rows, HGB-only correct: 0; market-only correct: 1; same-side rows: 46; differing-side rows: 1. HGB had lower row-level Brier loss on 36 rows, market on 11; same-side average HGB Brier advantage was 0.0026. The single differing-side case was market-correct/HGB-wrong. The aggregate proper-score edge therefore comes from confidence calibration among mostly same-side picks, not independent side calls.

Of 47 negative-near-zero primary rows, selected median price buckets are reported in `fair_odds_neighborhood.csv`: 46 shorter than -120, one from -120 to -101, and zero from -100 through even money or plus money. The lone near-even row was -115 and lost. There is no retained plus-money example in this negative-edge region. Best retained executable price is unavailable: these are aggregate side medians without provider-specific quote timestamps. ROI is withheld.

Among the fixed 0.5 edge bands with at least 20 settled rows, the largest pooled HGB Brier advantage appears in `PLUS_1_TO_PLUS_3` (N=38, advantage 0.0286; 2026-10-10|2026-10-11|2026-10-12|2026-10-13|2026-10-14). The most market-dominant fixed band is `GT_PLUS_15` (N=200, HGB advantage -0.0625). Both are descriptive pooled slices; discovery and prospective results stay separate in the period table.

The 25% HGB / 75% market fixed blend had overall Brier 0.2283 versus market 0.2270; it does not improve over market overall. Fixed blend and discovery/prospective tables are descriptive. Oct. 14 negative-near-zero results cover five settled rows; one finalized prospective date cannot establish stability or manual search value.

Near-zero-negative classification: `INSUFFICIENT_EVIDENCE`. HGB’s negative-near-zero Brier/log-loss advantages are small (0.0020/0.0052) across 47 rows, while only one side disagreement occurs; treat this as insufficient evidence. Large-positive-edge classification: `MODEL_OVERCONFIDENCE_REGION`. At +10% or higher, selected-side calibration gap is -0.1865 and HGB Brier trails market by 0.0435; this is an observed overconfidence pattern, not evidence of crowding. A frozen prospective near-zero validation candidate is not justified yet.

Funnel V1 is unchanged. Provider calls: 0. Paid credits: 0. Production DB mutations: 0.

## Backlog

| Item | Status |
|---|---|
| Funnel V1 prospective validation | Ongoing, unchanged |
| Oct. 15 funnel grading | Pending outcomes |
| SOG structural shadow and fixed blends | Continue prospective grading/collection |
| Official TOI binding | Backlog |
| Puck Line | Deferred |
| Tier3 override | Not needed |

Package: `artifacts/analysis/nhl/points_near_zero_edge_complementarity/2026-10-15/`
