# MLB Hits 0.5 first-principles season rebuild v1

`HITS05_2026_FRESH_START_CHRONOLOGICAL_MODEL_BUILD`

`2026_FIRST_PRINCIPLES_WALK_FORWARD_RECONSTRUCTION`

`HITS05_FIRST_PRINCIPLES_EVIDENCE_WEAK`

- Official range: 2026-03-25 through 2026-08-13 (139 slates, 1825 scheduled game records).
- Denominator/predictions/resolved: 47,198 / 47,198 / 37,082; prediction coverage 100.00%, resolution rate 78.57%.
- Model: Brier 0.250820, log loss 0.697922, ECE 0.082706.
- Baseline A: Brier 0.245839, log loss 0.684802; improvements -0.004982 / -0.013120.
- Baseline B: Brier 0.242037, log loss 0.677165; improvements -0.008783 / -0.020758.
- Confidence ordering point-monotonic: True. >=75%: n=5590, predicted=0.7833, observed=0.6406.
- August live overlap: 2,445; correlation 0.7535; mean absolute difference 0.0974.
- BetOnline reference: 544 rows; secondary only.
- Stitched-family relationship: `SAME_BROAD_STORY`.
- New certification review: `NOT_JUSTIFIED`. No certification is made here.

Material limitations: dated roster responses lack intraday version timestamps; active-roster eligibility intentionally includes bench players; historical probable-pitcher/BvP features use the frozen missing fallback; the first 2025 weeks are burn-in rather than a 2024-derived history; and this reconstruction is historical, not originally prospective.
