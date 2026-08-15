# First-principles evidence assessment

`HITS05_FIRST_PRINCIPLES_EVIDENCE_WEAK`

`NEW_CERTIFICATION_REVIEW = NOT_JUSTIFIED`

- Completion: yes; 139 official slates from 2026-03-25 through 2026-08-13.
- Denominator/predictions/resolved: 47,198 / 47,198 / 37,082; prediction coverage 100.00%, PA-resolution rate 78.57%.
- Aggregate: Brier 0.250820, log loss 0.697922, ECE 0.082706.
- Baselines: model Brier was worse than population by 0.004982 and worse than hitter-shrunk by 0.008783; all six temporal blocks were worse than both.
- Temporal stability: monthly/opening Brier ranged 0.246960–0.260627; ECE ranged 0.074042–0.119016.
- Confidence ordering: cumulative quintiles were monotonic; 4 of 6 temporal blocks were monotonic.
- Upper tail: >=75% rows n=5,590, mean prediction 0.7833, observed 0.6406, gap 0.1427; not acceptable.
- Clustered uncertainty: Brier improvement versus population 95% CI [-0.006420, -0.003575], versus hitter-shrunk [-0.009885, -0.007653]; both exclude zero on the harmful side.
- August live reference: 2,445 overlaps, correlation 0.7535, fresh/live Brier 0.251672/0.244412.
- BetOnline reference: 544 secondary rows, model/BetOnline Brier 0.264505/0.254087; no EV or ROI was calculated.
- Stitched-family relationship: `SAME_BROAD_STORY`; old/fresh Brier 0.246290/0.250820.
- Decision: a new certification review is not justified. This is not a certification.

Material limitations: dated roster responses lack intraday version timestamps; active-roster eligibility includes bench players; historical probable-pitcher/BvP features use the frozen missing fallback; the first 2025 weeks are burn-in rather than 2024-derived history; and this is reconstructed historical evidence rather than original prospective operation.
