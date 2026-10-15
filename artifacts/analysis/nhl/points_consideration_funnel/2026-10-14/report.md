# NHL Points consideration-funnel reduction

## Decision summary

Finalized HGB market-intersection dates: 2026-10-10, 2026-10-11, 2026-10-12, 2026-10-13. Historical primary 0.5 Stage 0 is 658 rows; Stage 1 removes 120, Stage 2 removes a further 275, and Stage 3 removes 36, leaving 227 (34.5%). Stage 1 removes 120/658 (18.2%) and keeps 81.8%; its removed group was 88–32 with -5.9 excess losses, so it removed a relatively useful subset. Stage 2 removes 275/538 (51.1%), 138–137 with +55.7 excess losses. Stage 3 removes a distinct 36/263, 14–22 with +6.6 excess losses; the final survivor set is 227/658 (34.5%).

At 0.5 the full Stage 0 was 380–278, expected losses 197.5, excess +80.5, Brier .2528 and log loss .7032. Final Stage 3 survivors: 140–87, expected losses 62.9, excess +24.1, Brier 0.2400, log loss 0.6744. Proper scores improve descriptively; excess losses remain.

Stage 1, Stage 2, and Stage 3 are provisional research exclusions. Stage 3 uses the previously identified opposite-side failure region and removes its remaining distinct population; the 36-row residual remains a small early-season sample and requires prospective validation. No permanent operational filter is established.

## Edge definitions

The export `edge_over/edge_under` is model-side probability less raw American implied probability from each side’s separately aggregated median price. This raw probability includes vig. The signed no-vig gap normalizes both raw side probabilities before comparison. The site’s Edge bps uses selected model probability minus `p_over_mkt` (or raw-price fallback), and retains rows with edge × 10,000 at or above the user threshold while missing values stay visible. The frontend ranked cards use Over probability minus market Over probability and a -350 to +500 price guard. EightRain computes selected-side EV as probability / American implied probability - 1 and the model-market gap from side probabilities; its active Points policy requires both strictly >0. The fair American price is model fair odds, not edge. This study computes median-price EV for description only; cross-book medians are not executable, so no ROI is claimed.

## Funnel summary

| Line | Stage | Start | Removed | Remain | Wins removed | Losses removed | Excess losses removed | Remaining excess losses |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 0.5 | STAGE_0 | 658 | 0 | 658 | 0 | 0 | 0.0 | 80.5 |
| 0.5 | STAGE_1 | 658 | 120 | 538 | 88 | 32 | -5.9 | 86.4 |
| 0.5 | STAGE_2 | 538 | 275 | 263 | 138 | 137 | 55.7 | 30.7 |
| 0.5 | STAGE_3 | 263 | 36 | 227 | 14 | 22 | 6.6 | 24.1 |
| 1.5 | STAGE_0 | 524 | 0 | 524 | 0 | 0 | 0.0 | 45.7 |
| 1.5 | STAGE_1 | 524 | 91 | 433 | 82 | 9 | 2.4 | 43.3 |
| 1.5 | STAGE_2 | 433 | 121 | 312 | 82 | 39 | 29.2 | 14.0 |
| 1.5 | STAGE_3 | 312 | 0 | 312 | 0 | 0 | 0.0 | 14.0 |
| 2.5 | STAGE_0 | 9 | 0 | 9 | 0 | 0 | 0.0 | 0.8 |
| 2.5 | STAGE_1 | 9 | 1 | 8 | 1 | 0 | -0.0 | 0.8 |
| 2.5 | STAGE_2 | 8 | 3 | 5 | 2 | 1 | 0.9 | -0.1 |
| 2.5 | STAGE_3 | 5 | 0 | 5 | 0 | 0 | 0.0 | -0.1 |

## Survivor-only reading

0.5 survivor performance: 140–87 (61.7%); expected losses 62.9, observed 87, excess +24.1; Brier 0.2400, log loss 0.6744; no-vig market Brier 0.2310, log loss 0.6613.

Negative-edge rows were 120/658 (18.2%), 88–32, -5.9 excess losses. The Stage 2 gap region was 275/658 overall (41.8%), all HGB UNDER; fixed sensitivity among Stage 1 at >10/>15/>20pp is 423/275/151. Opposite-side disagreement was 192 total (77–115, +44.9 excess); Stage 1 removed 1, Stage 2 removed 155, leaving 36 (14–22, +6.6 excess) after Stage 2. Those 36 span all four dates, have Brier .2621 and log loss .7174 versus no-vig market .2385/.6698, justifying a provisional third stage. This is the earlier signal on the same four-date evidence, not independent replication; prospective validation remains necessary.

Stage 1, 2, and 3 date counts are in `date_level_funnel_counts.csv`. Final survivor edge bands do not improve monotonically: 0–1pp n=15, 5–10, +5.3 excess; 1–3pp n=32, 24–8, -1.7 excess; 3–8pp n=108, 69–39, +10.0 excess; 8–15pp n=72, 42–30, +10.5 excess. The 1–3pp slice has Brier .1865/log loss .5627 versus no-vig market .2193/.6900, but it is descriptive only. The 0–1pp slice is small and poor. There were no production-edge values above 15pp among final survivors. The remaining 8–15pp tail has mean model-market gap 13.1pp, mean production edge 9.8pp, selected model probability 73.0%, mean market probability 59.9%, and only negative American prices (-280 to -117.5); by construction, the gap-edge difference reflects vig normalization. Final survivors are all HGB UNDER, so there are no near-even or plus-money price rows. The three favorite-price bands are mixed (19 rows at -129 to -110: 8–11; 96 at -199 to -130: 55–41; 112 at -200 or shorter: 77–35). No minimum edge or price rule is inferred. Median-price EV is descriptive and ROI is withheld.

Model confidence improves across broad bands but remains imperfect: 80%+ is 19–5 on n=24 and close to expected losses; 60–70% is 39–34 on n=73 and 70–80% is 79–44 on n=123, both with excess losses. No-vig market Brier/log loss are better than HGB overall among survivors (.2310/.6613 versus .2400/.6744). No retained manual-style NHL.com variables were bound to these rows, so their survivor-level separating value is unresolved. No further next candidate is justified after these three provisional exclusions; continue prospective validation.

Line 1.5 remains separate: Stage 0 n=524, Stage 1 n=433, Stage 2 n=312 and Stage 3 n=312 (59.5% raw); final survivor excess losses +14.0. Line 2.5 has only 9 rows and is not decision-grade.

## Oct. 14 dry view

Integrity PASS; retained observation 2026-10-14T19:29:25.340306Z; two games [2026020106, 2026020107]. Outcome-free two-sided pregame rows: 93 of 285 immutable predictions. At 0.5: 58 raw, 9 Stage 1 removed, 26 Stage 2 removed, 3 Stage 3 removed, 20 survive. Across all lines, 43/93 (46.2%) market-usable rows and 43/285 (15.1%) immutable predictions survive. Full per-line reduction is in `current_day_dry_funnel.csv`. This view leaves immutable predictions and exports unchanged.

## Constraints and backlog

All excluded rows remain in immutable source predictions and grades, with first exclusion and overlap flags in `removed_rows.csv`. No target shortlist count or inclusion rule was imposed. Preserve the SOG structural-shadow capture and prospective grades, fixed-blend collection, historical official-TOI binding, deferred Puck Line work, and no Tier3 override. Points backlog: prospective Stage 1/2/3 validation; investigate the next survivor failure region; defer inclusion criteria; later build a read-only consideration view and revisit manual NHL.com review after automated reduction matures.

The pattern can later be reused for SOG and Saves only after their own market/outcome identity and settlement rules are bound. No other prop was changed.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Production changes: 0. Pushed: NO.

Package: `artifacts/analysis/nhl/points_consideration_funnel/2026-10-14/`.
