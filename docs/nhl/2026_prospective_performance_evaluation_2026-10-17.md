# NHL 2026 prospective performance review — 2026-10-17

## Scope and method

This is a read-only synthesis of retained regular-season prediction, odds,
reconciliation, and grade packages available through October 16, 2026. October
17 predictions were not settled at the evaluation cutoff and are excluded.
Preseason and postseason rows are excluded. For daily performance summaries,
the latest revision per slate date was used. Line-level prop records are not
counted as independent games; both the line count and underlying identity count
are reported where known.

This review did not acquire odds, query or mutate production, retrain models,
or change model authority. Accuracy records are descriptive selection records;
they are not actual wager histories unless an observed quote, specified side,
and one-unit staking convention are all bound. The two existing priced ROI
studies below meet that narrow descriptive definition, but neither establishes
a durable edge.

## Performance snapshot

| Lane | Role and retained denominator | Observed performance | Bound price coverage / ROI |
|---|---|---|---|
| Moneyline | Production authority is `NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3` from Oct 14; `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2` is shadow after cutover. The fully paired and priced study contains 89 games, Sep 29–Oct 12. A separate Oct 13 daily grade has 16 V3 and 16 V2 game outcomes (10–6 and 11–5). Oct 14–16 adds 15 settled V3 production games (11–4). The post-cutover daily summary role labels repeat V3 as both `reference` and `challenger`, so those 15 rows are not treated as a V3/V2 comparison. | On the 89-game paired window, V3: 59–30 (66.3%), Brier 0.2225, log loss 0.6372, AUC 0.6923. V2: 51–38 (57.3%), Brier 0.2288, log loss 0.6467, AUC 0.6367. Market consensus: 55–34 (61.8%), Brier 0.2279, log loss 0.6473, AUC 0.6648. Across 18 pick disagreements, V3 won 13 and V2 won 5. V3 selected the market favorite in 69/89 games (47–22) and the underdog in 20/89 (12–8). V2 selected favorite 71/89 (44–27) and underdog 18/89 (7–11). The Oct13 and Oct14–16 rows are kept separate because their priced paired dataset is not in the 89-game study. | Consensus and BetOnline pregame price were bound on all 89 paired games. At one unit risk per selected game, retained BetOnline price ROI was V3 +13.3% (11.85 units), V2 −4.3% (−3.83 units), market favorite +1.5% (+1.35 units). These are in-sample descriptive returns, not staking recommendations. The large-gap bins are thin: V3 had 15 games at 10–15 percentage points and only 3 at ≥15 points; the last bin is too small to support a market-fade claim. |
| Puck Line | `NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1` is the reference/control; `NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2` is a shadow challenger. Latest per-date grade summaries through Oct 16 contain 120 reference and 115 challenger game outcomes; later 15 outcomes have not been folded into a new pooled probability evaluation. The existing fully paired market study covers 100 games, Sep 29–Oct 13. | In the paired 100-game study, three-class accuracy was 36% reference and 35% challenger. Multiclass log loss was 1.1107 and 1.1118; Brier sums were 0.6764 and 0.6789. Both were worse than the uniform three-class references (1.0986 log loss, 0.6667 Brier sum). They disagreed on 21 games: reference-only correct 8, challenger-only correct 7, both wrong 6. | Same-book qualified standard puck-line prices were available for 73/100 games; no-vig market consensus had the best binary home −1.5 Brier (0.2168 vs 0.2350 reference and 0.2297 challenger). Descriptive BetOnline returns on predicted directional 2+ margins only: reference 18–20, 38 priced selections, +6.13 units / +16.1%; challenger 12–11, 23 priced selections, +4.86 units / +21.1%. The one-goal class abstained. These small, selected samples do not offset the weak full-population three-class scores. |
| Shots on goal | Production: `poisson_baseline / baseline_v1`, D10. Retained daily grades through Oct 16 cover 1,821 player-games / 5,463 line outcomes on 9 slates: 4,248 wins, 1,215 losses, with 1,251 unresolved line outcomes. | Selected-side record is 77.8%. Per line: 1.5, 1,184/1,821 (65.0%); 2.5, 1,426/1,821 (78.3%); 3.5, 1,638/1,821 (90.0%). These threshold records are highly affected by the UNDER-heavy distribution and should not be read as edge. In the formal paired Oct 10–13 study (1,107 player-games / 3,321 line rows), D10 Brier/log loss were 0.1499/0.4652, count MAE 1.0395. D10/D20 25/75 blend was 0.1475/0.4559, MAE 1.0276; 50/50 was 0.1478/0.4573, MAE 1.0291. 25/75 improved Brier/log loss on 3 of 4 dates but remains classified small and unstable. | Across the 9 retained SOG coverage records, 2,992/6,714 prestart prediction-line rows had bound market matches (44.6%). Coverage does not certify every matched quote as the best executable price. No pooled ROI is reported because there is no single locked book, price-selection rule, and selection policy bound across this full window. |
| Points | Production authority: `NHL_POINTS_COUNT_HGB_V1`; `phoenix_v2` remains a shadow with corrected-source missingness semantics whose historical comparability is not established. Eight HGB grade packages include 1,427 settled player identities. Seven exact same-capture HGB/Phoenix comparisons cover 1,355 identical settled identities / 4,065 line outcomes. | On the common seven-capture set, HGB Brier/log loss by over threshold: 0.5, 0.2211/0.6345; 1.5, 0.0834/0.3097; 2.5, 0.0145/0.0768. Phoenix on those identical identities: 0.3063/0.8364; 0.3638/0.9909; 0.4820/1.4118. Selected-side records across all three lines: HGB 3,457–608; Phoenix 1,475–2,590. HGB won 56 and Phoenix 14 of the 70 disagreement rows on Oct 16 alone. These aggregates combine strongly different side distributions and are descriptive; retain the shadow's comparability caveat. | The daily summaries do not provide bound market-coverage counts for the common evaluation set; no Points ROI is reported. |
| Saves | `phoenix_v2` conditional-on-start model. Through Oct 16: 3,120 settled goalie-line outcomes (2,086 wins, 1,034 losses) and 923 unresolved, across 18 slate dates. The settled denominator represents 240 goalie starts at 13 lines each, not 3,120 independent goalie games. | Selected-side record is 66.9%. The sample is conditional on start/participation and lineup/goalie identity; non-starts and unresolved starter states must not become unders. There is not enough evidence here to call performance stable or calibrated. | Market coverage is `UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE` in the daily summaries. No ROI is supported. |

## Moneyline favorite, underdog, and market-gap readout

The audited 89-game paired dataset had 55 market-favorite wins and 34
underdog wins. V3's 20 underdog selections went 12–8; its 69 favorite
selections went 47–22. The control's 18 underdog selections went 7–11; its 71
favorite selections went 44–27. The 18 V3/control side disagreements favored
V3, 13–5, but only 8 of those 18 V3 picks agreed with the market favorite, so
the disagreement result is not solely a favorite-following effect.

Within the market-gap analysis, V3 and the no-vig market had comparable Brier
scores in the <2.5 percentage-point (n=22) and 2.5–5 point (n=18) groups.
V3's Brier was lower in the 5–10 point group (0.2027 vs 0.2081, n=31) and
10–15 point group (0.2233 vs 0.2416, n=15). The ≥15 point group had only 3
games (0.2988 vs 0.3197). This is a useful prospective question, not proof
that disagreement is exploitable; the highest-gap sample is particularly
underpowered.

## Reconciliation and evidence limits

The latest complete learning summary is for Oct 16. It grades two games for
Moneyline and Puck Line, 71 SOG player-games, 35 settled Points identities
(13 missing outcomes in the HGB grade), and 4 Saves goalie starts. The Oct 14–16
Moneyline summaries duplicate V3's identity under both role labels; only the
V3 production result is counted for those 15 games. Oct 17
prediction receipts are excluded because those outcomes were not settled at
the cutoff. Daily learning summaries are revised as source authority is
restated; the cumulative totals above use one latest revision per slate date.

The Moneyline and Puck Line priced studies use separate frozen windows and
their own retained quote contracts. SOG has partial matched quotes. Points and
Saves do not have complete bound market coverage. Therefore only the ML and
Puck Line priced subsets support descriptive ROI, each with its recorded
single-book rule and no claim of execution. The selected-side win rates across
props are not actual bet records.

Historical source/training caveats remain active: the Points Phoenix shadow is
not certified historically comparable; unresolved shot-attempt source rows
remain unknown rather than zero; no inference is made from ungraded or missing
outcomes. No historical rows or model authority were changed in this review.

## Practical raw CSV export contract for external filtering

The current SOG command-deck workflow writes raw decision ledgers for SOG,
Points, and Saves, then emits a reference-only current-slate CSV; challenger
rows are excluded from that final CSV. That is not a complete all-candidate
research export. For external filtering, retain an immutable raw package before
any filter with two related CSVs:

1. `candidate_predictions.csv`: exactly one row per eligible pregame
   prediction natural key, including every authorized production, challenger,
   and shadow arm; retain rows with no quote and rows that any later filter
   would reject.
2. `candidate_quotes.csv`: one row per prediction key and sportsbook quote;
   retain all qualified pregame quote rows and explicit no-match diagnostics in
   a companion coverage file rather than duplicating or dropping predictions.

Required shared lineage fields: `canonical_season`, `slate_date`, `game_type`,
`canonical_phase`, `run_id`, `run_type`, `scheduled_start_utc`,
`prediction_timestamp_utc`, `feature_cutoff_utc`, `prediction_identity`,
`model_role`, `model_family`, `model_version`, `feature_contract_sha256`,
`model_identity_sha256`, `prediction_artifact_sha256`, `odds_manifest_sha256`,
`market_key`, `market_line`, `selected_side`, `probability_over`,
`probability_under`, `expected_count` where applicable, and explicit
`candidate_policy_status=NOT_APPLIED`. Include stable game/player/goalie/team
IDs and the displayed name as a non-key field. Quote rows additionally require
`sportsbook_key`, provider event/market/outcome IDs, American and decimal price,
source quote timestamp, observation timestamp, quote qualification status, and
the exact prediction natural key. A quote must be strictly pregame for that
prediction's target; late or unmatched quotes cannot be substituted.

Filter results must be written as a separate derived CSV with the filter
expression/version/hash and parent raw-package manifest hash. They must not
replace, overwrite, or narrow the raw population. Do not include outcomes in a
pregame candidate file; bind them later in an append-only grade artifact.

## Bounded follow-up analyses

1. **Continue post-cutover Moneyline pairing.** Obtain at least 50 additional
   settled paired V3/V2 games under the current authority, then repeat
   favorite/underdog, market-gap, and proper-score comparisons. Predeclare gap
   bands; the present ≥15-point subgroup is only 3 games.
2. **Confirm D10/D20 SOG blend differences.** Accumulate another fixed window
   of exact paired production/25-75/50-50 rows and report date-clustered paired
   Brier, log loss, and player-game count MAE. Keep D10 production unchanged.
3. **Replace Puck Line feature-overlay iteration with margin-distribution
   diagnostics.** Both current three-class models score worse than uniform and
   worse than the market on 100 games; investigate coherent goal-difference
   probabilities and the OT/SO/empty-net label limitations before fitting or
   activation.

## Evidence index

- Latest daily summary and source hashes: `artifacts/operational/nhl/postgame_reconciliation/learning_restatements/2026-10-16/performance_summary=4f3ad7a133aa41bf8e62/performance_summary.json`.
- Moneyline paired pricing analysis: `artifacts/analysis/nhl/moneyline_prospective_performance/2026-10-13/`.
- Puck Line paired analysis: `artifacts/analysis/nhl/puck_line_framework_viability/2026-10-14/`.
- SOG paired D10/blend analysis: `artifacts/analysis/nhl/sog_fixed_blend_prospective_evaluation/2026-10-14/`.
- Points same-capture grade: `artifacts/operational/nhl/points_hgb_shadow/grades/season=2026/slate_date=2026-10-16/capture=captured_at=20261017T014336933291Z/grade=39614a4f0ff9ae47b902/grade.json`.
- SOG production/market coverage and daily grades: the latest-summary lineage above and the per-slate reconciliation packages under `artifacts/operational/nhl/postgame_reconciliation/`.
