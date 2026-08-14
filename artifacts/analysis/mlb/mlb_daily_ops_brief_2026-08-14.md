# MLB Daily Ops Brief — 2026-08-14

- Generated (UTC): `2026-08-14T14:33:46Z`
- Completed slate date: `2026-08-13`
- Current slate date: `2026-08-14`
- Overall Status: `PASS`

## Morning Workflow Handoff

- Real Ops Brief status: existing section order is preserved; full three-phase body rewrite remains prototype-only.
- Phase 1 intent: System Readiness — can I trust today's platform?
- Operational status: `PASS`
- Review Pipeline & Ops, source health, identity, freshness, feature lineage, invariants, and critical warnings before candidate review.

- Phase 2 intent: Today's Baseball — what kind of baseball day is today?
- Continue through the baseball context sections below before opening candidate CSVs.

- Phase 3 handoff: Begin Candidate Review — transition from observation to decision.
- Open Morning Workbench: [Morning Workbench](review_aids/performance/o15_morning_workbench.md)

## Snapshot
- Pipeline: `pass` | Ops: `pass` | Legacy postgrade alerts: `0 critical / 1 warning`
- Legacy Model vs Fade (2026-08-02 to 2026-08-02, paired=1924): model ROI `-4.79%` vs fade ROI `-11.35%`
- Hits Environment: signal `normal`, starter rows `18`, slate expected rows `26`

## Active MLB Prediction Authority
- Moneyline: `MONEYLINE_STANDALONE_PREDICTION_CERTIFIED` / `MONEYLINE_PUBLIC_PREDICTION_READY`; public feature flag `absent/false in this runtime`; current frozen rows `14`; betting authority `NO_QUALIFIED_MLB_BETTING_MODEL`.
  - Prospective evidence: `65-51`; Brier `0.242112384132311`; log loss `0.677189633580637`; STRONG `24-10`.
- Totals: `TOTALS_STANDALONE_PREDICTION_VALID_WITH_LIMITATIONS`; point foundation `RAW_V1`; fair-probability foundation `V1_INTERCEPT`; display `TOTALS_PRIVATE_ONLY`; current frozen `11`, pending/fail-closed `3`.
  - Cumulative raw: MAE `3.1507580242931423`, forecast-minus-actual bias `-0.6399807254920301`, CRPS `2.25072678774423`; intercept diagnostic: MAE `3.143395970094702`, bias `-0.14643072549202954`, CRPS `2.2195465855408534`.
- Pinnacle: mapped `11`; moneyline `11`; totals `11`; run line `11`; identity rejects `0`.
- Player props: `NO_QUALIFIED_MLB_PROP_MODEL`; collection and research-shadow activity below do not confer prediction authority.

## Pipeline & Ops
- Source: source `artifacts/mlb_pipeline_history.jsonl ; artifacts/mlb_prod12_ops_history.jsonl` | source_date `2026-08-14` | expected `2026-08-14` | mtime/generated `2026-08-14T12:59:07Z` | freshness `fresh` | cadence `persistent history; updates when daily gate/ops capture runs`
- Pipeline captured: `2026-08-14T12:59:07.100550+00:00` | failures: `0`
- Pipeline checks:
  - `prediction_gate`: `pass`
  - `prediction_flow_audit`: `pass`
  - `hits_expectation_sources`: `pass`
- Degraded prop lanes:
  - `outs_recorded`: `quality_accuracy_below_threshold` (accuracy `47.1` vs min `48.0`; total `792`)
  - `strikeouts_pitching`: `quality_accuracy_below_threshold` (accuracy `47.09` vs min `48.0`; total `773`)
- Ops captured: `2026-08-14T12:59:07.417245+00:00` | status `pass`
  - `status`: `pass`
  - `health`: `pass`
  - `incident`: `pass`

## Postgrade Alerts
- Source: source `artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json` | source_date `2026-08-02` | expected `2026-08-13` | mtime/generated `2026-08-03T23:41:40Z` | freshness `inactive-by-authority` | cadence `inactive legacy prop-production reporting`
- Authority: `INACTIVE_LEGACY` — retained for historical context; not an active operational-health input.
- Report date: `2026-08-02` | alerts `1` (critical `0`, warning `1`)
- [warning] `prop_model_win_rate_drop`: Prop-level model win rate dropped across recent windows.
  - Alert state: source_date `2026-08-02`, generated_at `2026-08-14T14:33:46Z`, last_changed_at `2026-04-16T22:21:49Z`, age_days `120`, new_today `False`, persistent `True`

## Model vs Fade
- Source: source `tmp/analysis/mlb_model_vs_fade_summary.json` | source_date `2026-08-02` | expected `2026-08-13` | mtime/generated `2026-08-03T23:41:39Z` | freshness `inactive-by-authority` | cadence `inactive legacy wager reporting`
- Authority: `INACTIVE_LEGACY` — no qualified MLB betting model currently consumes this surface.
- Source window: `2026-08-02` to `2026-08-02`
- Rows CSV: `artifacts/analysis/mlb/execution_vs_model/2026-08-02/reconcile_rows.csv`
- Paired bets: `1924`
- Model: win rate `62.27%`, ROI `-4.79%`
- Fade: win rate `37.73%`, ROI `-11.35%`
- Delta (fade - model): `-6.57%` | fade_beating_model_alert `False`
- Alert state: active `False`, source_date `2026-08-02`, generated_at `2026-08-14T14:33:46Z`, last_changed_at ``, age_days `0`, new_today `False`, persistent `False`

## Prop Outlook Freshness
- Source: source `backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv` | source_date `2026-08-02` | expected `2026-08-13` | mtime/generated `2026-08-14T12:50:07Z` | freshness `inactive-by-authority` | cadence `inactive legacy prop-model outlook`
- Regime CSV: `backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv`
- Max latest_usable_date: `2026-08-02` | prop count `13` | outs_recorded present `True`
- Reporting alignment CSV: `backend/mlb/exports/reporting_alignment/reporting_alignment_2026-08-13.csv` | stale_outlook_source count `0`

## Model Performance By Prop
- Source: source `backend/mlb/exports/model_performance/prop_rolling_summary.csv ; backend/mlb/exports/model_performance/prop_daily_performance.csv` | source_date `2026-08-02` | expected `2026-08-13` | mtime/generated `2026-08-14T12:50:00Z` | freshness `inactive-by-authority` | cadence `inactive legacy prop-model grading summary`
- Rolling summary CSV: `backend/mlb/exports/model_performance/prop_rolling_summary.csv`
- Daily performance CSV: `backend/mlb/exports/model_performance/prop_daily_performance.csv`
- source_type `full_slate_model_pick` | active prop count `13` | missing_reason count `16`
- Critical props: `none`
- Watch props: `none`

## Hits Over 1.5 Watch Candidates
- Scope: review aid only; not a production rule, selector, upload filter, or threshold change.
- Source: `MISSING_INPUT: artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_2026-08-14.csv`
- Candidate definition: Quick Card candidate + hits over 1.5 + `d7_hits_per_game > 1.0` + `starter_expected_hits_allowed >= 5.0`.
- Row count: `0` | A/A `0` | A/B `0`
- Top candidates: unavailable because the current-slate watch candidate CSV is missing or unreadable.

## Hits Over 1.5 Layered Candidates
- Scope: review aid only; not a production rule, selector, upload filter, or threshold change.
- Source: `MISSING_INPUT: artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-08-14.csv`
- Counts: d7_hot `0` | d7+d15 `0` | d7+d15+starter `0` | QC watch `0`
- A/A counts: QC watch `0` | d7+d15+starter non-QC `0` | d7+d15 no-starter `0` | d7-only discovery `0`
- A/B counts: QC watch `0` | d7+d15+starter non-QC `0` | d7+d15 no-starter `0` | d7-only discovery `0`
- Top QC watch candidates: unavailable because the current-slate layered candidate CSV is missing or unreadable.
- Top non-QC d7+d15+favorable starter candidates: unavailable because the current-slate layered candidate CSV is missing or unreadable.

## Hits Under 1.5 Favorite Audit
- Scope: review aid only; not a production rule, selector, upload filter, or threshold change.
- Source: `MISSING_INPUT: artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-08-14.csv`
- Counts: all u1.5 `0` | d7 cold `0` | d7+d15 cold `0` | d7+d15+tough starter `0` | QC watch `0`
- A/A counts: QC watch `0` | d7+d15+tough starter non-QC `0` | d7+d15 no-tough-starter `0` | d7-only discovery `0`
- A/B counts: QC watch `0` | d7+d15+tough starter non-QC `0` | d7+d15 no-tough-starter `0` | d7-only discovery `0`
- Top QC watch candidates: unavailable because the current-slate u1.5 audit CSV is missing or unreadable.
- Top non-QC d7+d15+tough starter candidates: unavailable because the current-slate u1.5 audit CSV is missing or unreadable.

## Hits 1.5 Alternate Discovery
- Scope: DISCOVERY ONLY; alternate market; Over-only feed; not production scoring, uploads, or grading.
- Source: `OPTIONAL_MISSING: artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_2026-08-14.csv`
- Counts: total `0` | d7+d15 `0` | d7+d15+starter `0`
- Top alternate candidates: unavailable because the current-slate alternate discovery CSV is missing or unreadable.

## Hits O1.5 Prospective Run 1 Utility
- Scope: historical frozen Run 1 process evidence only; not a production selector, upload rule, or active candidate capture.
- Status: `BLOCKED_ROWS_REQUIRE_MANUAL_REVIEW` | automatic wrapper `removed_from_routine_wrapper` | producer active `False`
- Frozen rows `40` | graded `29` | pending `0` | blocked/manual-review `11`
- Latest frozen date `2026-07-17` | latest grading date `2026-07-17` | generated `2026-07-18T23:35:21+00:00`
- Artifacts: machine JSON: [machine JSON](../model_development/mlb_o15_market_anchored_ranking_prospective/machine_readable_prospective_grading_2026-07-17.json) | graded ledger CSV: [graded ledger CSV](../model_development/mlb_o15_market_anchored_ranking_prospective/prospective_graded_ledger_2026-07-17.csv) | prediction ledger CSV: [prediction ledger CSV](../model_development/mlb_o15_market_anchored_ranking_prospective/prospective_prediction_ledger_2026-07-17.csv)

## Review Aid Performance
- Source: source `artifacts/analysis/mlb/review_aids/performance/review_aid_performance_summary.json` | source_date `2026-08-13` | expected `2026-08-13` | mtime/generated `2026-08-14T12:52:09Z` | freshness `source_not_ready` | cadence `daily after completed-slate reconcile; review aid reporting only`
- Scope: review aid outcome tracking only; not a production rule, selector, upload filter, or threshold change.
- Status: `source_not_ready` | latest completed slate `2026-08-13` | board rows `9176` | matched `2815`
- Layer = review-aid provenance, not A/A-style hitter/starter tier.
- o1.5 Layer 4 (QC + d7/d15 + starter context): no latest completed slate rows.
- o1.5 Layer 3 (d7/d15 + starter context): no latest completed slate rows.
- o1.5 alternate Layer A (alternate d7/d15 + favorable starter, no QC): no latest completed slate rows.
- u1.5 Layer 4 (QC + d7/d15 + starter context): no latest completed slate rows.
- u1.5 Layer 3 (d7/d15 + starter context): no latest completed slate rows.
- u1.5 Layer 2 (d7/d15 form only): no latest completed slate rows.
- u1.5 A/A: no latest completed slate rows.
- Source-not-ready detail: missing completed-slate reconcile: artifacts/analysis/mlb/execution_vs_model/2026-08-13/reconcile_rows.csv

## Reconstructed Hits 1.5 All-Market Tier Audit
- Scope: reconstructed all-market research audit from execution reconcile rows; not actual generated board artifact performance and not a production rule, selector, upload filter, or threshold change.
- Latest completed slate: `2026-08-02` | source `artifacts/analysis/mlb/review_aids/hits_15_tier_backtest_summary.json`.
| board | window | tier | resolved | WR | ROI | sample |
|---|---|---:|---:|---:|---:|---|
| o1.5 | `last_7` | `B/B` | `6` | `66.67%` | `78.17%` | `small_sample_lt_10` |
| o1.5 | `last_14` | `B/A` | `21` | `57.14%` | `29.66%` | `small_sample_lt_25` |
| o1.5 | `last_14` | `B/B` | `15` | `46.67%` | `26.93%` | `small_sample_lt_25` |
| u1.5 | `last_7` | `B/C` | `8` | `87.50%` | `28.00%` | `small_sample_lt_10` |
| u1.5 | `last_7` | `B/B` | `12` | `75.00%` | `24.67%` | `small_sample_lt_25` |
| u1.5 | `last_14` | `B/B` | `22` | `77.27%` | `24.39%` | `small_sample_lt_25` |

## Path Forward
- Source: source `derived` | source_date `2026-08-14` | expected `2026-08-14` | mtime/generated `2026-08-14T14:33:46Z` | freshness `fresh` | cadence `derived each brief run`
1. Keep legacy prop quality flags informational: Legacy threshold flags remain for outs_recorded,strikeouts_pitching, but player-prop authority is NO_QUALIFIED_MLB_PROP_MODEL. Do not infer a retraining or promotion action from them. Command: `none`.
2. Keep current prediction authority unchanged: Continue certified moneyline monitoring and private totals evidence collection. Player props remain collection/research only; a read-only market monitor may be scoped separately. Command: `none`.

## BvP Impact
- Source: source `artifacts/analysis/mlb/mlb_bvp_impact_latest.json` | source_date `2026-08-03` | expected `2026-08-14 or 2026-08-13` | mtime/generated `2026-08-03T11:00:15Z` | freshness `research-refresh-failed` | cadence `before first morning slate run; may carry prior completed slate afterward`
- Label date: `2026-08-03` | requested slate date: `2026-08-03` | rows evaluated `1085` of `1085`
- Non-zero probability deltas: `865` | mean abs delta `2.76%` | max abs delta `15.74%`
- Top impacted prop type: `runs_scored` (rows `151`, mean abs delta `5.49%`)

## Hits Environment & Matchups
- Source: source `artifacts/analysis/mlb/mlb_hits_environment_latest.json` | source_date `2026-08-14` | expected `2026-08-14` | mtime/generated `2026-08-14T12:50:12Z` | freshness `fresh` | cadence `daily for current slate workspace context`
- Eval date `2026-08-13` | signal `normal` | hits/game `15.0` | z-score `-0.7012555421577297`
- Starter residuals: rows `18`, vs d7 avg `-0.2777777777777778`, vs weighted avg `-0.7479738245913876`
- Slate expected matchups: rows `26` / `27`, avg expected `4.636817797660764`, avg line-expected `0.17376817793730648`
- Full-game context (starter + bullpen proxy): rows `26` / `27`, avg bullpen add-on `3.3393298059964724`, avg team expected `7.98421706506003`
- Forecast unavailable starters: rows `1` | reasons `resolved_identity_insufficient_history=1`
### Starters present in odds without hits-allowed forecast (n=1)
- Robert Stock (NYM vs WSH): reason `resolved_identity_insufficient_history`, prior starts `2`, line `4.5`, books `3`
### Highest expected hits-allowed matchups (n=26)
- Matthew Liberatore (STL vs CHC): expected 5.87, pitcher_base 5.64, offense_factor 1.040
- Sean Newcomb (CWS vs DET): expected 5.79, pitcher_base 5.18, offense_factor 1.118
- George Kirby (SEA vs HOU): expected 5.56, pitcher_base 6.23, offense_factor 0.892
- Kyle Freeland (COL vs SF): expected 5.45, pitcher_base 6.91, offense_factor 0.789
- Brandon Pfaadt (ARI vs ATL): expected 5.14, pitcher_base 5.25, offense_factor 0.978
- Gerrit Cole (NYY vs TOR): expected 4.93, pitcher_base 5.00, offense_factor 0.986
- Chris Sale (ATL vs ARI): expected 4.86, pitcher_base 4.81, offense_factor 1.010
- Seth Lugo (KC vs LAA): expected 4.82, pitcher_base 5.53, offense_factor 0.871
- Gavin Williams (CLE vs SD): expected 4.78, pitcher_base 4.44, offense_factor 1.078
- Bubba Chandler (PIT vs BOS): expected 4.73, pitcher_base 4.41, offense_factor 1.073
- Jake Bennett (BOS vs PIT): expected 4.71, pitcher_base 4.92, offense_factor 0.957
- Andrew Alvarez (WSH vs NYM): expected 4.69, pitcher_base 4.44, offense_factor 1.056
- Landen Roupp (SF vs COL): expected 4.68, pitcher_base 4.76, offense_factor 0.983
- Kumar Rocker (TEX vs OAK): expected 4.67, pitcher_base 5.16, offense_factor 0.905
- Sandy Alcantara (MIA vs CIN): expected 4.53, pitcher_base 5.92, offense_factor 0.766
- Clay Holmes (CHC vs STL): expected 4.51, pitcher_base 4.62, offense_factor 0.976
- Robert Gasser (MIL vs LAD): expected 4.51, pitcher_base 4.92, offense_factor 0.915
- Grayson Rodriguez (LAA vs KC): expected 4.51, pitcher_base 5.33, offense_factor 0.845
- Shane Bieber (TOR vs NYY): expected 4.36, pitcher_base 5.33, offense_factor 0.817
- Michael King (SD vs CLE): expected 4.26, pitcher_base 4.30, offense_factor 0.992
- Chase Burns (CIN vs MIA): expected 4.16, pitcher_base 4.27, offense_factor 0.973
- Jackson Jobe (DET vs CWS): expected 4.09, pitcher_base 4.15, offense_factor 0.986
- Gage Jump (OAK vs TEX): expected 4.07, pitcher_base 5.21, offense_factor 0.780
- Steven Matz (TB vs BAL): expected 3.82, pitcher_base 4.23, offense_factor 0.903
- Yoshinobu Yamamoto (LAD vs MIL): expected 3.74, pitcher_base 4.20, offense_factor 0.890
- Peter Lambert (HOU vs SEA): expected 3.31, pitcher_base 4.35, offense_factor 0.760
### Lowest expected hits-allowed matchups (n=26)
- Peter Lambert (HOU vs SEA): expected 3.31, pitcher_base 4.35, offense_factor 0.760
- Yoshinobu Yamamoto (LAD vs MIL): expected 3.74, pitcher_base 4.20, offense_factor 0.890
- Steven Matz (TB vs BAL): expected 3.82, pitcher_base 4.23, offense_factor 0.903
- Gage Jump (OAK vs TEX): expected 4.07, pitcher_base 5.21, offense_factor 0.780
- Jackson Jobe (DET vs CWS): expected 4.09, pitcher_base 4.15, offense_factor 0.986
- Chase Burns (CIN vs MIA): expected 4.16, pitcher_base 4.27, offense_factor 0.973
- Michael King (SD vs CLE): expected 4.26, pitcher_base 4.30, offense_factor 0.992
- Shane Bieber (TOR vs NYY): expected 4.36, pitcher_base 5.33, offense_factor 0.817
- Grayson Rodriguez (LAA vs KC): expected 4.51, pitcher_base 5.33, offense_factor 0.845
- Robert Gasser (MIL vs LAD): expected 4.51, pitcher_base 4.92, offense_factor 0.915
- Clay Holmes (CHC vs STL): expected 4.51, pitcher_base 4.62, offense_factor 0.976
- Sandy Alcantara (MIA vs CIN): expected 4.53, pitcher_base 5.92, offense_factor 0.766
- Kumar Rocker (TEX vs OAK): expected 4.67, pitcher_base 5.16, offense_factor 0.905
- Landen Roupp (SF vs COL): expected 4.68, pitcher_base 4.76, offense_factor 0.983
- Andrew Alvarez (WSH vs NYM): expected 4.69, pitcher_base 4.44, offense_factor 1.056
- Jake Bennett (BOS vs PIT): expected 4.71, pitcher_base 4.92, offense_factor 0.957
- Bubba Chandler (PIT vs BOS): expected 4.73, pitcher_base 4.41, offense_factor 1.073
- Gavin Williams (CLE vs SD): expected 4.78, pitcher_base 4.44, offense_factor 1.078
- Seth Lugo (KC vs LAA): expected 4.82, pitcher_base 5.53, offense_factor 0.871
- Chris Sale (ATL vs ARI): expected 4.86, pitcher_base 4.81, offense_factor 1.010
- Gerrit Cole (NYY vs TOR): expected 4.93, pitcher_base 5.00, offense_factor 0.986
- Brandon Pfaadt (ARI vs ATL): expected 5.14, pitcher_base 5.25, offense_factor 0.978
- Kyle Freeland (COL vs SF): expected 5.45, pitcher_base 6.91, offense_factor 0.789
- George Kirby (SEA vs HOU): expected 5.56, pitcher_base 6.23, offense_factor 0.892
- Sean Newcomb (CWS vs DET): expected 5.79, pitcher_base 5.18, offense_factor 1.118
- Matthew Liberatore (STL vs CHC): expected 5.87, pitcher_base 5.64, offense_factor 1.040
### Highest expected team hits allowed (starter + bullpen) (n=26)
- Sean Newcomb (CWS vs DET): team_expected 10.01 = starter_expected 5.79 + bullpen_add_on 4.22
- George Kirby (SEA vs HOU): team_expected 9.55 = starter_expected 5.56 + bullpen_add_on 3.99
- Kyle Freeland (COL vs SF): team_expected 9.10 = starter_expected 5.45 + bullpen_add_on 3.64
- Grayson Rodriguez (LAA vs KC): team_expected 9.06 = starter_expected 4.51 + bullpen_add_on 4.55
- Seth Lugo (KC vs LAA): team_expected 8.93 = starter_expected 4.82 + bullpen_add_on 4.11
- Kumar Rocker (TEX vs OAK): team_expected 8.79 = starter_expected 4.67 + bullpen_add_on 4.11
- Landen Roupp (SF vs COL): team_expected 8.77 = starter_expected 4.68 + bullpen_add_on 4.08
- Gage Jump (OAK vs TEX): team_expected 8.71 = starter_expected 4.07 + bullpen_add_on 4.64
- Shane Bieber (TOR vs NYY): team_expected 8.23 = starter_expected 4.36 + bullpen_add_on 3.87
- Brandon Pfaadt (ARI vs ATL): team_expected 8.19 = starter_expected 5.14 + bullpen_add_on 3.06
- Sandy Alcantara (MIA vs CIN): team_expected 8.13 = starter_expected 4.53 + bullpen_add_on 3.59
- Matthew Liberatore (STL vs CHC): team_expected 8.12 = starter_expected 5.87 + bullpen_add_on 2.25
- Bubba Chandler (PIT vs BOS): team_expected 8.11 = starter_expected 4.73 + bullpen_add_on 3.38
- Andrew Alvarez (WSH vs NYM): team_expected 8.07 = starter_expected 4.69 + bullpen_add_on 3.38
- Gavin Williams (CLE vs SD): team_expected 8.05 = starter_expected 4.78 + bullpen_add_on 3.27
- Chris Sale (ATL vs ARI): team_expected 7.76 = starter_expected 4.86 + bullpen_add_on 2.90
- Chase Burns (CIN vs MIA): team_expected 7.69 = starter_expected 4.16 + bullpen_add_on 3.53
- Clay Holmes (CHC vs STL): team_expected 7.51 = starter_expected 4.51 + bullpen_add_on 3.00
- Gerrit Cole (NYY vs TOR): team_expected 7.32 = starter_expected 4.93 + bullpen_add_on 2.39
- Jake Bennett (BOS vs PIT): team_expected 7.30 = starter_expected 4.71 + bullpen_add_on 2.59
- Michael King (SD vs CLE): team_expected 7.17 = starter_expected 4.26 + bullpen_add_on 2.90
- Robert Gasser (MIL vs LAD): team_expected 7.11 = starter_expected 4.51 + bullpen_add_on 2.60
- Yoshinobu Yamamoto (LAD vs MIL): team_expected 6.84 = starter_expected 3.74 + bullpen_add_on 3.11
- Jackson Jobe (DET vs CWS): team_expected 6.55 = starter_expected 4.09 + bullpen_add_on 2.46
- Steven Matz (TB vs BAL): team_expected 6.54 = starter_expected 3.82 + bullpen_add_on 2.72
- Peter Lambert (HOU vs SEA): team_expected 5.99 = starter_expected 3.31 + bullpen_add_on 2.68
### Lowest expected team hits allowed (starter + bullpen) (n=26)
- Peter Lambert (HOU vs SEA): team_expected 5.99 = starter_expected 3.31 + bullpen_add_on 2.68
- Steven Matz (TB vs BAL): team_expected 6.54 = starter_expected 3.82 + bullpen_add_on 2.72
- Jackson Jobe (DET vs CWS): team_expected 6.55 = starter_expected 4.09 + bullpen_add_on 2.46
- Yoshinobu Yamamoto (LAD vs MIL): team_expected 6.84 = starter_expected 3.74 + bullpen_add_on 3.11
- Robert Gasser (MIL vs LAD): team_expected 7.11 = starter_expected 4.51 + bullpen_add_on 2.60
- Michael King (SD vs CLE): team_expected 7.17 = starter_expected 4.26 + bullpen_add_on 2.90
- Jake Bennett (BOS vs PIT): team_expected 7.30 = starter_expected 4.71 + bullpen_add_on 2.59
- Gerrit Cole (NYY vs TOR): team_expected 7.32 = starter_expected 4.93 + bullpen_add_on 2.39
- Clay Holmes (CHC vs STL): team_expected 7.51 = starter_expected 4.51 + bullpen_add_on 3.00
- Chase Burns (CIN vs MIA): team_expected 7.69 = starter_expected 4.16 + bullpen_add_on 3.53
- Chris Sale (ATL vs ARI): team_expected 7.76 = starter_expected 4.86 + bullpen_add_on 2.90
- Gavin Williams (CLE vs SD): team_expected 8.05 = starter_expected 4.78 + bullpen_add_on 3.27
- Andrew Alvarez (WSH vs NYM): team_expected 8.07 = starter_expected 4.69 + bullpen_add_on 3.38
- Bubba Chandler (PIT vs BOS): team_expected 8.11 = starter_expected 4.73 + bullpen_add_on 3.38
- Matthew Liberatore (STL vs CHC): team_expected 8.12 = starter_expected 5.87 + bullpen_add_on 2.25
- Sandy Alcantara (MIA vs CIN): team_expected 8.13 = starter_expected 4.53 + bullpen_add_on 3.59
- Brandon Pfaadt (ARI vs ATL): team_expected 8.19 = starter_expected 5.14 + bullpen_add_on 3.06
- Shane Bieber (TOR vs NYY): team_expected 8.23 = starter_expected 4.36 + bullpen_add_on 3.87
- Gage Jump (OAK vs TEX): team_expected 8.71 = starter_expected 4.07 + bullpen_add_on 4.64
- Landen Roupp (SF vs COL): team_expected 8.77 = starter_expected 4.68 + bullpen_add_on 4.08
- Kumar Rocker (TEX vs OAK): team_expected 8.79 = starter_expected 4.67 + bullpen_add_on 4.11
- Seth Lugo (KC vs LAA): team_expected 8.93 = starter_expected 4.82 + bullpen_add_on 4.11
- Grayson Rodriguez (LAA vs KC): team_expected 9.06 = starter_expected 4.51 + bullpen_add_on 4.55
- Kyle Freeland (COL vs SF): team_expected 9.10 = starter_expected 5.45 + bullpen_add_on 3.64
- George Kirby (SEA vs HOU): team_expected 9.55 = starter_expected 5.56 + bullpen_add_on 3.99
- Sean Newcomb (CWS vs DET): team_expected 10.01 = starter_expected 5.79 + bullpen_add_on 4.22
- Team-level expected vs actual eval (context as-of `2026-08-12`): rows `18` / `18`, coverage `100.0`, expected avg `7.905848388197648`, actual avg `7.5`, residual avg `-0.40584838819764807`, MAE `3.961470316334148`, RMSE `4.7738355072405705`
### Biggest over-expected misses (n=5)
- Tyler Phillips (MIA vs PIT): expected_team 6.75, actual 16.00, residual +9.25
- Jacob deGrom (TEX vs LAA): expected_team 7.14, actual 15.00, residual +7.86
- Taj Bradley (MIN vs PHI): expected_team 7.36, actual 12.00, residual +4.64
- Andrew Abbott (CIN vs CWS): expected_team 7.42, actual 12.00, residual +4.58
- Roki Sasaki (LAD vs MIL): expected_team 6.88, actual 10.00, residual +3.12
### Biggest under-expected misses (n=5)
- Cade Cavalli (WSH vs CHC): expected_team 9.65, actual 1.00, residual -8.65
- Payton Tolle (BOS vs TOR): expected_team 8.48, actual 2.00, residual -6.48
- Braxton Ashcraft (PIT vs MIA): expected_team 8.45, actual 3.00, residual -5.45
- Shane Drohan (MIL vs LAD): expected_team 8.27, actual 4.00, residual -4.27
- Logan Gilbert (SEA vs NYY): expected_team 8.05, actual 4.00, residual -4.05

## Freshness Audit
| section | source_date | expected_date | generated_at / mtime | freshness_status | note |
|---|---:|---:|---|---|---|
| Ops Brief Input Refresh | `2026-08-13` | `2026-08-13` | `2026-08-14T12:52:09Z` | `partial-current-authority` | each brief run before report generation. dependency_missing_count=0; refresh_failed_count=3; stale_after_refresh_count=2; reconcile_rows_csv=artifacts/analysis/mlb/execution_vs_model/2026-08-13/reconcile_rows.csv; reconcile_rows_exists=False |
| Certified Moneyline Lifecycle | `2026-08-14` | `2026-08-14` | `2026-08-14T12:30:47Z` | `fresh` | each governed daily refresh invocation. rows=14; status=moneyline lifecycle artifact |
| Private Totals Lifecycle | `2026-08-14` | `2026-08-14` | `2026-08-14T12:49:15Z` | `fresh` | each governed daily refresh invocation. rows=11; status=TOTALS_SHADOW_DAILY_LIFECYCLE_COMPLETE |
| Pinnacle Main-Market Capture | `2026-08-14` | `2026-08-14` | `2026-08-14T12:48:33Z` | `fresh` | each governed daily refresh invocation. mapped=11; moneyline=11; totals=11; run_line=11 |
| Pipeline & Ops | `2026-08-14` | `2026-08-14` | `2026-08-14T12:59:07Z` | `fresh` | persistent history; updates when daily gate/ops capture runs. Latest captured gate state carries forward until a new gate capture is written. |
| Postgrade Alerts | `2026-08-02` | `2026-08-13` | `2026-08-03T23:41:40Z` | `inactive-by-authority` | inactive legacy prop-production reporting. Historical only; NO_QUALIFIED_MLB_PROP_MODEL means this source is not required for active operations. |
| Model vs Fade | `2026-08-02` | `2026-08-13` | `2026-08-03T23:41:39Z` | `inactive-by-authority` | inactive legacy wager reporting. Historical only; no qualified MLB betting model or active prop wager lifecycle. |
| Prop Outlook Freshness | `2026-08-02` | `2026-08-13` | `2026-08-14T12:50:07Z` | `inactive-by-authority` | inactive legacy prop-model outlook. Not required under NO_QUALIFIED_MLB_PROP_MODEL. |
| Model Performance By Prop | `2026-08-02` | `2026-08-13` | `2026-08-14T12:50:00Z` | `inactive-by-authority` | inactive legacy prop-model grading summary. Historical performance retained; not an active model-authority input. |
| Path Forward | `2026-08-14` | `2026-08-14` | `2026-08-14T14:33:46Z` | `fresh` | derived each brief run. Recommendations are generated from the loaded section states. |
| BvP Impact | `2026-08-03` | `2026-08-14 or 2026-08-13` | `2026-08-03T11:00:15Z` | `research-refresh-failed` | before first morning slate run; may carry prior completed slate afterward. BvP prewarm producer attempted date(s) 2026-08-14,2026-08-13 but failed before impact refresh: DNS/name resolution failure contacting statsapi.mlb.com. logs: artifacts/ops/mlb_bvp_prewarm_daily.out.log; artifacts/ops/mlb_bvp_prewarm_daily.err.log |
| Hits Environment & Matchups | `2026-08-14` | `2026-08-14` | `2026-08-14T12:50:12Z` | `fresh` | daily for current slate workspace context. Requested as-of 2026-08-14; league evaluation date 2026-08-13; team eval context as-of 2026-08-12. |
| Hits Over 1.5 Watch Candidates | `n/a` | `2026-08-14` | `n/a` | `not-required-current-authority` | daily current-slate upload prep before Ops Brief render. MISSING_INPUT: artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_2026-08-14.csv |
| Hits Over 1.5 Layered Candidates | `n/a` | `2026-08-14` | `n/a` | `not-required-current-authority` | daily current-slate upload prep before Ops Brief render. MISSING_INPUT: artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-08-14.csv |
| Hits Under 1.5 Favorite Audit | `n/a` | `2026-08-14` | `n/a` | `not-required-current-authority` | daily current-slate upload prep before Ops Brief render. MISSING_INPUT: artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-08-14.csv |
| Hits 1.5 Alternate Discovery | `n/a` | `2026-08-14` | `n/a` | `optional-missing` | daily current-slate upload prep before Ops Brief render. OPTIONAL_MISSING: artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_2026-08-14.csv |
| Review Aid Performance | `2026-08-13` | `2026-08-13` | `2026-08-14T12:52:08.995324+00:00` | `source_not_ready` | daily after completed-slate reconcile; review aid reporting only. status=source_not_ready; board_rows=9176; matched=2815 |
| Total Bases Shadow Candidate | `n/a` | `2026-08-14` | `n/a` | `stale-research` | daily current-slate shadow scoring; analysis-only. rows=0; side_changed=0; production_outputs_changed=False |
| Total Bases Shadow Evaluation | `2026-07-23` | `2026-08-14` | `2026-07-23T18:09:55Z` | `stale-research` | daily cumulative read-only evaluation after shadow scoring. rows_scored=2978; rows_with_outcomes=1940; side_changed=491 |
| Feature Lineage Health | `2026-08-14` | `2026-08-14` | `2026-08-14T14:33:45Z` | `fresh` | daily after current-slate selector/upload diagnostics are written. status=pass; pass=1; warn=0; fail=0; bvp_payload_artifacts=1; bvp_missing_required=0 |
| Source Health | `2026-08-13` | `2026-08-13` | `2026-08-14T12:50:01Z` | `fresh` | daily after reporting alignment audit. Expected after reporting alignment audit for the completed slate. |
| Legacy Player-Prop Workspace Staging | `2026-08-14` | `2026-08-14` | `2026-08-14T14:33:46Z` | `inactive-by-authority` | inactive legacy player-prop staging service. not refreshed in final render |

## Source Health
- Source: source `backend/mlb/exports/reporting_alignment/reporting_alignment_2026-08-13.csv` | source_date `2026-08-13` | expected `2026-08-13` | mtime/generated `2026-08-14T12:50:01Z` | freshness `fresh` | cadence `daily after reporting alignment audit`
- postgrade_alerts_json: `ok`
- model_vs_fade_json: `ok`
- prop_regime_csv: `ok`
- model_performance_summary_csv: `ok`
- model_performance_daily_csv: `ok`
- reporting_alignment_csv: `ok`
- bvp_impact_json: `ok`
- hits_environment_json: `ok`
- hits_o15_watch_candidates_csv: `missing`
- hits_o15_layered_candidates_csv: `missing`
- hits_u15_favorite_audit_csv: `missing`
- hits_o15_alternate_discovery_csv: `missing`
- hits_15_tier_backtest_json: `ok`
- review_aid_performance_json: `ok`
- total_bases_shadow_summary_json: `missing`
- total_bases_shadow_evaluation_json: `ok`
- feature_lineage_health_json: `ok`
- input_refresh_status_json: `ok`
- pipeline_history_jsonl: `ok`
- ops_history_jsonl: `ok`
- betonline_capture_integrity_json: `ok`
- hits05_full_spine: `ok`
- o15_prospective_status: `ok`
- today_workspace: `not_refreshed`
- Feature lineage health: status `pass` | slate_date `2026-08-14` | pass `1` warn `0` fail `0` | BvP payload artifacts `1` | BvP missing required `0`
  - BvP compact payload rates: slate_output=94.8%
  - lane_selector_output: `skip` rows `0` path `backend/mlb/exports/model_v2/lanes/today/2026-08-14/hits_lane_selector_2026-08-14.csv` issues `expected_unavailable:NO_QUALIFIED_MLB_MODEL`
  - ranking_upload_input: `skip` rows `0` path `backend/mlb/exports/model_v2/lanes/today/2026-08-14/hits_lane_selector_2026-08-14_ranking_upload_input.csv` issues `expected_unavailable:NO_QUALIFIED_MLB_MODEL`
  - quick_card_output: `skip` rows `0` path `backend/mlb/exports/model_v2/lanes/today/2026-08-14/quick_card_hits_2026-08-14.csv` issues `expected_unavailable:NO_QUALIFIED_MLB_MODEL`
  - ranking_upload_diagnostics: `skip` rows `0` path `backend/mlb/exports/model_v2/upload/2026-08-14/ranking_tool_upload_diagnostics_2026-08-14.csv` issues `expected_unavailable:NO_QUALIFIED_MLB_MODEL`
  - quick_card_upload_diagnostics: `skip` rows `0` path `backend/mlb/exports/model_v2/upload/2026-08-14/quick_card_tool_upload_diagnostics_2026-08-14.csv` issues `expected_unavailable:NO_QUALIFIED_MLB_MODEL`

## Legacy Player-Prop Workspace Staging
- Source: source `backend.app.services.mlb.today_workspace_service.fetch_today_workspace` | source_date `2026-08-14` | expected `2026-08-14` | mtime/generated `2026-08-14T14:33:46Z` | freshness `inactive-by-authority` | cadence `inactive legacy player-prop staging service`
- This is the old player-prop staging service, not the certified moneyline panel mounted on `/mlb/today`.
requested_slate_date: 2026-08-14
active_slate_date: None
row_count: 0
last_updated: None

Status: NOT_REFRESHED
Reason: not refreshed in final render
attempted_function: None
attempted_url: None
exception_type: n/a
exception_message: n/a
failure_classification: not refreshed in final render
retry_attempted: False
retry_succeeded: False

## Hits 0.5 Full-Spine Replacement
- Status: `ACTIVE_PARTIAL_COVERAGE` | model `4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b`
- Latest parent run tag: `local_daily_20260814T123002Z` | generated `2026-08-14T12:48:24Z`
- Parent rows `0` | scored rows `0` | withheld rows `1`
- Parent state: `PARENT_ARTIFACT_ZERO_VALID_NO_LINEUPS` | date contract `PARENT_ARTIFACT_DATE_CONTRACT_PASS`
- Hits 0.5 market rows `128` | candidate-routed `93` | fallback `35` | coverage `72.66%`
- Games with candidate coverage `6` | dominant fallback `NOT_ELIGIBLE`
- Lineup/starter coverage: `OFFICIAL_LINEUP_NOT_YET_POSTED=25, STARTER_UNRESOLVED=3`
- Rollback: `MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT=0 available`
- Parent scores CSV: [Parent scores CSV](../model_development/mlb_hits05_current_nonmarket_parent_producer/2026-08-14/local_daily_20260814T123002Z/hits05_scored_current_rows_2026-08-14.csv)
- Withheld ledger CSV: [Withheld ledger CSV](../model_development/mlb_hits05_current_nonmarket_parent_producer/2026-08-14/local_daily_20260814T123002Z/hits05_withheld_ledger_2026-08-14.csv)
- Slate output CSV: [Slate output CSV](../../../backend/mlb/data/processed/mlb_slate_output.csv)

| window | PT | ET | UTC | purpose | parent generated | model status | rollback |
|---|---:|---:|---:|---|---|---|---|
| `0530_pt` | 05:30 | 08:30 | 12:30 | early projected slate | yes | `ACTIVE_PARTIAL_COVERAGE` | available |
| `0830_pt` | 08:30 | 11:30 | 15:30 | morning refresh | yes | `ACTIVE_PARTIAL_COVERAGE` | available |
| `1100_pt` | 11:00 | 14:00 | 18:00 | lineup-development refresh | yes | `ACTIVE_PARTIAL_COVERAGE` | available |
| `1300_pt` | 13:00 | 16:00 | 20:00 | afternoon confirmed-lineup refresh | yes | `ACTIVE_PARTIAL_COVERAGE` | available |
| `1630_pt` | 16:30 | 19:30 | 23:30 | late-game refresh | yes | `ACTIVE_PARTIAL_COVERAGE` | available |

## BetOnline Player-Prop Capture Integrity
- Daily semantic summary JSON: [Daily semantic summary JSON](betonline_capture_integrity/2026-08-14/betonline_capture_integrity_daily_summary_2026-08-14.json)
- Daily classification: `HEALTHY` | generated `2026-08-14T14:30:59.577862Z`
- Expected windows `5` | executed `1` | missing eligible `0`
- Scheduler `com.proppadia.mlb.refresh.daily` | stdout `artifacts/ops/mlb_refresh_daily.out.log` | stderr `artifacts/ops/mlb_refresh_daily.err.log` | last successful run `local_daily_20260814T123002Z`
- Latest direct BetOnline player-prop capture: `2026-08-14T12:33:00.366761+00:00`
- Current outage status: `DIRECT_BETONLINE_PLAYER_PROP_ROWS_PRESENT` | execution authorization `AUTHORIZED_DIRECT_BETONLINE_ROWS_PRESENT`

| window PT | expected UTC | executed | semantic status | BetOnline rows | FanDuel rows | missing/partial markets | execution |
|---|---|---:|---|---:|---:|---|---|
| 05:30 | 2026-08-14T12:30:00Z | True | `BETONLINE_CAPTURE_SEMANTIC_PASS` | 1002 | 272 | stolen_bases | `AUTHORIZED_DIRECT_BETONLINE_ROWS_PRESENT` |
| 08:30 | 2026-08-14T15:30:00Z | False | `PENDING_FUTURE_WINDOW` | 0 | 0 | none | `n/a` |
| 11:00 | 2026-08-14T18:00:00Z | False | `PENDING_FUTURE_WINDOW` | 0 | 0 | none | `n/a` |
| 13:00 | 2026-08-14T20:00:00Z | False | `PENDING_FUTURE_WINDOW` | 0 | 0 | none | `n/a` |
| 16:30 | 2026-08-14T23:30:00Z | False | `PENDING_FUTURE_WINDOW` | 0 | 0 | none | `n/a` |

- Guardrail: missing direct BetOnline prices are `NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE`; FanDuel line-only proxy remains disabled and non-executable.

## HITS 0.5 EXPECTED-PA RESEARCH SHADOW — NO PRODUCTION OR WAGER EFFECT

- Current capture: 5/5 window paths reported; model contract `14ef8cc3069dccf85920c10ea557919e6113ed801b2868b02c19d01031c1b737`.
- Prior-slate grading: 0 resolved, 126 unresolved.
- Pilot progress: bounded review not complete; status `PROCESS_VALIDATED_OUTCOME_SAMPLE_EARLY`.
