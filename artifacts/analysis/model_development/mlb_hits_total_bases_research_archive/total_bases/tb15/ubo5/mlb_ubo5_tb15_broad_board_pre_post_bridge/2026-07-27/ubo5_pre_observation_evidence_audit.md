# UBO-5 pre-observation evidence audit

- Governing probability evidence: `artifacts/analysis/model_development/mlb_ubo5_total_bases_established_hitter_implementation_readiness/2026-07-23/line_1_5_evaluation.csv`
- Supported row ledger: `artifacts/analysis/model_development/mlb_ubo5_total_bases_established_hitter_implementation_readiness/2026-07-23/supported_population_manifest.csv`
- Model artifact: `artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib`
- SHA256: `505bbd44fee7ba5b4331e81692efd0da24afc1ae1e22e2081f6c65e0804d844d`
- Dates: 2026-07-02 through 2026-07-21; 16 slate dates.
- Eligible TB 1.5 rows: 974; OVER probability target; established certified historical starters; strict-prior PA >=100.
- Feature history endpoint: strict-prior per target date; training cutoff 2024-12-31.
- UBO-5: Brier 0.242976, log loss 0.679217, mean probability 0.390023, actual rate 0.406571.
- Production comparison: Brier 0.249835; paired UBO-5 improvements were 0.006859 Brier and 0.014343 log loss.
- The favorable result established probability improvement over production. It did not test positive BetOnline edge or ROI.
- The historical bridge uses exact player/game identity, confirmed lineup/order, authentic run-tagged two-sided BetOnline prices, and the last pregame snapshot where available.
