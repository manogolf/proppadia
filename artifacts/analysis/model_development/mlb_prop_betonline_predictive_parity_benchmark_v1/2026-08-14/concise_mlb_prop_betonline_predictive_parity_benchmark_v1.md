# MLB Prop BetOnline Predictive Parity Benchmark v1

Read-only exact-line, exact-side, provider-specific comparison. No EV, ROI, retraining, recalibration, selection, or deployment.

- Hits: n=7564; model Brier 0.244818 vs BetOnline 0.239073 (delta +0.005745); log loss delta +0.012098; ECE delta +0.002928.
- Hits+Runs+RBIs: n=152; model Brier 0.246260 vs BetOnline 0.248514 (delta -0.002254); log loss delta -0.005035; ECE delta +0.003478.
- Pitcher Strikeouts: n=80; model Brier 0.254831 vs BetOnline 0.244073 (delta +0.010759); log loss delta +0.024111; ECE delta +0.134616.
- Total Bases: n=6114; model Brier 0.253857 vs BetOnline 0.245634 (delta +0.008223); log loss delta +0.017748; ECE delta +0.040117.

- Hits 0.5: `HITS05_PREDICTIVE_PARITY_MIXED`.
- Total Bases: `TOTAL_BASES_PREDICTIVE_PARITY_COMPARABLE`.
- Pitcher strikeouts: `PITCHER_K_PREDICTIVE_PARITY_INSUFFICIENT`.
- Hits+Runs+RBIs: `INSUFFICIENT_SYNCHRONIZED_EVIDENCE`.
- UI: `PROP_UI_MARKET_MONITOR_ONLY_REMAINS_CORRECT`.
- Betting authority remains `NO_QUALIFIED_MLB_PROP_MODEL`.
