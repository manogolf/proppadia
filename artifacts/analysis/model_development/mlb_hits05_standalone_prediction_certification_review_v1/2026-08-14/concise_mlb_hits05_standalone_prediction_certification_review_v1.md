# MLB Hits 0.5 standalone prediction certification review v1

- Model: `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb` / `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`; standalone baseball LR/RF blend, 73 features, no market input or market calibration.
- Governing evidence: 20,138 strict predictions / 18,319 resolved; Brier 0.243787; log loss 0.681230; ECE 0.033839.
- `SAMPLE_EVIDENCE = SUFFICIENT`; `TEMPORAL_STABILITY = PASS`; `GENERATION_STABILITY = PASS`; `PROSPECTIVE_CONTINUITY = PASS`; `CONFIDENCE_ORDERING = PASS`.
- Calibration: `CALIBRATION_ACCEPTABLE_WITH_KNOWN_UPPER_TAIL_LIMITATION`; >=75% n=507, predicted 77.540%, observed 63.708%.
- BetOnline synchronized n=10,319: Proppadia/BetOnline Brier 0.244582/0.242270, log loss 0.682733/0.677578, ECE 0.036264/0.024810. BetOnline is modestly better overall.
- Probability relationship: Pearson 0.442959; Spearman 0.435065; mean/median absolute separation 6.108%/5.189%; >=5pp 5,323 (51.58%); >=10pp 1,976 (19.15%).
- Unique correctness: Proppadia-only 897; BetOnline-only 852; both correct 5,088; both wrong 3,482.
- Error correlations: residual 0.988220; squared 0.767101; absolute 0.766251.
- Incremental information: `EVIDENCE_PRESENT` under leave-one-month-out diagnostic fitting. No combined model was retained or promoted.
- `HITS05_MEANINGFULLY_INDEPENDENT_PREDICTION_OPINION`. This is independent opinion, not proven edge.
- `REPRODUCIBILITY = PASS_WITH_SMALL_PROVENANCE_PATCH`.
- `HITS05_STANDALONE_PREDICTION_CERTIFIED_WITH_LIMITATIONS`; `HITS05_PUBLIC_PREDICTION_READY`.
- `PERSIST_EXPLICIT_P_UNDER_0_5 = YES`; not implemented.

Human decision: authorize or decline a separately scoped public-product implementation with required limitation/no-betting-edge disclosures. No recalibration, selector, EV/ROI, combined model, production, or UI change occurs here.
