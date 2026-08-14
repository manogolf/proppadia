# MLB Hits 0.5 2026 season-to-date evidence v1

- Evidence window: `2026-03-25` through `2026-08-13`; no replay or probability reconstruction from features.
- Recoverable season: 27,167 predictions / 24,967 resolved; Brier 0.246290; log loss 0.686380; ECE 0.048995.
- Strict pregame: 20,138 / 18,319; Brier 0.243787; log loss 0.681230; ECE 0.033839; ordering `MONOTONIC`.
- Model generations represented: 7 (six dated strict-history generations plus one unresolved pre-May-8 generation; the July 9 generation continues byte-identically through August 13).
- Strict monthly Brier range: 0.243285–0.245479; calibration remains broadly stable with persistent structural upper-tail overconfidence.
- August 3–13: verified 2,682/2,483, Brier 0.244760, log loss 0.682670, ECE 0.031982; `AUGUST_CONTINUITY_CONSISTENT`.
- Old `0.244277` “full-board” reference: May 8–August 2 only, 17,603 predictions / 13,579 resolved, six Tier B generations; it was a coherent player-game binary board, not full-season evidence.
- BetOnline strict synchronized n=10,319: Proppadia/BetOnline Brier 0.244582/0.242270; log loss 0.682733/0.677578; ECE 0.036264/0.024810.
- >=15pp separation n=564: Proppadia/BetOnline Brier 0.260692/0.254322; behavior is `MIXED` because historical deterioration remains in the pooled season while the smaller August prospective cohort did not reproduce it.
- `HITS05_2026_SEASON_EVIDENCE_STABLE_WITH_KNOWN_LIMITATIONS`; `HITS05_CERTIFICATION_REVIEW_JUSTIFIED`.
- Explicit `P_UNDER_0_5` forward-lineage patch remains appropriate and unimplemented.

Human review: determine formal certification criteria and whether upper-tail overconfidence requires a separately authorized calibration study before any certification decision. No certification, recalibration, selector, EV/ROI, pipeline, or UI change occurs here.
