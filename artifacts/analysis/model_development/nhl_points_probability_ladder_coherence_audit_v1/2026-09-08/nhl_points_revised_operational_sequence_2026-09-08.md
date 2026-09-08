# Revised NHL Operational Sequence

1. **Next:** `NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1_WITH_EXPLICIT_LADDER_COHERENCE_GATE`.
   - Bind the existing three model hashes without refitting.
   - Preserve raw probabilities and model-specific identity.
   - Require one unique 0.5/1.5/2.5 row per player-game.
   - Exclude the entire ladder from candidate/upload/execution eligibility when raw maximum adjacent violation is at least 0.01.
   - Retain excluded ladders as observation-only records with exact reason and magnitude.
   - Do not repair, sort, clip, or recalibrate probabilities.
2. Exercise that immutable Points path with fixtures, then one bounded preseason observation if fixtures pass. Preseason remains non-evaluation and non-actionable.
3. After prospective evidence exists, decide separately whether sub-1-point crossings require a bounded reconciliation study. No study is required to run the gated observation path.
4. Keep Goalie Saves `NOT_READY_FOR_PRESEASON_BURN_IN`. A separately authorized, timestamp-certifiable projected/confirmed starter source remains a prerequisite; this audit performs no goalie work.
5. Defer recommendation, upload, execution, learned reconciliation, calibration, retraining, and challenger research.
