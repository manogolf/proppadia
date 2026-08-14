# MLB Hits 0.5 two-sided probability reconstruction v1

- Contract: `SELECTED_SIDE_PROBABILITY_CONTRACT_CONFIRMED`; complement invariants pass with zero violations.
- Canonical earliest-strict-pregame board: 17,603 player-games; outcome complete 13,579; common paired BetOnline board 9,764.
- Proppadia full-board Brier 0.244277, log loss 0.682127, ECE 0.036572; BetOnline 0.241531, 0.676049, 0.022362.
- Confidence ordering: present; temporal `STABLE`. At >=15pp separation, model Brier 0.2617702693575172 vs BetOnline 0.2503635052786806.
- The selected-side framing materially conditioned the prior Over/Under lanes; the full board evaluates one coherent P(1+ hit) forecast instead of treating selected directions as independent boards.
- `HISTORICAL_MODEL_IDENTITY = UNRESOLVED`. Evidence: `HITS05_TWO_SIDED_PROBABILITY_EVIDENCE_MIXED`.
- `PROVENANCE_WORK_JUSTIFIED = YES`; `PROSPECTIVE_CAPTURE_REVIEW_JUSTIFIED = YES`. No next step was executed.
