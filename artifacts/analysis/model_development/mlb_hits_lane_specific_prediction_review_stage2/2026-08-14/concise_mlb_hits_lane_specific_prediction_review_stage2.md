# MLB Hits lane-specific prediction review — Stage 2

Descriptive review only; no certification, recalibration, threshold, selector, EV/ROI, UI, or model change.

- `HITS_05_OVER`: n=5132; Brier 0.246826; log loss 0.687437; ECE 0.049858; ordering `FLAT`; temporal `STABLE`; >=15pp n=133, Brier 0.27461821508690226.
- `HITS_05_UNDER`: n=1618; Brier 0.249386; log loss 0.691875; ECE 0.046021; ordering `MONOTONIC_OR_NEAR_MONOTONIC`; temporal `DETERIORATING`; >=15pp n=356, Brier 0.253578486712323.
- `HITS_15_OVER`: n=97; Brier 0.267368; log loss 0.728212; ECE 0.229447; ordering `FLAT`; temporal `DETERIORATING`; >=15pp n=78, Brier 0.2699865338295384.
- `HITS_15_UNDER`: n=717; Brier 0.217089; log loss 0.625357; ECE 0.097994; ordering `PARTIAL_ORDERING`; temporal `STABLE`; >=15pp n=30, Brier 0.23772337791743334.

## QUESTIONS_REQUIRING_HUMAN_DELIBERATION_STAGE2
- Is Hits 0.5 high-confidence overconfidence mild enough to tolerate, or does it undermine probability trust?
- Do Hits 0.5 Over and Under differ enough to require separate treatment?
- Is Hits 1.5 Under evidence sufficiently large and persistent for lane-specific prospective capture?
- Is Hits 1.5 Over too sparse for a reliable conclusion?
- Should resolved prospective evidence be required before deliberating about any lane?
- Should any later decision be lane-specific rather than Hits-family-wide?
