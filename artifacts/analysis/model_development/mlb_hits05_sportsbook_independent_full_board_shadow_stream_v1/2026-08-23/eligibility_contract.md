# Frozen eligibility contract

- Grain: one `(Eastern slate date, official MLB gamePk, MLB player ID, hits, 0.5, exact model ID)` identity. Doubleheaders remain distinct by `gamePk`; game number and doubleheader state are preserved.
- Hitter: must appear in the official governed `CONFIRMED_LINEUP` capture for that exact game. Projected, absent, duplicate, unresolved, or cross-game player identities fail closed.
- Starter: exact opposing probable/official starter ID must be present in the same governed pregame capture. No minimum starter-history depth is imposed; the artifact's governed missing-value behavior remains unchanged.
- PA/history: no minimum prior hitter PA or game count. Missing model inputs use only the frozen runtime's registered zero/missing-indicator behavior. The hitter-shrunk evaluation baseline uses only games before the slate date with PA greater than zero.
- Inputs: the exact 73 registered feature columns must be vectorizable by the frozen runtime. Feature preparation or scoring failure excludes the row visibly and leaves it retryable while pregame.
- Timing: lineup source timestamp and score/capture timestamp must each be strictly earlier than scheduled start. The official schedule must be pregame. A row first encountered after start is `PREGAME_CUTOFF_FAILED`/fail closed and is never reconstructed.
- Postponement: a non-pregame official status is excluded. A future run may score only if the official source supplies a still-pregame rescheduled start and every timing condition passes.
- No appearance: the prediction remains in the denominator but is reported as `NO_APPEARANCE_UNRESOLVED`, excluded from appearance-resolved proper scores, and never converted to a loss or zero-hit outcome.
- Market: no market observation is required. Market rows attach only to an already-frozen canonical prediction and never create one.
- Outcome: scoring records `outcomes_accessed=0`; grading waits for exact official player-stat completeness and uses a separate immutable outcome table.
