# BULLPEN_RECENCY_FRESHNESS_INVARIANT_V1

- Completed official MLB feeds advance the relief-history supplement before subsequent totals scoring.
- Every state uses `official_date < target_date`; same-date and target-game information is excluded.
- Prediction-time cutoff, latest eligible date, last team-game date, source hashes, acquisition timestamps, and `BULLPEN_RECENCY_FRESHNESS_INVARIANT_V1` are retained.
- Current source coverage plus no qualifying relief outs is `VALID_ZERO_BURDEN`.
- A latest eligible source date older than one day is `BULLPEN_HISTORY_STALE`; burden/count are null and context scoring fails closed.
- New feature provenance is bound to the immutable prediction context. Existing prediction rows remain untouched.
