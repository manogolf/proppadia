# MLB 2026 Moneyline phase gating V1

The active Moneyline grading boundary and the shared Moneyline report loader now
join the source-hashed canonical authority by exact gamePk. Regular-season and
postseason evaluation are disjoint. Preseason is excluded; special, missing,
unknown, stale, conflicting, and duplicate authority fails closed. No phase
column was added to a Moneyline ledger, and no prediction, outcome, price,
probability, pick, timestamp, or model was rewritten.

The retained local reconciliation proves 633 prediction rows, 630 outcome rows,
and 15,489 retained Moneyline price observations (630 gamePks) are all
authoritative regular-season games. Their regular-gate membership/content hashes
are unchanged, so no historical restatement is required. The pre-existing
aggregate metric artifact is byte-identical; its older input lineage is not an
exact-gamePk manifest and remains a documented historical limitation.

Postseason behavior is proven only with synthetic fixtures covering all six
supported round codes. Operational readiness remains blocked until a real
authoritative postseason game is retained and an ordinary prediction/outcome/
price window validates the path. Prediction quality is not claimed to improve;
only evaluation membership changes. Market/ROI calculations remain separately
labeled hypothetical and are never combined with prediction-quality metrics.

Agreement-study, cross-lane Ops Brief, general daily-index, and legacy one-off
model-development outputs were not modified. The next action is ordinary source
retention followed by a no-special-authorization postseason shadow observation
when the first real postseason game appears.
