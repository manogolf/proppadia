# Implementation and validation report

- Earliest replayable point: governed official lineup/starter capture in the current nonmarket parent producer, after predictions-wide has preserved its separate market lineage and before slate-output/public routing. The exact Hits artifact is scored independently of the parent model and sportsbook population.
- First prospective slate: 2026-08-24. No prior date is admitted as prospective evidence.
- Persistence: separate append-only SQLite prediction, feature, eligibility, rank, market, outcome, and run ledgers. All mutable operations are rejected by database triggers.
- Daily integration: one nonblocking hook in the existing installed MLB daily wrapper immediately after current nonmarket parent completion; no new scheduler and no additional provider request.
- Scoring: exact model/hash verified at runtime, exact 73-column order verified, no market-named registered features, zero outcome access, strict start-time checks, and immutable player-game identities.
- Grading: independent canonical official-stat lookup only after the existing exact player-stat completeness artifact passes. No-appearance identities remain unresolved.
- Market capture: append-only attachment from existing certified lineage to an existing frozen prediction. Market presence cannot add or remove predictions.
- Process validation: a synthetic far-future pre-start fixture is scored through the exact artifact into a temporary ledger and explicitly labeled `PROCESS_ONLY_REPLAY`; it is never retained in the prospective ledger or evidence report.
- Tests cover exact binding/market independence, deterministic scoring, process-only evidence exclusion, post-start rejection, prediction/outcome immutability, separate market attachment, and deterministic ledger validation.

Validation result: 7 focused tests passed. The governed pre-start fixture appended 1 prediction and 1 feature-context row to a temporary ledger, with 0 outcomes, 0 markets, 0 duplicate identities, `outcomes_accessed=0`, and `PASS_PROCESS_ONLY_NOT_PROSPECTIVE_EVIDENCE`. All 16 deterministic validator checks passed, including the governed package manifest, exact artifact binding, SQLite integrity/foreign keys, 14 append-only triggers, strict pre-start timing, payload hashes, and no scoring contamination. A retained 05:30-style empty official-lineup capture was also parsed as 30 retryable team boards and zero eligible hitters rather than a false completed board.

The scorer is ready to begin with the next eligible slate, 2026-08-24, subject to the normal governed lineup/starter source becoming available before each game starts.
