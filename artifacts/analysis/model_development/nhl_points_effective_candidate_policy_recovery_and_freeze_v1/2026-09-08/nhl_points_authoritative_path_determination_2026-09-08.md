# NHL Points authoritative candidate-policy path determination

Task: `NHL_POINTS_EFFECTIVE_CANDIDATE_POLICY_RECOVERY_AND_FREEZE_V1`  
Assessment date: `2026-09-08`

## Determination

There is no surviving authoritative Points candidate/upload path to freeze. The callable legacy operator path is `backend.nhl.cli daily --with-odds` through Points scoring, generic prediction persistence, and `build_points_with_market.py`; it terminates in a mutable display CSV. `bin/nhl_ops.sh` and the Command Deck expose candidate and upload commands only for SOG.

The current `/nhl/props` operator route does display “Top Points Model Edges,” but it is explicitly a research workspace surface. It computes an Over-only maximum-gap line per player-game, filters prices to -350 through +500, ranks by gap then selected model probability, and displays ten cards. Points cards can only be watched or opened; they cannot be saved or uploaded. The separate form explicitly labels Points staged.

The retained static `nhl/site/index.html` is an older client-only analysis table. Its defaults choose the first line (0.5), retain unpriced rows under its edge filter, rank priced rows by EV, and export a generic table with runtime-mutable Top N (default 20). The current backend mounts its data directory, not the static page. This behavior materially conflicts with the React research surface and is not book-upload authority.

The generic `/api/nhl/props/add` service can accept a manually constructed `points` row, but no current Points UI invokes it. Its record lacks sportsbook, quote timestamp/status, market snapshot, policy identity, upload identity, and execution evidence. It is a compatibility/manual persistence mechanism, not an automated candidate or upload policy.

The new season-2026 Points shadow path is therefore the only authoritative prospective path. It correctly applies the ladder-coherence gate first and then fails closed with `RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`; no effective policy JSON is accepted and C/U/E remain empty.

## Git and archive evidence

- Local Git tracks Points scoring/display origins in `b8cef981`, `9d59664e`, and the React Points edge surface in `97a673f2`.
- Repository and all-name Git history searches found no Points candidate selector, Points upload builder, candidate ledger, or upload ledger.
- The 48 canonical `points_with_market.csv` files span 2026-02-27 through 2026-04-16, excluding 2026-04-10. Six files in `exports/history` are exact SHA256 aliases and were excluded from replay.
- The archive contains 59,133 prediction-line rows and 19,711 complete player-game ladders. It is a display/market corpus, not candidate/upload authority.

## Decision

`NHL_POINTS_EFFECTIVE_POLICY_DECISION = NOT_RECOVERABLE_SHADOW_ONLY`

No config, hash identity, selector integration, upload artifact, execution record, or production change is authorized or created.
