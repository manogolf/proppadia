# NHL season-2025 player-prop market archive canonical join index

## Result

The immutable book-level index contains 1,058,841 raw SOG/Points/Saves outcome observations and 1,046,999 identity- and timing-qualified pregame observations from 195 raw market parent files. It retains every book, line, side, price, timestamp, status, raw locator, and parent hash. No source file was changed and no missing side was synthesized.

## Decisions

- `SOG_BOOK_LEVEL_ARCHIVE_READINESS` = `READY_WITH_DOCUMENTED_LIMITATIONS`
- `POINTS_BOOK_LEVEL_ARCHIVE_READINESS` = `READY_WITH_DOCUMENTED_LIMITATIONS`
- `SAVES_BOOK_LEVEL_ARCHIVE_READINESS` = `READY_WITH_DOCUMENTED_LIMITATIONS`
- `POINTS_PREDICTION_MARKET_LINKAGE_READINESS` = `READY_WITH_DOCUMENTED_LIMITATIONS`
- `SAVES_PREDICTION_MARKET_LINKAGE_READINESS` = `READY_WITH_DOCUMENTED_LIMITATIONS`

## Controls and limitations

Event bindings require canonical team orientation plus schedule corroboration. Player bindings are game-scoped; exact normalized names are corroborated and initial/last aliases are accepted only when unique. Ambiguous, unresolved, conflicting, post-start, indeterminate, invalid-price, and suspended rows remain in the raw evidence table but not in the qualified table.

The observation-time precedence is outcome, market, bookmaker, response, then filesystem/archive timestamp. Timing qualification compares that observation time to the provider event's scheduled commence time, falling back to the canonical schedule only when provider time is absent; both start times remain preserved. Historical filesystem mtimes record retrospective retrieval, not contemporaneous capture. The known at/post-start market-object counts and the April 16 Saves alias-expansion discrepancy are tested in `reconciliation_report.csv`.

Prediction linkage means only canonical game/player/line correspondence to a qualified quote. It is not evidence of candidate selection, upload, execution, or grading. Points rows retain ladder-coherence disposition without deleting blocked predictions. Saves retains the finding that no certified expected starter existed, `start_prob` was null then zero-filled, actual `start_flag` was postgame-only, and market presence was not confirmation.

## Enabled next

This index enables a Points historical outcome spine/coherent prediction foundation, book-level historical comparison, accurate characterization of Saves market availability, and optional SOG market benchmarking. It does not establish Points/Saves policy or performance, certify goalie starters, authorize any season-2026 lane, or support betting-edge claims.
