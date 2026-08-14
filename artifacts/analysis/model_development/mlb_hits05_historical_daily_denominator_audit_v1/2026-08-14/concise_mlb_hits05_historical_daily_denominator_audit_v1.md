# MLB Hits 0.5 historical daily denominator audit v1

- Legitimate retained Hits 0.5 range: 2026-03-25 through 2026-08-14; legacy model artifacts end 2026-08-03 and recovered benchmark ends 2026-08-02.
- May 8 is the first reconcile schema with scheduled start, enabling strict pregame validation.
- Retained model dates: 129; games: 1572; hitter-game opportunities: 30,303.
- BetOnline Hits 0.5 player-games: 25,402; model-selected player-games: 28,840.
- Model selected-side rows: Over 22,105; Under 8,119. The producer emitted a preferred direction per source row, not a governed two-sided all-board population; side flips and repeated snapshots can expose both historical sides.
- Original synchronized Hits 0.5 rows: 6,750; recovered: 11,072.
- `PRE_MAY8_RECOVERY = PARTIALLY_RECOVERABLE`; `POST_AUG2_RECOVERY = PROSPECTIVE_ONLY`; `HISTORICAL_MODEL_PROVENANCE = UNRESOLVED`.
- Decision: `HITS05_HISTORY_IS_SELECTED_SUBSET_NOT_FULL_BOARD`.
