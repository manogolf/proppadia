# August 2 normal outcome reconciliation

The prior 25 unsupported rows were a clean-room retrieval-scope gap. Nineteen already had exact local `player_stats` rows and all verified exactly against final official feeds. The other six were official nonappearances, for which the normal pipeline correctly had no appearance row.

Across the frozen 120: 103 exact local rows verified with zero stat conflicts; 17 lacked local rows but were resolved from official evidence (8 nonappearances, 2 zero-PA substitutes, and 7 official appearances). Source/hash coverage is 120/120 and technical unresolved is zero.

The complete-game comparison found 19 official batting appearances without normal `player_stats` rows, 0 extra local batter rows, and 0 stat mismatches across the represented games. Therefore the capture decision is `NORMAL_PIPELINE_OUTCOME_CAPTURE_MISSING_PLAYER_ROWS`. No database repair is authorized; the empty correction overlay confirms no local-vs-official value conflict among rows that exist.

Routine closeout now consumes the certified official reconciliation layer. `player_stats` is an index/comparison surface, while preserved or freshly recovered official payloads and SHA-256 hashes govern certification.
