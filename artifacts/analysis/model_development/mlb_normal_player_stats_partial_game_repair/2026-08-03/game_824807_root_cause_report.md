# Game 824807 root cause

Primary cause: `insert_mlb_stat_derived._final_games` required `status.detailedState == "Final"`. MLB finalized game 824807 as `Completed Early: Rain` with authoritative `abstractGameState=Final`, `codedGameState=F`, and status code `FR`. The loader excluded the entire game before `game_info`, feed acquisition, parsing, or upsert. That is why all 19 official batter participants—as well as every downstream row for the game—were absent.

Contributing cause: the completed-slate health gate accepted date-level presence of some `model_training_props` and `player_stats` rows. It never compared each final game's official participant set, so the other 14 games allowed August 2 to pass.

The original ingestion schedule/live/boxscore payload and a per-game loader log were not preserved, so row-level claims about that exact HTTP response are deliberately marked `NOT_PROVABLE`. However, current final schedule/feed state, zero `game_info`/player/downstream rows, and the deterministic exact-string selection code establish that the terminal nonliteral detailed state excluded the game before parsing. There is no evidence of a transaction interruption, row failure, or deletion.

Current parser replay produced all 19 authoritative batter participants, including zero-PA/substitute evidence, with zero missing, extra, or mismatched rows. The clean dry run inserted exactly those 19 identities, changed zero existing rows, and the second invocation wrote zero rows. Rollback SQL is exact.

The finalized gate now invokes per-game official participant/stat completeness. Missing games enter bounded exact-game recovery; conflicts remain visibly `COMPLETED_GAME_PLAYER_STATS_INCOMPLETE` and broad overwrite is refused.
