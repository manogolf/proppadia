# BvP exactly-once and identity assessment

As of the frozen extraction at `2026-09-22T22:37:47Z`, the local acquisition state contains 4 attempts, 4 results, 4 successes, and 4 distinct successful dates, with zero duplicate successful dates. The successful 2026-09-22 primary acquisition was followed by three ordinary windows that returned the preserved status `BVP_INLINE_SUCCESS_ALREADY_EXISTS`; no second acquisition was performed for that date.

The identity journal contains 1,426 records across four files and 49 distinct official gamePks. `refresh_mlb_bvp_pvb.py` keeps `game_id == official_game_id`; date/team matching is expressly optional telemetry and cannot replace official identity. Duplicate game IDs, off-date identity, and unresolved postponed/cancelled/suspended status fail closed. The operational storage key is `(prop_type, player_id, game_id, feature_set_tag)`.

This proves current exact-gamePk acquisition health, including a same-team doubleheader on 2026-09-22 with separate gamePks and starts. It does not prove postseason operation. BvP rows do not carry an independently maintained phase column; their exact gamePk is sufficient for a later authoritative join. Postseason BvP may be retained for governed 2027 strict-prior research while being excluded from 2026 regular-season evaluation, provided the file authority has first been extended to those gamePks.
