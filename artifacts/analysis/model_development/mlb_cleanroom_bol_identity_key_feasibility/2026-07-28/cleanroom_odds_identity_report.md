# BetOnline Player-prop Identity-key Feasibility

## Result

The current parser did not omit a usable player identity key. The certified current
cycle manifest contains 53 payloads: 31 official MLB payloads and all 22 odds
payloads. For stronger cross-snapshot evidence, this audit also inspected the prior
two clean-room attempts: 63 event-odds payloads and
3 event-list payloads in total. The raw player-prop outcome
contains only side, display-name description, price, and point.

The documented `includeSids` expansion was tested. BetOnline returned a stable
event-page SID and link, but market SID was null and all 38
outcome SIDs were null. The SID identifies the game, not a player or selection.

Repeated payloads demonstrate that the provider event ID remains stable while prices
can change, but it collides across every player in the game. No player entity exists
to crosswalk authoritatively to MLBAM.

Routes A, B, and C fail. Route D is technically possible but remains
`NAME_DEPENDENT_BRIDGE_PROHIBITED`. No reviewed alternative simultaneously certifies
BetOnline coverage, stable player identity, and an authoritative MLB-ID crosswalk.

No parser or clean-room database change is authorized.
