# Exact Game-roster Identity Bridge Pilot

All 330 preserved BetOnline outcome rows were
evaluated. Event binding used exact home/away teams and scheduled start within ten
minutes; player binding used only the official game roster and approved deterministic
full-name normalization.

- Exact unique: 330
- No official roster match: 0
- Multiple official roster matches: 0
- Event-binding failures: 0
- Certifiable: YES

## Population and rates

- Provider events/games: 3
- Distinct player names: 55
- Over/Under paired identity groups: 55
- Repeated-snapshot groups: 55
- Price-change groups: 55
- Exact unique admission rate: 100.00%
- Unmatched rate: 0.00%
- Ambiguous rate: 0.00%
- Game-binding failure rate: 0.00%

All admitted mappings retain their raw path and SHA-256. Every repeated snapshot and
both sides resolved to the same MLB player ID despite price changes.

## Implementation

- Append-only bridge observations written: 165
- Exact odds sides written: 330
- Remaining identity rejects: 0
- Latest fully two-sided board rows: 39

The 55 identity groups are the ever-observed pilot population. Sixteen were no longer
present as fully two-sided offers in the latest snapshot; the neutral board correctly
contains the 39 latest two-sided rows.
