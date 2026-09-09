# NHL season-2025 Saves market-listed goalie / actual-starter concordance

## Decision

`MARKET_LISTING_SUFFICIENT_WITH_ADDITIONAL_GATE`

The analysis uses 28,078 qualified pregame Saves outcome observations across 1,056 games. Actual starter is postgame-only and available for 706 team-games (706 starter rows); it never enters the pregame table. The latest qualified market state overlaps 544 evaluable team-games and uniquely lists the actual starter in 538 (98.9%).

Market listing is an availability signal, not starter confirmation. All qualified rows have source status `NOT_PROVIDED`; suspended/closed comparison is not estimable from the qualified population. Most dates contain one surviving snapshot, so discordant listings cannot be labeled true starter changes. Canonical identity gates exclude known join errors; remaining discordance is retained as unresolved without a confirmed pregame source.

## Policy comparison

- A_EVERY_ACTIVE_MARKET_LISTED_GOALIE: coverage=0.7705382436260623, mismatch=0.010948905109489052
- B_EXACTLY_ONE_LISTED_GOALIE_PER_TEAM: coverage=0.7648725212464589, mismatch=0.003703703703703704
- C_MULTIPLE_BOOKS_UNIQUE_GOALIE_AGREEMENT: coverage=0.7705382436260623, mismatch=0.003676470588235294
- D_EXTERNAL_PROJECTED_CONFIRMED_SOURCE: coverage=NOT_MEASURABLE, mismatch=NOT_MEASURABLE

Policy A is too permissive because every additional listed goalie becomes a nonstarter prediction. Policy B is the simplest fail-closed market-only shadow gate. Policy C tests independent book agreement and preserves broader states only when one goalie has unique multi-book support. Policy D remains the requirement for any future projected/confirmed starter state; it has no historical repository coverage to score here.

No candidate, upload, execution, production authorization, or starter-confirmation designation is created by this assessment.
