# Historical replay exclusion recovery

The review reproduced all 89 original exclusions without changing the V1 control population (`a8f4aa89…d0c7b4`). The first inventory stopped when its expected derived `bol_tb15_market_rows.csv` path was absent. It did not interpret the older run-tagged raw odds JSON together with the paired same-run roster.

That retrieval gap caused 57 false-negative date exclusions. Those dates contain 8,606 exact Grade B identities across 737 games. The older provider event time is consistently one minute later than the paired official game time; team identity plus a bounded start-time comparison preserves a unique game binding, including doubleheaders.

The remaining 32 dates are not recoverable without weakening the contract: 29 have no eligible two-sided BetOnline TB 1.5 market, and three have no qualifying normal-run evidence. No authoritative archive pilot was justified because an archive cannot recreate missing contemporaneous sportsbook prices.

The V2 population was frozen before outcomes. Combined with the original control it contains 9,267 identities, 62 dates, and 796 games. No outcomes, final lineups, later prices, or current roster state influenced membership.

Complete-game verification found exact official/local batter-participant sets for all 796 represented games. At the frozen-identity level, 8,659 local rows verified exactly, 575 were missing locally, and 33 had stat mismatches; official exact-ID feeds governed without database writes.
