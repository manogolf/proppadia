# MLB current state and prop next steps — 2026-08-14

- Moneyline: certified/public-ready; current frozen rows 14; betting authority remains disabled.
- Totals: valid with limitations/private-only; current frozen 11; pending 3.
- Player-prop collection: healthy; direct BetOnline and supplemental FanDuel observations remain useful as market data.
- Prop prediction authority: `NO_QUALIFIED_MLB_PROP_MODEL`. No current prop lane demonstrates improved prediction quality. Hits 0.5 evidence is insufficient; DH evidence is insufficient; the Total Bases shadow is stale and did not improve Brier.
- Decision: `PROP_SECTION_RESTORE_MARKET_MONITOR_ONLY`. A read-only market monitor is justified by healthy collection, but prediction content is not.
- Exact next step: separately scope a read-only, non-executable prop market monitor using direct-source provenance; do not restore prediction content without a new qualification review.
