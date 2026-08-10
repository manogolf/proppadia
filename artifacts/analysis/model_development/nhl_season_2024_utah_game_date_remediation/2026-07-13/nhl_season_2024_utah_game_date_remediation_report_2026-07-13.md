# NHL season 2024 Utah game-date remediation and schedule certification

## Result

All 82 frozen Utah game dates were recovered and certified by exact agreement among official NHL club-schedule `gameDate`, official gamecenter landing `gameDate`, and the official start timestamp converted to Eastern date. Game ID and home/away identity match all 82 parent rows. There are no conflicts or unresolved dates.

The root cause is a bounded `HISTORICAL_IMPORT_GAP|FRANCHISE_TRANSITION_DEFECT`: Utah rows were inserted with the full historical batch but uniquely missed the later date enrichment applied to all 1,316 other season `2024` rows. The exact historical command is not preserved. A non-destructive overlay is used; `nhl.games` and all parent sources remain unchanged.

Schedule chronology is rebuilt on all 1,398 frozen games using date then game ID. Exact schedule-chain coverage rises from 163 to 1,398; date-blocked rows fall from 1,235 to 0; rest/opponent-rest qualified rows rise from 145 to 1378. No same-team same-date double booking, identity collision, season leakage, same-game leakage, or future-game inclusion exists.

Schedule/rest features are now `READY`; team+schedule baseline research is `READY_WITH_BOUNDED_LIMITS`. Goalie timing, model training, prices, ROI, deployment, and restart remain blocked. Exactly one next activity is unlocked: NHL full-game moneyline simple baseline specification and process-validation design.
