# NHL three-lane preseason rehearsal — executive summary

`READY_WITH_DOCUMENTED_NONBLOCKING_LIMITATIONS`

The complete offline day-one sequence passed against one isolated, nonempty season-2026 preseason fixture sharing game ID `2026020001` across Moneyline, SOG, and Points. Both MIDDAY and FINAL_PREGAME were create-only and distinct; finalized trees remained byte-stable through later phases and grading. No database write, network fetch, live mutable archive, upload, execution, or Goalie Saves process occurred.

Moneyline produced P=1/M=1 in both phases. SOG produced P=12/M=12 at MIDDAY and P=12/M=6 at partial-coverage FINAL; its explicit fixture effective-config hash was `86095e176f3fd916aca4b932bde3ce4810823dbf1ba751b05e6007af464fe442`, candidate lineage ran, upload output was disabled, and E=0. Points produced P=9/M=6 at MIDDAY and P=9/M=3 at partial-coverage FINAL; its materially incoherent ladder stayed visible in P and absent from M, while C/U/E remained zero under `RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`.

All 24 injected negative tests passed: duplicate run identities, absent SOG policy, substitute Points policy, post-start quotes, unknown game types, incomplete output, low/zero-usefulness coverage states, partial-slate RED, unsafe mutable-input RED, cross-lane isolation, and accidental Saves invocation. All preseason grades were non-evaluative, numeric Moneyline regular-season targets were absent, SOG emitted no ordinary preseason settlements, Points emitted no observed preseason result, and no grader implied execution or mutated its pregame parent.

