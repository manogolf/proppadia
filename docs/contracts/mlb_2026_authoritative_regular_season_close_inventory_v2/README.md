# MLB 2026 regular-season close inventory reconciliation V2

This is an offline reconciliation package only. It does not perform season
closure, create a close package, access a database, or make a network request.

The population is the unchanged, hash-verified V1 game-phase authority:
2,430 exact regular-season gamePks (`gameType=R`) and 489 preseason games.
V2 reevaluates the prior 88-game blocker ledger against the V1 retained
schedule evidence and the exact-hash live-feed artifacts listed in
`retained_live_feed_evidence.jsonl`. Additional schedule-history evidence for
the four date-conflict cases is pinned in
`retained_schedule_relationship_evidence.jsonl`; raw StatsAPI artifacts remain
in their existing retention locations and are not copied into this package.

The deterministic result accepts 69 of the prior 88 blockers and leaves 19
unresolved. Retained schedule history resolves 824785 as a postponement and
reschedule by exact gamePk and complementary relationship fields across its
postponed and final appearances. The other three date conflicts remain
unresolved pending retained transition evidence; 16 blockers still lack a
matching retained feed in the documented roots. Each unresolved row carries
its evidence requirement.

To rebuild/validate offline:

```sh
.venv/bin/python -m backend.mlb.scripts.reconcile_mlb_2026_regular_season_close_inventory_v2
```

The `--build` option is for an explicitly authorized refresh of the pinned
evidence manifest and generated package. The committed default validator only
checks hashes and deterministic reconstruction against the pinned evidence.
