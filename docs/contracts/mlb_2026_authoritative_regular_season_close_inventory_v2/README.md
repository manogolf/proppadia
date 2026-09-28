# MLB 2026 regular-season close inventory reconciliation V2

This is an offline reconciliation package only. It does not perform season
closure, create a close package, access a database, or make a network request.

The population is the unchanged, hash-verified V1 game-phase authority:
2,430 exact regular-season gamePks (`gameType=R`) and 489 preseason games.
V2 reevaluates the prior 88-game blocker ledger against the V1 retained
schedule evidence and the exact-hash live-feed artifacts listed in
`retained_live_feed_evidence.jsonl`. Raw StatsAPI artifacts remain in their
existing retention locations and are not copied into this package.

The deterministic result is 68 newly accepted finals, 20 unresolved games,
and the already accepted prior population. Four exact-gamePk live feeds are
final but conflict with the retained schedule's officialDate; with no retained
schedule relationship establishing a resolved reschedule, they remain
unresolved. The other 16 have no matching retained live feed in the searched
retention roots. Each unresolved row carries its specific evidence requirement.

To rebuild/validate offline:

```sh
.venv/bin/python -m backend.mlb.scripts.reconcile_mlb_2026_regular_season_close_inventory_v2
```

The `--build` option is for an explicitly authorized refresh of the pinned
evidence manifest and generated package. The committed default validator only
checks hashes and deterministic reconstruction against the pinned evidence.
