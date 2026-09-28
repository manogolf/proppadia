# MLB 2026 regular-season close inventory reconciliation V2

This is a reconciliation package only. It does not perform season closure,
create a close package, access a database, or make a network request during
build or validation.

The population is the unchanged, hash-verified V1 game-phase authority:
2,430 exact regular-season gamePks (`gameType=R`) and 489 preseason games.
V2 reevaluates the prior 88-game blocker ledger against V1 schedule evidence,
retained exact-hash live-feed artifacts, and the bounded Moneyline history
schedule root `artifacts/ops/mlb_public_game_moneyline_history_schedules/`.
Every matching source-file/gamePk pair found in those retained roots is pinned
in `retained_live_feed_evidence.jsonl` or
`retained_schedule_relationship_evidence.jsonl`; raw artifacts remain in their
existing retention locations. Their hashes and the interpretation rule are
recorded in `reconciliation_manifest.json`.

The deterministic result accepts 87 of the prior 88 blockers and leaves only
823490 unresolved. Fifteen of the 16 former feed gaps have retained exact-gamePk
regular-season schedule appearances with `Final` status and scores; 823490 has
one retained live-feed capture but no status accepted by the frozen contract.
The single GET receipt is in `source_completion_823490_receipt.json`; its
198,713-byte raw response is retained at the receipt's hash-addressed path. The
feed confirms exact gamePk 823490, `gameType=R`, season 2026, and teams 110/147,
but reports `Final / C / CR / Cancelled: Rain`. The frozen cancellation tuple
does not recognize status code `CR` or the detailed state `Cancelled: Rain`,
so V2 preserves the evidence and leaves the game unresolved without broadening
the close contract. For 823489, 824703, and 824705, matching exact gamePk,
`gameType=R`, season 2026, stable away/home teams, and one consistent playable
terminal feed link the schedule appearances despite differing `officialDate`
values in older captures. The terminal feed's `officialDate` is recorded as
`played_official_date`; all observed schedule/feed dates and appearances remain
retained in the inventory evidence. A date change alone is not a conflict and
does not require explicit `rescheduledFrom` when exact identity and one
consistent completed outcome establish the game. Conflicting identity, teams,
terminal status, or result remains fail-closed. 824785 remains resolved using
its retained postponed/final appearance relationship, including final-feed
SHA-256 `61bdfdaae620c95da70b0e0940d854d8a789a0e1ef2e0e5bb325b7de41d8406b`.

To rebuild/validate offline:

```sh
.venv/bin/python -m backend.mlb.scripts.reconcile_mlb_2026_regular_season_close_inventory_v2
```

The `--build` option refreshes the pinned manifest from the documented bounded
retained roots and rebuilds the generated package. The default validator only
checks hashes and deterministic reconstruction against the pinned evidence.
