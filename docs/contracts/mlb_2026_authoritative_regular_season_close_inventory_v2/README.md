# MLB 2026 regular-season close inventory reconciliation V2

This is a reconciliation package only. It does not perform season closure,
create a close package, access a database, or make a network request during
build or validation.

The check-only close-readiness entrypoint remains
`backend/mlb/scripts/prepare_mlb_2026_regular_season_close_v1.py` for command
compatibility, but now validates this V2 package and its pinned reconciliation
manifest rather than the intentionally incomplete V1 inventory.

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

The deterministic result accepts all 88 prior blockers. Fifteen of the 16 former feed gaps have retained exact-gamePk
regular-season schedule appearances with `Final` status and scores; 823490 has
one retained live-feed capture reporting an authoritative cancellation.
The single GET receipt is in `source_completion_823490_receipt.json`; its
198,713-byte raw response is retained at the receipt's hash-addressed path. The
feed confirms exact gamePk 823490, `gameType=R`, season 2026, and teams 110/147,
but reports `Final / C / CR / Cancelled: Rain`. V2 now narrowly recognizes this
receipt-pinned source tuple (and its matching schedule form `Final / C / CR /
Cancelled`, reason `Rain`) as `AUTHORITATIVELY_CANCELLED`, distinct from `FINAL`.
This is terminal for close accounting only: no played date, score, final
outcome, or game result is created. The adapter requires exact gamePk 823490,
`gameType=R`, season 2026, teams 110/147, reason `Rain`, and the pinned feed
SHA-256. Unknown or conflicting cancellation evidence remains unresolved. The
V1 package and phase authority are unchanged. For 823489, 824703, and 824705, matching exact gamePk,
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

## Separate regular-season close operation

`backend.mlb.scripts.close_mlb_2026_regular_season` is separate from the
readiness checker. Its default invocation is a read-only plan and publishes
nothing. The close document contains the exact 2,430 V2 inventory rows and
binds the authority manifest, inventory bytes, and reconciliation manifest by
SHA-256. `AUTHORITATIVELY_CANCELLED` is retained as a terminal accounting
disposition; 823490 has no played date or outcome, and postseason rows are not
included.

Publication requires both `--execute` and a separately reviewed JSON
authorization with schema `MLB_2026_REGULAR_SEASON_CLOSE_AUTHORIZATION_V1`,
`authorizes_close: true`, non-empty `authorization_id` and `authorized_by`,
and an `input_binding` exactly matching the three hashes and 2,430 count shown
by the plan command. No authorization artifact is included in this package.
The default output is write-once: publication is atomic and refuses to replace
an existing close artifact, including on repeat invocation. The operation is
file-only; it makes no database or network request and does not activate phase
authority.
