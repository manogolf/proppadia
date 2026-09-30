# MLB 2026 postseason phase-authority extension V1

This child extends the immutable V1 file authority only for the four exact
gamePks in the retained Sep 29 schedule evidence. The source file is pinned by
SHA-256 `fca8218b214cbe3bf6b4d9d58fbf81cfa29bb9a384afed5f9f468c9958bb8b3e`.
The V1 proposal remains an unchanged byte prefix; the child proposal adds only
849843, 849845, 849849, and 849851, each classified from source `gameType=F`
as `POSTSEASON` / `WILD_CARD`. `source_round` preserves the source labels:
`NL Wild Card Series` for 849843 and 849845 and `AL Wild Card Series` for
849849 and 849851. No other postseason game is asserted by this extension.

The child descriptor and evidence files are under
`backend/mlb/season_transition/authority_snapshots/v2/`. The active selector
uses the existing V1 selection schema and the repository's governed activation
path. The regular-season population remains V1-pinned at 2,430 games; the
retained V2 close artifact remains byte-identical at SHA-256
`fa96e14158d6c5e77856be9d03a8d4eae1b2c4eb922442fb20f13a1ec1e112e3`.

Validate offline with:

```sh
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_phase_authority_v2 --validate
.venv/bin/python -m unittest backend.mlb.tests.test_mlb_2026_postseason_authority_extension_v1
.venv/bin/python -m backend.mlb.scripts.reconcile_mlb_2026_regular_season_close_inventory_v2
```

No daily wrapper, database, network/provider request, migration, or scheduled
run is part of this authority update. The next natural Moneyline window is
the operational validation; do not manually replay Sep 29.
