# MLB 2026 authoritative regular-season close inventory V1

This package binds the complete 2026 regular-season population to the existing
verified file authority and reconciles disposition only from retained,
source-hashed StatsAPI schedule observations and exact-gamePk live feeds.
Membership is never inferred from dates, model participation, market coverage,
or outcome availability.

## Result

- Authoritative population: 2,919 gamePks.
- Regular season: 2,430 gamePks.
- Preseason: 489 gamePks.
- Missing, unknown, conflicting, or duplicate phase identities: zero.
- Dispositions: 2,316 `FINAL`, 25
  `POSTPONED_RESCHEDULED_IDENTITY_RESOLVED`, one
  `SUSPENDED_RESUMED_IDENTITY_RESOLVED`, zero
  `AUTHORITATIVELY_CANCELLED`, 88 `SCHEDULED_NOT_FINAL`, and zero
  `UNRESOLVED_IDENTITY_OR_STATUS`.

The inventory is valid and complete, but regular-season close readiness is
`REGULAR_SEASON_CLOSE_BLOCKED` because 88 gamePks still lack retained
authoritative terminal status evidence. Their exact identities are in
`scheduled_not_final_game_pks.json`: 16 were current-date nonterminal games and
72 were future scheduled games in the committed temporal audit. No status was
reconstructed from a date.

## Provenance and identity

`close_inventory_manifest.json` pins the authority proposal, authority-record
population, retained-source manifest, exact ordered gamePk population, and
inventory bytes. Every JSONL record retains the exact raw game type,
normalized phase, scheduled-start observations, authoritative status evidence,
final score/outcome when present, schedule relationships, disposition reason,
and source artifact identity/SHA-256.

Disposition evidence comprises the 464 files / 9,092 observations already
bound by the phase authority plus two ordinary September 22 retained schedule
captures / 32 observations frozen in
`disposition_source_supplement_manifest.jsonl`, plus 348 exact-gamePk final
StatsAPI live feeds selected by the source-hashed temporal-coverage audit. The
combined 814-file / 9,472-observation population is independently hashed. The
retained raw feeds remain at their original paths and were not copied into this
contract package.

Those 348 games were prior inventory omissions, not unfinished games. Their
terminal disposition is accepted only when the feed root `gamePk` equals
`gameData.game.pk`, raw game type and season match phase authority, and the
authoritative status fields form a recognized final state. Scores, dates,
filenames, and file mtimes cannot establish terminal status. Each accepted row
retains the feed path, SHA-256, and StatsAPI metadata timestamp when present.

StatsAPI represents each observed postponement/reschedule and suspension/resume
under the same exact gamePk. `relationship_ledger.jsonl` preserves that fact,
the raw relationship fields, and explicit original/replacement/resumed identity
objects. There are 28 relationship-bearing gamePks: 25 terminal schedule
observations with resolved reschedule identity, one terminal resumed game, one
locally recovered final feed whose reschedule relationship remains preserved,
and one current-date rescheduled game still lacking terminal evidence.

## Hardened close checker

The prepared checker accepts no caller arguments:

```bash
PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python \
  -m backend.mlb.scripts.prepare_mlb_2026_regular_season_close_v1
```

It reads only this canonical package, verifies the pinned manifest and authority
hashes, requires the exact 2,430-game regular population, rejects omitted,
extra, preseason, or postseason identities, and exits nonzero for scheduled or
unresolved games. It has no execution mode, authorization token, arbitrary
inventory option, output option, or package-writing path. Validation ran this
checker in check-only mode and observed the required nonzero blocked result; it
did not create a close package or execute a close.

## Validation and next action

The dependency-free validator executed all 18 intended scenarios with no
failures or skips. It also rebuilt the inventory offline and required byte-for-
byte deterministic equality. See `validation_report.json` for the exact report.

The smallest next action is ordinary retention for the 88 remaining games,
followed by a separately authorized offline rebuild. No close action is
justified until both `SCHEDULED_NOT_FINAL` and
`UNRESOLVED_IDENTITY_OR_STATUS` are zero and separate closure authorization is
given.
