# MLB 2026 authoritative regular-season close inventory V1

This package binds the complete 2026 regular-season population to the existing
verified file authority and reconciles disposition only from the retained,
source-hashed StatsAPI schedule observations. Membership is never inferred
from dates, model participation, market coverage, or outcome availability.

## Result

- Authoritative population: 2,919 gamePks.
- Regular season: 2,430 gamePks.
- Preseason: 489 gamePks.
- Missing, unknown, conflicting, or duplicate phase identities: zero.
- Dispositions: 1,968 `FINAL`, 25
  `POSTPONED_RESCHEDULED_IDENTITY_RESOLVED`, one
  `SUSPENDED_RESUMED_IDENTITY_RESOLVED`, zero
  `AUTHORITATIVELY_CANCELLED`, 436 `SCHEDULED_NOT_FINAL`, and zero
  `UNRESOLVED_IDENTITY_OR_STATUS`.

The inventory is valid and complete, but regular-season close readiness is
`REGULAR_SEASON_CLOSE_BLOCKED` because 436 gamePks lack retained authoritative
terminal status evidence. Their exact identities are in
`scheduled_not_final_game_pks.json`. No status was reconstructed from a date.

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
`disposition_source_supplement_manifest.jsonl`. The combined 466-file / 9,124-
observation population is independently hashed. The ignored raw responses were
not added to this contract package.

StatsAPI represents each observed postponement/reschedule and suspension/resume
under the same exact gamePk. `relationship_ledger.jsonl` preserves that fact,
the raw relationship fields, and explicit original/replacement/resumed identity
objects. There are 28 relationship-bearing gamePks: 25 final resolved
reschedules, one final resumed game, and two rescheduled games still lacking
retained terminal evidence.

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
inventory option, output option, or package-writing path. This task did not run
that close checker and did not execute a close.

## Validation and next action

The dependency-free validator executed all 10 intended scenarios with no
failures or skips. It also rebuilt the inventory offline and required byte-for-
byte deterministic equality. See `validation_report.json` for the exact report.

The smallest next action is to allow ordinary retained authoritative schedule
observations to supply terminal status evidence, then rebuild this inventory
offline. No close action is justified until both `SCHEDULED_NOT_FINAL` and
`UNRESOLVED_IDENTITY_OR_STATUS` are zero and separate closure authorization is
given.
