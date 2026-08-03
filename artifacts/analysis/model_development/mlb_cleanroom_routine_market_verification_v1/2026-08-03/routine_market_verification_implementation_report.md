# Routine market verification implementation — 2026-08-03

The lineup-gated H1 path is retired because it is temporally misaligned, not because it statistically failed. The replacement freezes the first non-empty eligible normal-run population, using exact run-tagged game/player identity, same-run player/game roster state, two-sided BetOnline TB 1.5 prices, timestamps, and SHA-256 lineage. Lineups and batting order are absent from pregame membership.

August 2 migration: 120 otherwise eligible early identities existed before the lineup filter: 89 admitted and 31 excluded only because lineup was unconfirmed. All 31 have exact identity and two-sided-market records. Preserved final feeds verify role/result for 6; the remaining 25 are explicitly `MISSING_AUTHORITATIVE_SUPPORT` in this bounded migration package, not inferred. This is source/data verification only.

August 3 is not used: `local_daily_20260803T123004Z` completed before implementation certification. The next untouched Pacific date is the first prospective opportunity.

Tests: 58 passed. Installed wrapper loads `backend/.env`; old hooks are 0, routine hook is 1, and the sidecar is nonblocking after normal output generation.
