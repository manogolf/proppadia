# MLB 2026 Postseason Authority Extension Preflight V1

This is a read-only design preflight. It did not call a provider, connect to a
database, run a pipeline, change a schedule, build or activate authority, close
the regular season, or change production source.

## Decision

Use an **immutable versioned full snapshot**. Keep the current V1 proposal and
source manifest at their existing paths and hashes. A candidate V2 consists of:

1. the 2,919 current proposal rows copied byte-for-byte and in the same order;
2. new exact-`gamePk` rows derived only from newly retained StatsAPI schedule
   bytes through `contract_v1`;
3. an immutable union source manifest which preserves every V1 source entry;
4. a version descriptor pinning the predecessor, proposal, source manifest,
   phase contract, counts, supported window, and base/new population hashes;
5. a reviewed active-descriptor switch performed atomically and only under a
   separate authorization.

This combines the simple one-index consumer behavior of a full snapshot with
append-only preservation of the V1 rows. An overlay is smaller on disk, but it
adds precedence, union, and rollback logic to every load. Replacing V1 in place
would destroy its identity. The database sidecar remains a possible later
backend, but adds operational state without solving a present file-authority
need; the superseded dual-table migration remains prohibited.

## Current authority and observed limitations

The verified file authority is exactly 2,919 gamePks: 2,430
`REGULAR_SEASON`, 489 `PRESEASON`, and zero `POSTSEASON`. Its proposal SHA-256
is `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`,
its source-manifest SHA-256 is
`766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`,
and its semantic authority-record hash is
`5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84`.
It supports 2026-02-20 through 2026-09-27.

The present loader is not extension-capable. Although its constructor accepts
alternate paths and expected file hashes, it still requires exactly 2,919
rows, 464 source files, 9,092 source observations, the frozen type/phase
counts, and the module-level date horizon. It supports neither an appended
snapshot nor an overlay, and a different full replacement fails those fixed
checks.

The offline builder can parse an explicitly named retained schedule file, but
directory discovery matches only files named `schedule.json`. The active
governed-lineup and provider-binding paths retain files named
`statsapi_schedule_<date>_<run>.json` and `statsapi_schedule__<run>.json`.
They therefore are not automatically discovered. The clean-room
`schedule.json` layout is discoverable, but its locally retained estate ends
at 2026-08-02 and cannot be assumed to be the ordinary postseason source.

## What ordinary retention preserves

The ordinary current-slate schedule calls retain unmodified response bytes and
SHA-256 before normalized use. A retained 2026-09-22 governed-lineup response
contains exact `gamePk`, exact `gameType`, `season`, `gameDate`,
`seriesDescription`, and a real `rescheduledFrom` relationship. The canonical
parser preserves raw type, source season, raw round, normalized round/phase,
and eight rescheduled/resumed relationship fields. `gameDate` is preserved in
the immutable raw response, but the current authority proposal record does not
materialize scheduled start; a candidate descriptor/source reference must not
claim otherwise.

The installed daily runbook derives the MLB date from the current Eastern date
and contains no September 27 stop. Its governed lineup and provider schedule
captures therefore can retain an actual postseason schedule response in an
ordinary window. The agreement V4 collector is different: its authorization is
explicitly frozen through 2026-09-27 and must not be extended by this authority
change.

The old one-request source-completion acquisition covered 2026-02-20 through
2026-03-25 and does not cover postseason. No additional dedicated request is
scientifically required if an already-authorized ordinary current-slate
StatsAPI window retains the new game. That ordinary free StatsAPI request still
exists; the claim is zero **additional** requests and zero paid credits.

## No evidence versus failure

An offline scan returns `NO_NEW_AUTHORITY_EVIDENCE` only when every selected
retained file exists, hashes and parses successfully, all observations of the
2,919 base gamePks agree with V1, and no previously absent gamePk with an
authoritative classified StatsAPI type is present. It is not an error and must
not activate anything.

Missing files, hash mismatches, malformed payloads, absent or unknown types,
season/type/round/relationship conflicts, duplicate identity conflicts, or a
base-row semantic difference are failures. A newer configured snapshot that
fails validation blocks all governed use; never silently fall back to V1.

## Consumer conclusion

Moneyline, Full-board Hits, RAW Totals, Totals C, and the phase partition side
of agreement already consume the stable exact-gamePk interface and record the
authority identity in new evidence. They need no lane-specific phase logic.
Provider-event binding, Pinnacle, feature lineage, BvP joining, and training
likewise remain interface-compatible after one shared loader cutover.

Two exceptions require explicit handling:

- The close inventory/checker hard-requires the 2,919/2,430/489 V1 counts and
  the V1 manifest. It must stay explicitly pinned to V1 or be made
  version-aware while proving the same 2,430 regular-season set. The Ops Brief
  calls that checker, so it is blocked by the same coupling.
- Agreement collection is code-ready for phase partitioning but its actual V4
  acquisition horizon stops on September 27. Postseason collection would need
  a separate acquisition authorization/configuration; authority extension does
  not grant it.

Historical contracts and retained artifacts keep their original hashes. They
are not refreshed or re-certified merely because a later active descriptor
exists. Tests which mean “V1 fixture” must instantiate V1 explicitly; tests of
the active authority must use the configured descriptor.

### Exact rejection and refresh map

The following current code directly asserts V1 population or hash values and
would reject, or mischaracterize as a failure, an extended default authority:

- `game_phase_authority_v1.py` (hashes, 2,919 rows, 464 files, 9,092
  observations, type/phase counts, and supported window);
- `regular_season_close_inventory_v1.py` plus its validator and test (2,919
  total and 2,430/489 phase counts);
- the Ops Brief/daily-index validator and test (2,919 total);
- the Moneyline and Full-board Hits validators (2,919 and/or zero postseason);
- the original Hits file-authority pilot validator (exact proposal/source
  hashes and counts);
- the training eligibility dry-run builder (exact proposal/source hashes); and
- the prepared canonical activation loader (2,919-row V1 contract).

These are not reasons to rewrite old packages. V1 historical validators should
load the explicit V1 descriptor. Active-authority tests should validate the new
descriptor. Totals and agreement gate unit tests use injected synthetic
authorities and do not hard-code the V1 population, while their retained
contract artifacts remain immutable and keep the authority hashes recorded at
the time they were produced.

## First actual postseason game

The first ordinary retained schedule containing a new gamePk and raw type
`F`, `D`, `L`, `W`, `P`, or `C` is sufficient source evidence for an offline
candidate. It is not, by itself, proof that every lane is operational: a lane
may have no naturally eligible market, player prop, BvP row, outcome, or
authorized agreement capture for that game. After a separately authorized
descriptor activation, each lane must be observed in its next ordinary eligible
window. Existing retained raw bytes are reused; no manual API replay or
duplicated acquisition is part of the design.

## Required classifications

| Classification | Result |
|---|---|
| Ordinary postseason source-retention readiness | `READY_RAW_EXACT_GAME_PK_ORDINARY_WINDOW`; actual postseason evidence not yet retained in authority |
| Offline authority-extension readiness | `DESIGN_READY_IMPLEMENTATION_REQUIRED`; discovery/versioning/activation are absent |
| Current consumer compatibility | `CONDITIONAL_SHARED_INTERFACE_READY`; current loader and close coupling reject extension |
| Additional-request requirement | `ZERO_ADDITIONAL_DEDICATED_REQUESTS_EXPECTED_ZERO_PAID_CREDITS`; ordinary free StatsAPI window still occurs |
| Activation readiness | `BLOCKED_NO_POSTSEASON_EVIDENCE_AND_NO_VERSIONED_LOADER` |
| Rollback readiness | `DESIGN_READY_NOT_IMPLEMENTED`; atomic descriptor repin is specified |
| Existing-authority preservation | `READY_IF_IMMUTABLE_VERSIONED_FULL_SNAPSHOT_IS_ENFORCED` |
| Smallest justified implementation task | `IMPLEMENT_VERSIONED_FILE_AUTHORITY_DESCRIPTOR_BUILDER_LOADER_AND_CLOSE_PINNING_NO_ACTIVATION` |
