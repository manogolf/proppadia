# NHL Postgame Official Request Accounting Contract V1

## Scope and terminology

This contract applies to `bin/nhl_postgame_reconcile.sh YYYY-MM-DD --execute`.
It does not govern bookmaker traffic, which remains separately prohibited in
postgame reconciliation.

- `logical_request`: one intended endpoint/identity operation, whether it uses
  the network or a verified preserved response.
- `network_attempt`: one actual HTTP transport attempt. Retries are separate
  attempts under the same logical request.
- `successful_response`: a network attempt returning an accepted HTTP status.
- `failed_attempt`: a transport exception or rejected/non-success terminal HTTP
  response. An allowlisted redirect is accounted separately and is not a
  success, failure, retry, or fallback.
- `fallback_attempt`: an attempt against an explicitly secondary endpoint or
  endpoint variant.
- `cache_hit` / `preserved_response_reuse`: a content-addressed response was
  verified by endpoint identity and SHA-256 and used without network traffic.
- `authority_boundary_request`: the reconciliation wrapper's authoritative
  schedule or canonical boxscore request. It is a subset of total workflow
  requests, never a synonym for the total.

## Complete governed call graph

| Stage / caller | Endpoint family and identity | Required | Retry / fallback | Persistence and reuse |
|---|---|---:|---|---|
| `run_nhl_postgame_reconciliation.fetch_official` | schedule/date | yes | one authority attempt | content-addressed; reused by schedule ingestion and skater schedule discovery |
| same | boxscore/game | yes, one per canonical game | one authority attempt | content-addressed; reused by goalie and both skater boxscore consumers |
| `import_schedule_today.fetch_schedule_for_date` | schedule/date | yes | standalone: up to six attempts per candidate and two fallback forms; governed reconciliation: reuse required | exact authority schedule reuse |
| `refresh_players_and_roster_today.fetch_roster` | roster/team | yes | up to seven attempts; one governed `current`-to-eight-digit-season redirect hop; direct eight-digit season roster is fallback after terminal 404 only | distinct endpoint; repeated team/date use within the same slate reuses its verified terminal response |
| `refresh_players_and_roster_today.fetch_player_name_strict` | player landing/player | conditional | up to seven attempts | exact-ID lookup only; never a bulk roster-name backfill |
| `seed_goalie_logs_for_date.game_ids_from_api` | schedule/date | DB-empty fallback only | up to six attempts | governed reconciliation reuses authority schedule |
| `seed_goalie_logs_for_date.fetch_boxscore_api` | boxscore/game | yes | standalone up to six attempts | governed reconciliation reuses authority boxscore |
| `seed_skater_logs_for_date.get_schedule` | schedule/date | yes | standalone up to six attempts | governed reconciliation reuses authority schedule |
| `seed_skater_logs_for_date.refresh_roster_status_from_box` | boxscore/game | yes | standalone up to six attempts | governed reconciliation reuses authority boxscore |
| `seed_skater_logs_for_date.get_boxscore` | boxscore/game | yes | standalone up to six attempts | governed reconciliation reuses authority boxscore |
| `seed_skater_logs_for_date.ensure_player_exists` | player landing/player | conditional | up to six attempts | exact-ID lookup only for a participant absent from verified roster names and exact external-ID mappings; response is preserved and returned ID must match |
| `ingest_shiftcharts_for_date.fetch_shiftcharts` | shift chart/game | yes | one attempt | distinct response contract; not replaceable by boxscore |
| `backfill_game_manpower_segments.fetch_pbp` | play-by-play/game | yes | one attempt | distinct response contract; not replaceable by boxscore |
| `fill_pp_toi_minutes_for_date.fetch_boxscore` | boxscore/game | currently dormant in the executed DB-overlap path | one attempt if called | governed reconciliation requires authority boxscore reuse |

All network-capable children inherit a reconciliation run ID, journal path,
response-cache directory, requested slate date, canonical game IDs, and their
hash. Missing or inconsistent context fails closed before a governed request.

## Journal and response-cache guarantees

The journal is mode `0600` beneath a mode `0700` per-run directory. Each HTTP
attempt or reuse is one append-and-fsync JSON record. Records contain sanitized
resource identities, timings, PID, stage, endpoint family, attempt number,
retry/fallback class, status or typed error, response length/hash, duration and
disposition. URLs, headers, credentials, query strings and command lines are not
recorded. Governed roster redirects additionally record sanitized source and
destination targets and the destination SHA-256; credentials, arbitrary query
data, and headers remain excluded.

## Governed roster redirect policy

Automatic redirect following remains disabled globally. Only a `307` or `308`
from `https://api-web.nhle.com/v1/roster/TEAM/current` may create one destination
attempt under the same logical operation. The normalized destination must be
HTTPS on exactly `api-web.nhle.com`, use no nonstandard port, credentials,
query, or fragment, retain the same three-letter team, and have the exact path
`/v1/roster/TEAM/YYYYYYYY`, where `YYYYYYYY` is the official eight-digit season
ID (repository season `2026` maps to `20262027`). Relative locations are
normalized before validation. Missing or malformed locations, other statuses,
foreign hosts, downgrade, team or season drift, loops, and a second hop fail
closed.

The redirect response and destination request are separate sequential
`NETWORK_ATTEMPT` records with one logical request ID. The first disposition is
`ALLOWED_REDIRECT`; the second has reason `REDIRECT_FOLLOW`. An allowed redirect
increments redirect and network-attempt counts, but not retry, fallback,
success, or failure counts. A 3xx body is never preserved. Redirect rejection
cannot activate the explicit fallback or any implicit network request.

Authority schedule and boxscore bodies are stored as mode-`0600`,
content-addressed objects. Reuse requires matching endpoint family, date/game
identity, byte length and SHA-256. A missing, partial, wrong-identity or altered
object fails closed and cannot trigger an implicit replacement request.

Completion requires the final accounting summary to reconcile exactly with the
shared journal. A missing journal, mismatched run/game-set identity, unexpected
date/game request or incorrect authority-boundary total blocks publication.

## Expected September 19-shaped seven-game plan after reuse

For seven games involving 12 unique teams (two teams repeat), the normal
no-retry, no-fallback plan is below. It is not a universal ceiling: a slate
with 14 unique teams requires 14 roster network responses instead.

| Endpoint family | Logical operations | Network attempts | Preserved reuses |
|---|---:|---:|---:|
| Schedule | 3 | 1 | 2 |
| Boxscore | 28 | 7 | 21 |
| Roster | 14 | 12 | 2 |
| Shift chart | 7 | 7 | 0 |
| Play-by-play | 7 | 7 | 0 |
| **Total** | **59** | **34** | **25** |

Player-landing lookups, roster fallbacks, HTTP retries and failed attempts add
explicit journal records. They are not hidden inside the baseline.

For the verified September 20 evidence, the 59-operation base has 31 authority-
source reuse events and 14 roster-source reuse events. Seven shift-chart and
seven play-by-play operations require network access. The local roster/boxscore
inventory contains 280 participating IDs and 548 fully named roster IDs; 54
participants are absent from those current rosters, 53 have exact audited
external-ID mappings, and only NHL ID `8484537` requires an authoritative
conditional lookup. The evidenced topology is therefore 60 logical operations,
45 preserved-response reuses, and 15 new network operations. These totals are a
verified slate-specific preflight, not a forced universal expectation.

## September 19 historical boundary

The completed September 19 package remains immutable. Its recorded count of
eight covered only the authority-fetch boundary: one schedule and seven
boxscores. Child-process attempts were not durably journaled, so the complete
historical network-attempt count is `UNRECOVERABLE`; source reconstruction
proves the following minimum logical call graph:

| Historical stage | Minimum logical operations | Historical attempts |
|---|---:|---|
| authority schedule | 1 | 1 recorded |
| authority boxscore | 7 | 7 recorded |
| schedule ingestion | 1 | `UNRECOVERABLE` |
| roster collection (two teams per game) | 14 | `UNRECOVERABLE` |
| goalie boxscore collection | 7 | `UNRECOVERABLE` |
| skater schedule discovery | 1 | `UNRECOVERABLE` |
| skater roster-alignment and stats boxscores | 14 | `UNRECOVERABLE` |
| shift charts | 7 | `UNRECOVERABLE` |
| play-by-play | 7 | `UNRECOVERABLE` |
| conditional player-landing lookups | 0 proven; actual unknown | `UNRECOVERABLE` |
| PP-TOI boxscore helper | 0 (dormant DB-overlap branch) | 0 |
| **proved minimum** | **59** | **complete total `UNRECOVERABLE`** |

Roster season fallbacks and transport retries may have raised the total. The
retained logs do not preserve attempt-level evidence capable of resolving them.
This accounting limitation does not alter the verified
game outcomes, lane grades, substantive identity or manifest.

Immutable evidence:

- package identity: `75d3f2ec2d50f53ee79232954f6e5d0de1a97c4dc23d21994bfea0c247d0ce86`
- `SHA256SUMS`: `324d75c116fc540962f7155861958b45d474e75b98f48fe173f2f16a8a0c6c23`
- `RUN_COMPLETE.json`: `126a7d99365c8a7c070c07bae3da200ff986ccac44a8a825a6a0040a47d6dc08`

Future completed reconciliation packages must contain
`official_request_journal.jsonl` and `official_request_accounting.json`, both
covered by the package manifest.

## Typed multi-source response reuse

A later execution may use repeatable `--response-source ROLE=RUN_ID` arguments.
The typed ledger permits `AUTHORITY_RESPONSE_SOURCE` only for the exact schedule
and canonical boxscore identity set and `ROSTER_RESPONSE_SOURCE` only for its
verified terminal roster identities. Before a request run is created or any
database or network access occurs, the reconciler verifies each source journal,
tree fingerprint, canonical game-set hash, endpoint family, resource identity,
cache index, object name, byte length, object SHA-256, and journal response
record. Missing, extra, overlapping, wrong-family, conflicting, or altered
evidence fails closed.

For September 20 the authority source is the original run's eight objects and
the roster source is the third run's 14 terminal roster objects. The second
run's rejected 307 and the third run's 250 unpreserved player-landing responses
are not reusable. A declared reusable identity cannot fall through to network.

Reuse has no implicit network fallback. The new journal records one
`PRESERVED_RESPONSE_REUSE` event for each authority operation, including the
source role and run ID, source-journal SHA-256, index SHA-256, object SHA-256, and a
cross-run marker. The completed package records the source run and complete
response-set lineage. The failed source run remains byte-for-byte unchanged.

## Multi-ancestor request lineage

When more than one failed execution precedes completion, response sources remain
explicit through repeatable typed `--response-source` arguments; every failed
execution is supplied separately through repeatable `--lineage-request-run-id`.
Before creating a new request run or accessing the database or network, the
reconciler verifies each allowlisted receipt: run and slate identity, journal
SHA-256, full request-tree fingerprint, canonical game-set hash, response
indexes, objects, lengths and hashes, and the reuse-chain relationship.
Duplicate ancestors, cycles, altered evidence, and game-set mismatch fail
closed.

For September 20 the original eight-response run is both an authority source
and a failed ancestor; the third run is both a 14-response roster source and a
failed ancestor. The second run is lineage evidence only: its nine reuse records
must resolve to the original objects and its roster 307 remains non-reusable.
A completed package records both response-source roles and all three distinct
failed-ancestor roles, their hashes and relationships, plus the completed
execution's run ID, journal hash, and game-set hash. A run may appear once in
each of the independent source and failed-ancestor collections without forming
a cycle. The standalone redirect diagnostic is out-of-band accounting, not a
request-run ancestor or fabricated journal record.

## Player identity resolution

Collectors bind NHL external IDs through one shared transaction-safe resolver.
It inserts with conflict-safe SQL inside a savepoint, then reads and verifies
both unique dimensions: `(player_id, provider)` and
`(provider, provider_player_id)`. An exact mapping is idempotent. Either conflict
direction fails closed without overwrite, and an unexpected SQL exception rolls
back the savepoint before the surrounding transaction can continue. Names,
including abbreviated names, never authorize merging distinct NHL IDs.
