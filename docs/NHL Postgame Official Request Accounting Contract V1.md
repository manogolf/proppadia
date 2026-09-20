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
- `failed_attempt`: a transport exception or non-success HTTP response.
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
| `refresh_players_and_roster_today.fetch_roster` | roster/team | yes | up to seven attempts; season roster is fallback after current roster | distinct endpoint; repeated team/date use within the same slate reuses its verified response |
| `refresh_players_and_roster_today.fetch_player_name_strict` | player landing/player | conditional | up to seven attempts | distinct endpoint; only for missing names |
| `seed_goalie_logs_for_date.game_ids_from_api` | schedule/date | DB-empty fallback only | up to six attempts | governed reconciliation reuses authority schedule |
| `seed_goalie_logs_for_date.fetch_boxscore_api` | boxscore/game | yes | standalone up to six attempts | governed reconciliation reuses authority boxscore |
| `seed_skater_logs_for_date.get_schedule` | schedule/date | yes | standalone up to six attempts | governed reconciliation reuses authority schedule |
| `seed_skater_logs_for_date.refresh_roster_status_from_box` | boxscore/game | yes | standalone up to six attempts | governed reconciliation reuses authority boxscore |
| `seed_skater_logs_for_date.get_boxscore` | boxscore/game | yes | standalone up to six attempts | governed reconciliation reuses authority boxscore |
| `seed_skater_logs_for_date.ensure_player_exists` | player landing/player | conditional | up to six attempts | distinct endpoint; only if the official boxscore lacks a usable name |
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
recorded.

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
