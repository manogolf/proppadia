# MLB 2026 regular-season close and postseason data plan V1

Status: **PREPARED_AND_OFFLINE_VALIDATED; NOT ACTIVATED; REGULAR SEASON NOT CLOSED**
Contract: `MLB_2026_REGULAR_SEASON_CLOSE_AND_POSTSEASON_DATA_PLAN_V1`
Prepared: 2026-09-21

This package changes no model, prediction, threshold, publication, wager, live database, paid acquisition, or scheduler. It defines the transition, provides a fail-closed classifier and validator, and prepares—but does not execute—the close command and schema migration.

## Governing decisions

1. Authoritative source game type, never calendar date, determines phase.
2. The only normalized phases are `PRESEASON`, `REGULAR_SEASON`, and `POSTSEASON`. Recognized special games such as All-Star have no phase and are excluded fail closed.
3. `POSTSEASON` always has a separately retained round. Legacy generic `P`/`C` types use an explicit `*_UNSPECIFIED` round; they are never guessed from dates.
4. Exact MLB `gamePk` is the canonical identity. Date/team fallback is not authoritative for doubleheaders, reschedules, or resumed games.
5. Canonical game tables preserve raw type and normalized phase once. Other lanes derive phase through exact canonical identity unless an immutable payload already preserves the source value. No blanket downstream column duplication is required.
6. Missing, unknown, or conflicting authoritative type blocks admission, grading membership, reporting certification, and close.

## Source semantics and retained evidence

The field inventory is [game_type_field_manifest.csv](game_type_field_manifest.csv). It was generated from retained local bytes, not a live request.

- MLB StatsAPI schedule uses `dates[].games[].gameType`; game feeds expose the equivalent at `gameData.game.type`. The retained 2026 schedule subset contains 79 `R` rows. It also contains 78 `Final` detailed states and one `Postponed` row.
- Retrosheet uses `gameinfo.csv.gametype`. Retained exact values are `regular`, `Regular`, `exhibition`, `wildcard`, `divisionseries`, `lcs`, `worldseries`, `championship`, `playoff`, and `allstar`.
- Retrosheet's `suspend` is a completion/resumption date, not a phase. There are 203 populated retained rows. It does not change `gametype`.
- The retained StatsAPI schedules contain `rescheduledFrom` on three games and `rescheduleDate` on one postponed game. Those relationship fields describe disposition; they do not change `gameType`.

Source identities used for this audit:

| Retained source | SHA-256 |
|---|---|
| `schedule_2026-03-26_2026-07-21.json` | `6cf64c0cf2f363b7aa144dd058f3d7dbd7853e833c1122be104dbfee9851acc1` |
| `schedule_2026-07-23_2026-07-26.json` | `e3906af52a533ddc608fed3f238c98f1aca8373364b5626ecffc5f944c1929e6` |
| `schedule_2026-07-27_2026-07-27.json` | `e18020098eac2675c50bbec4e722d339525b25a2a0af0b4896fe272d80dbcb75` |
| Retrosheet `gameinfo.csv` | `ce7fca848a56d411716781bf90bb4b307354cd0b67c6164872729ad2adb34351` |
| Retrosheet release manifest | `092db3293caca92b889a6ed31af8ac9bb464d5b1c393d08d997358438b211b96` |

### Identity behavior

- A postponed game's original `gamePk` remains its disposition identity. A replacement/makeup game may have its own `gamePk`; preserve both and the raw `rescheduledFrom`/`rescheduleDate` relationship.
- A suspended/resumed game retains its authoritative source identity when the source retains the same `gamePk`. Never manufacture continuity from matching teams and date.
- `officialDate`, scheduled start, and actual resume date are attributes, not phase inputs.
- Season names are `MLB_<season>_<phase>`, for example `MLB_2026_REGULAR_SEASON`. Season is an authoritative source/config value, not the year parsed from a played date.
- Status and phase are orthogonal. The retained game `824490` is `gameType=R`, `detailedState=Postponed`, and has a reschedule date. It remains regular season.

## Frozen phase contract

| Raw StatsAPI type | Phase | Round / treatment |
|---|---|---|
| `S`, `E`, `I` | `PRESEASON` | No postseason round |
| `R` | `REGULAR_SEASON` | No postseason round |
| `F` | `POSTSEASON` | `WILD_CARD` |
| `D` | `POSTSEASON` | `DIVISION_SERIES` |
| `L` | `POSTSEASON` | `LEAGUE_CHAMPIONSHIP_SERIES` |
| `W` | `POSTSEASON` | `WORLD_SERIES` |
| `P` | `POSTSEASON` | `PLAYOFFS_UNSPECIFIED` |
| `C` | `POSTSEASON` | `CHAMPIONSHIP_UNSPECIFIED` |
| `A`, `N` | none | Known special game; excluded fail closed |
| missing, unknown, conflicting | none | Contract error; excluded fail closed |

The executable authority is `backend/mlb/season_transition/contract_v1.py`.

## End-to-end audit and correction boundary

The detailed classifications are in [end_to_end_propagation_audit.csv](end_to_end_propagation_audit.csv), and operational status is in [lane_readiness.csv](lane_readiness.csv).

The live read-only schema inspection on 2026-09-21 confirmed that `mlb.game_info`, `mlb_cleanroom_v1.games`, immutable Moneyline prediction/outcome tables, and `mlb.bvp_stats` have no authoritative phase fields. Consequently, the overall transition is **not ready for postseason activation**.

Corrections and activation steps required before postseason processing:

1. Apply the prepared canonical schema migration only after a dry-run and backup/recovery review.
2. Review and transactionally activate the source-hashed offline backfill proposal. Producer source now populates canonical fields, uses named game inserts, and fails closed on partial schema or invalid type, but neither migration nor backfill has been applied.
3. Remove `row.get("game_type") or "R"` in the Full-board Hits scorer and every `fillna("R")`/`COALESCE(...,'R')` phase assumption in research/evaluation.
4. Phase-gate Moneyline, RAW Totals, Totals C, and graders using exact canonical `gamePk`; preserve postseason separately.
5. Replace `run_mlb_market_strong_agreement_separation_prospective_v1.late_season_regime`, which currently infers postseason from October/November, with canonical source phase.
6. Add explicit regular/postseason partitions to the daily report, Ops Brief, indexes, and exports.

The prepared migration is `backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql`. It is intentionally not applied by this task.

## Regular-season close specification

The close package must freeze:

- exact canonical regular-season population and disposition;
- immutable Moneyline predictions, outcomes, and proper scores;
- RAW Totals, Totals C, and Full-board Hits evidence;
- BvP acquisition/identity and feature-lineage health;
- Pinnacle, BetOnline, other market coverage, and agreement-study progress;
- request/credit accounting;
- all outstanding unresolved rows;
- model qualification, publication, and wagering status;
- source/config identities and SHA-256 manifests.

### Superseded close authorization design

The caller-supplied inventory and static-token design below was superseded by
`MLB_2026_AUTHORITATIVE_REGULAR_SEASON_CLOSE_INVENTORY_V1`. It is retained as
historical design context only and is not an executable close interface.

The original design returned `REGULAR_SEASON_CLOSE_AUTHORIZED` only when all of the following were true in one frozen inventory:

1. Every canonical row has authoritative `source_game_type=R` and normalized `season_phase=REGULAR_SEASON`.
2. Every canonical row is `FINAL`, `CANCELLED`, `POSTPONED_AUTHORITATIVELY_DISPOSED` with a source reference, or `EXPLICITLY_UNRESOLVED` with a reason and operator acknowledgement. Any scheduled, preview, in-progress, delayed, or suspended/resume-pending row blocks close, regardless of nominal date.
3. Canonical game identities are unique.
4. Every required lane is present and both its manifest and ledger pass.
5. Every lane reports zero ungraded eligible predictions, duplicate prediction identities, duplicate outcome identities, post-start violations, outcome-leakage violations, and postseason rows in regular outputs.
6. Every required freeze section is populated (the unresolved-row list may validly be empty), and every source/config identity has a lowercase hexadecimal SHA-256.
7. Model promotion, publication, and wagering remain false.

Postseason rows can coexist in storage but cannot appear in the frozen regular population or regular reports.

### Current check-only command

The command accepts no arguments and reads only the pinned, source-hashed
canonical inventory. It always remains check-only and cannot write a close
package:

```bash
PYTHONPATH=. .venv/bin/python -m backend.mlb.scripts.prepare_mlb_2026_regular_season_close_v1
```

`backend/mlb/season_transition/close_inventory_template_v1.json` and the old
authorization-token narrative are also historical only. There is no execution
flag, caller-supplied population, static token, output directory, or close
package writer in the current command. Actual closure requires a separate,
future authorization and implementation.

## Fixed late-season reporting cohorts

These cohorts do not affect predictions or thresholds:

| Cohort | Membership |
|---|---|
| `ORDINARY_REGULAR_SEASON` | `season_phase=REGULAR_SEASON` and official date before 2026-09-01 |
| `SEPTEMBER_LATE_SEASON_REGIME` | `season_phase=REGULAR_SEASON` and official date on/after 2026-09-01 |
| `ELIMINATED_VS_CONTENDING_STRICT_PRIOR` | Late-season row where both teams' eliminated/contending status is from an authoritative observation strictly before scheduled start |
| `LATE_CONTEXT_UNRESOLVED` | Required strict-prior competitive status is unavailable or conflicts |
| `FINAL_REGULAR_SEASON_TOTAL` | All and only `season_phase=REGULAR_SEASON` rows |

Calendar date may define a reporting subcohort only after source type proves regular-season membership. It never determines phase. Rescheduled `R` games played after the nominal end stay in the final regular-season total and block close while active.

No late-season-only cohort may qualify a model. Qualification requires the unchanged full regular-season evidence rules and a separately reported ordinary-season base; late-season rows are context/sensitivity evidence.

## Postseason collection and reporting plan

Existing daily lanes may continue for postseason games only when their present inputs and identity gates remain valid. Preserve, by exact game identity:

- schedule, raw type, normalized phase, round, series description, and raw reschedule/resume relationships;
- rosters, confirmed lineups, and probable/confirmed starters with observation times;
- BvP and player events;
- Moneyline, RAW Totals, Totals C, and Full-board Hits immutable predictions/outcomes;
- player-prop and main-market observations, feature lineage, and canonical outcomes;
- strictly-prior rest days, series score, elimination status, starter rest, and bullpen workload.

Series score and elimination status must carry `observed_at_utc < scheduled_start_utc`; otherwise they are unresolved and excluded. Postseason ledgers may reuse existing physical storage only when canonical phase is unambiguous. Default reports must be postseason-only and round-separated. They must never be pooled into regular-season certification, evidence floors, qualification, or publication.

No new paid acquisition is authorized. Pinnacle/BetOnline/other captures remain limited to already authorized routine calls. No threshold change, promotion, publication, wagering, or 2027 policy decision is authorized.

## 2027 future-use contract

Eligible for possible strict-prior feature history after a separate 2027 decision:

- batter/pitcher events and BvP appearances;
- pitching/batting skill and contact components;
- workload history;
- injuries, participation, and roster history.

Requires regression or contextual treatment:

- team strength and run differential;
- bullpen/starter workload;
- lineup strength;
- home field and rest effects.

Postseason-evaluation-only by default:

- postseason model accuracy/calibration;
- market agreement, ROI, and line movement;
- series/elimination strategy effects.

The strict-prior feature builder may see a postseason player event only when its event timestamp precedes the 2027 feature as-of time. That does not grant the row membership in any 2026 regular-season evaluation.

## Scheduler and offseason safety

Observed source behavior:

- The API/UI offseason contract correctly treats a successful zero-game schedule as healthy.
- The main `mlb_prod12_cron_cycle.sh` is not offseason-safe for cost control: it calls roster, BvP, stat-derived, wide-prediction, and artifact stages without a preceding authoritative zero-slate/phase gate; wide predictions default to a minimum of one row.
- Moneyline, Totals, Hits, main-market, and BvP sidecars do not share one no-game/phase policy. The SportsGameOdds main-market hook documents one request per invocation and has no zero-game preflight in its wrapper.
- Therefore no-game days, days between postseason series, the day after the World Series, and offseason dates can produce empty-row failures and/or unnecessary requests. Schedules must remain unchanged until the future transition is approved.

Prepared future modes:

1. `ACTIVE_POSTSEASON`: free authoritative schedule preflight; run an existing lane only for exact `POSTSEASON` games and only where its input is valid.
2. `NO_GAME_DAY`: successful empty schedule is healthy; no paid market/player-prop call; grading/reconciliation may run for unresolved prior games.
3. `OFFSEASON`: suppress all paid acquisition and prediction generation; retain only useful free health/status checks and unresolved reconciliation.

### Exact offseason authorization condition

Offseason mode is authorized only after all are true:

1. The authoritative World Series game population is final/cancelled/authoritatively disposed with no suspended, resumed-pending, postponed-pending, or ungraded eligible postseason identity.
2. Every postseason lane manifest/ledger and request-credit account passes, including outstanding corrections.
3. The postseason closeout package and SHA-256 manifest exist.
4. A free authoritative schedule check finds no active MLB Major League `R`, `F`, `D`, `L`, `W`, `P`, or `C` game requiring collection or grading; known specials remain excluded.
5. An operator explicitly enables `OFFSEASON`; the day after the nominal World Series date alone is insufficient.

The future implementation should gate paid calls after one free schedule/status check, without removing LaunchAgents. Restart for 2027 requires an authoritative `S`/`E` preseason slate or `R` regular slate, a successful identity/credential preflight, explicit operator transition to active mode, and separate authorization for any paid routine. Document the exact environment flags and rollback before changing a schedule. No LaunchAgent or schedule was changed here.

## Deterministic validation

Run:

```bash
PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python -m backend.mlb.scripts.validate_mlb_2026_season_transition_v1
PYTHONPATH=. /Users/jerrystrain/Projects/proppadia/.venv/bin/python -m pytest -q backend/mlb/tests/test_mlb_2026_season_transition_v1.py
```

The fixture suite covers regular-only semantics, mixed regular/postseason storage, every modern postseason round, retained postponed/rescheduled and suspended/resumed regular games, unknown/conflicting types, date-classification traps, an early close attempt, postseason exclusion from regular reports, and strict-prior future feature availability without evaluation contamination.

## Deliverable index

- Game-type field manifest: `game_type_field_manifest.csv`
- End-to-end propagation audit: `end_to_end_propagation_audit.csv`
- Frozen phase contract and deterministic validator: `backend/mlb/season_transition/contract_v1.py`
- Regular-season close specification and runbook: this document
- Prepared close command: `backend/mlb/scripts/prepare_mlb_2026_regular_season_close_v1.py`
- Late-season, postseason, future-use, and scheduler plans: this document
- Historical/transition fixtures: `backend/mlb/season_transition/fixtures/phase_and_close_cases_v1.json`
- Close inventory template: `backend/mlb/season_transition/close_inventory_template_v1.json`
- Readiness by lane: `lane_readiness.csv`
- Prepared schema: `backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql`
- Package integrity: `sha256_manifest.txt`
