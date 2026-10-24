# Proppadia — Project Brief

- As of: 2026-10-18 (repo commit `fbd87a74`)
- Purpose of this file: a self-contained reference for big-picture,
  architecture, and feasibility discussions held outside the codebase (e.g. a
  claude.ai Project). It summarizes; the linked repo docs remain authoritative.
- Refresh when: the architecture, data sources, production model authority, or
  operating model changes. Sections 10–11 are dated snapshots and go stale
  fastest.

## 1. What Proppadia is

Proppadia is a single-operator sports-prediction platform for **MLB and NHL**.
It acquires official stats and sportsbook odds, stores them in Postgres,
builds features, trains models, and produces daily predictions for player
props (and some game markets such as moneyline and totals). Predictions are
compared against sportsbook prices, graded after games finish, and surfaced
through a web app (proppadia.com), operator reports, and pick-upload CSVs for
an external pick platform ("8rain").

It operates as a **research lab with production discipline**: heavy emphasis
on data provenance, restricted database access, frozen evaluation contracts,
and champion/challenger governance. There is no documented monetization or
subscription model; the practical user is the operator.

Two non-negotiable modeling rules shape everything:

- **Prediction purity:** model predictions must not use sportsbook/line
  inputs as features; book lines are used only for comparison and pricing
  (`docs/Cross-Sport Product Plan.md`).
- **`0` vs `NULL`:** `0` is a verified observed zero; `NULL` is unknown. Absent
  events are never silently counted as zero
  (`docs/contracts/missingness_and_zero_semantics.md`).

## 2. System at a glance

```
 External sources                    Local Mac (launchd schedules)
 ───────────────                     ────────────────────────────────────────────
 MLB StatsAPI, NHL APIs  ──┐
 Retrosheet, pybaseball  ──┤  acquisition jobs          feature builders / models
 The Odds API (credits)  ──┼──► (bin/*.sh, Makefile, ──► (backend/mlb, backend/nhl)
 SportsGameOdds          ──┤     backend/nhl/cli.py)              │
 Bookmaker.eu, Pinnacle  ──┘            │                         │ predictions
                                        ▼                         ▼
                         Supabase Postgres 15  ◄──────── grading / reconciliation
                         schemas: mlb.*, nhl.*           (postgame, append-only)
                         restricted roles per job
                                        │
            ┌───────────────────────────┼──────────────────────────────┐
            ▼                           ▼                              ▼
   FastAPI on Render            Local artifacts (~tens of GB)   Upload CSVs
   /api/*, /api/nhl/*,          ops briefs, daily index,        (8rain pick
   /api/ops/* (token)           research reports, CSVs          platform)
            │
            ▼
   React/Vite site on Vercel (proppadia.com), Supabase auth
```

Model bundles are trained locally and published to a persistent disk on the
API host (with a storage backup), activated by symlink swap
(`docs/MLB Prod12 Runbook.md`).

## 3. Tech stack and hosting

| Layer | Technology | Where |
|---|---|---|
| Backend / pipelines | Python 3.11, FastAPI, uvicorn, psycopg 3, pandas, numpy, scipy, scikit-learn 1.6.1, httpx, pybaseball | `requirements.txt`, `backend/` |
| Database | Supabase Postgres 15 (plus Supabase auth/storage) | schema dump `backups/pre_pa_schema.sql` |
| Frontend | React 19, Vite 7, react-router, Tailwind 3, Recharts, supabase-js | `frontend/` (deployed on Vercel) |
| API hosting | Render web service with persistent model disk | `docs/MLB Prod12 Runbook.md` |
| Scheduling | macOS launchd on the operator's Mac; hosted GitHub cron is **retired** | `docs/README.md`, `backend/*/launchagents/` |
| Command surface | `Makefile` (~400 targets, ~300 KB), `bin/` shell wrappers, `backend/nhl/cli.py` | repo root |
| Python entrypoint | always `./bin/python` | `AGENTS.md` |
| Tests | pytest; ~330 test files across `backend/tests`, `backend/mlb`, `backend/nhl` | |

## 4. Repository layout

| Path | Role |
|---|---|
| `backend/app/` | FastAPI runtime (`api_server.py`, `routers/`) |
| `backend/domains/{mlb,nhl}/` | Repository/domain logic used by the API |
| `backend/mlb/` | MLB pipelines: acquisition, precompute, prediction, totals, research, SQL migrations |
| `backend/nhl/` | NHL pipelines: CLI, orchestration, features, models, shadow lanes, SQL migrations |
| `backend/shared/` | Shared DB/runtime helpers (connection routing, role handling) |
| `backend/scripts/`, `backend/tests/` | Smoke/contract checks and tests |
| `backend/_legacy/` | Quarantined old code (`docs/Legacy Quarantine Map.md`) |
| `frontend/` | Web app |
| `bin/` | Operational shell entrypoints and `bin/python` |
| `docs/` | Doctrine, contracts (~43 in `docs/contracts/`), runbooks, backlogs, NHL evaluation reports |
| `artifacts/` | Generated ops and research outputs (reports partly tracked; large data outputs ignored and kept local) |
| `models_out/`, `exports/` | Model bundles and training exports |
| `nhl/site/` | Static NHL data served by the API |
| `archive/` | Retired frontend and workflows |

Size: ~6,100 tracked files; ~1,500 commits from 2025-09-25 to 2026-10-18
(MLB code originated in an earlier "baseball-streaks" repo).

## 5. Data sources

| Source | Sport | Supplies | Notes |
|---|---|---|---|
| MLB StatsAPI | MLB | Schedules, games, box scores, rosters | Official identity source (numeric `gamePk`, player IDs) |
| Retrosheet | MLB | Historical play-by-play/game history | Backfills |
| pybaseball / Baseball Savant / FanGraphs | MLB | Statcast and derived stats | Minor usage |
| NHL web/stats APIs | NHL | Schedules, box scores, play-by-play, shift charts, rosters | Official identity source |
| The Odds API | Both | Player-prop and game odds snapshots | **Credit-metered**; routine collection is not throttled, but large historical backfills require spend review first (`AGENTS.md`) |
| SportsGameOdds | Both | Main-market odds (trial lane) | |
| Bookmaker.eu, Pinnacle | MLB | Additional price capture for market comparison | Raw collection kept separate from evaluation |

Doctrine requires new features and acquisition methods to be verified against
official source observations, with raw provider responses and provenance
retained (`docs/PROJECT_DOCTRINE.md`, `docs/CANONICAL_IDENTITY_DOCTRINE.md`).

## 6. Database

- **Schemas:** `mlb.*` (~15 tables) and `nhl.*` (~70 tables) in one Supabase
  Postgres instance, plus Supabase's own auth/storage schemas.
- **Key MLB tables:** `player_props`, `model_training_props`,
  `prop_features_precomputed`, `player_stats`, `player_derived_stats`,
  `bvp_stats`, `game_info`, `today_*` materialized views,
  moneyline prediction/outcome tables, `schema_migration_ledger_v1`.
- **Key NHL tables:** `games`, `players`, `predictions`,
  `skater_game_logs_raw`, `goalie_game_logs_raw`, `shiftcharts_*`,
  `shot_on_goal_events`, `training_features_*`, `eval_sog_daily`.
- **Migrations:** `backend/mlb/sql/migrations/` (~49, many with rollbacks) and
  `backend/nhl/sql/migrations/` (~12).
- **Access model (central constraint):** routine jobs use restricted,
  purpose-specific roles — never owner-level access, even to work around a
  failure.
  - MLB: ingest via `SET ROLE` to a restricted ingest role with row-level
    security; separate read-only research role (`backend/shared/db/pg.py`).
  - NHL: an access map routes each job to its own login/role (ingest, ops
    writer, source correction, research) and rejects mismatches
    (`backend/nhl/database_access.py`).
  - Populated historical source values are protected; corrections go through
    a separate authorized procedure.
- Large research CSV/parquet outputs live on local disk, not in the database.

## 7. MLB pipeline

- **Markets:** "Prod12" over/under player props — hits, total_bases,
  strikeouts_batting, earned_runs, doubles, hits_allowed, strikeouts_pitching,
  walks, hits_runs_rbis, runs_scored, walks_allowed, rbis. Watchlist:
  outs_recorded, home_runs, runs_rbis. Additional lanes: full-game totals,
  public game moneyline (shadow), Hits 0.5 full-board shadow, SportsGameOdds
  main-market trial.
- **Daily chain** (`bin/proppadia_mlb_refresh_daily.sh`): roster refresh →
  stat-derived refresh → integrity check → predictions-wide → slate output →
  book upload → Ops Brief / Daily Index → health gates.
- **Models:** per-prop RandomForest with isotonic-calibrated logistic
  regression (`backend/mlb/model_trainer.py`), weekly retrain over ~3 years of
  history. Gates check prediction success, coverage, minimum accuracy, and a
  runs_scored balance guard.
- **Game-phase authority:** every evaluation is partitioned by exact game ID
  into regular season / postseason / preseason; postseason is shadow-only.
- **Outputs:** daily MLB Ops Brief (`artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md`),
  candidate CSVs, Quick Card, upload files.

## 8. NHL pipeline

- **Markets:** shots on goal (SOG), points, goalie saves, moneyline; puck line
  is shadow-only.
- **Command surface:** `backend/nhl/cli.py` (`daily`, `fetch-odds`,
  `refresh-rosters-all`, `build-sog`, `build-saves`, `build-points`, `guard`)
  and morning orchestration (`backend/nhl/scripts/run_nhl_morning_orchestration.py`,
  launchd at 07:30), plus postgame reconciliation (`bin/nhl_postgame_reconcile.sh`).
- **Daily order (contracted):** prior-day reconciliation must be certified
  before schedule → roster → features → predictions → pregame odds observers
  (`docs/NHL Comprehensive Daily Capture Contract V1.md`).
- **Model authority (Oct 2026):**
  - Moneyline: `..._SHOT_FINISHING_CHALLENGER_V3` production since Oct 14;
    V2 logit control as shadow.
  - SOG: Poisson baseline (D10) production; D10/D20 blends under evaluation.
  - Points: HistGradientBoosting count model production; `phoenix_v2` shadow.
  - Saves: `phoenix_v2`, conditional on goalie start.
  - Puck Line: baseline + challenger, both shadow.

## 9. Serving layer

- **API** (`backend/app/api_server.py`): `/api/health`, MLB `/api/*`,
  NHL `/api/nhl/*`, operator `/api/ops/*` gated by an ops token header; static
  NHL data mounted from `nhl/site/data`.
- **Web app** (`frontend/src/routes/AppRouter.jsx`): `/mlb/slate`,
  `/mlb/predictions`, `/nhl/slate`, `/nhl/props`, `/nhl/predictions`, player
  profiles, `/metrics`, `/watchlist`, `/ops`, login via Supabase auth.
- **Two planes** (`docs/Cross-Sport Product Plan.md`): a user plane that is
  consistent across sports, and an isolated operator plane (book upload,
  publish, scheduling controls).
- **External export:** pick-upload CSVs for the 8rain platform
  (`backend/nhl/eightrain_adapter.py`).

## 10. Modeling and evaluation discipline

- **Research lifecycle:** exploratory → research-active → operational-research
  → production-candidate → production; each step requires automation, health
  checks, docs, and visibility (`docs/PROJECT_DOCTRINE.md`).
- **Feature completeness:** a feature is not done until daily generation keeps
  it correct and regressions are detected — "backfilled data is not finished
  data."
- **Champion/challenger:** frozen baselines, pre-declared challengers,
  promotion classification
  (`docs/model_development/champion_challenger_experiment_specification.md`).
- **Evidence standard:** accuracy records are descriptive, not wager
  histories; ROI claims require bound quotes, a defined side, and a staking
  rule (`docs/model_development/evidence_and_claim_discipline.md`, proposed).
- **Metrics:** Brier score and log loss first; AUC, MAE for counts; shadow
  lanes and frozen prospective contracts for out-of-sample checks.

## 11. Current status snapshot (2026-10-18)

- **MLB:** regular season close is blocked pending 88 scheduled-not-final
  games; postseason is shadow-only. The Ops Brief reports
  `NO_QUALIFIED_MLB_PROP_MODEL` and `NO_QUALIFIED_MLB_BETTING_MODEL` — no MLB
  prop or moneyline model currently holds betting/selection authority.
  Historical lane audits show negative ROI in the main prop segments (e.g.
  full-history hits_runs_rbis −5.9%, runs_scored −3.1%;
  `artifacts/analysis/mlb/prop_family_lane_weakness_audit_summary.md`).
- **NHL** (`docs/nhl/2026_prospective_performance_evaluation_2026-10-17.md`):
  - Moneyline V3, 89-game paired priced study: 66.3% vs market favorite 61.8%;
    Brier 0.2225 vs market 0.2279; descriptive one-unit ROI +13.3%. The report
    states this is in-sample descriptive and **does not establish a durable
    edge**; it calls for ≥50 more paired games.
  - Puck Line: both models score worse than a uniform baseline.
  - SOG/Points/Saves: high selected-side win rates are driven by UNDER-heavy
    distributions; no ROI is supported because market coverage is incomplete.
- **Recent engineering focus:** NHL daily-run integrity (reconciliation-first
  ordering, final-pregame capture), MLB restricted-writer cutover and
  postseason game-phase authority, and NHL evaluation research.

## 12. Constraints, risks, and open questions (feasibility lens)

- **Edge is unproven.** The only positive priced result (NHL moneyline) is a
  small, in-sample window; MLB props are currently unqualified and
  historically negative.
- **Single operator, single machine.** Scheduling runs on a local Mac via
  launchd; the operator is also the primary user.
- **Operational overhead is high relative to output:** ~400 Make targets,
  ~43 MLB phase/postseason contracts, hundreds of dated run-log folders, tens
  of GB of local artifacts, and most recent commits are governance, contracts,
  and receipts rather than model improvements.
- **Provider cost:** odds acquisition is credit-metered; no monthly budget
  figure is documented.
- **Stale docs:** `docs/As Built Snapshot.md` and `docs/Project Map.md` date
  from Feb 2026; `frontend/README.md` is template boilerplate; the MLB Prod12
  runbook still calls `.venv/bin/python` instead of `./bin/python`.
- **Open backlog themes:**
  - NHL (`docs/NHL Backlog.md`): credential rotation and historical PP-TOI
    correction status unverified; natural-run acceptance of pregame observers,
    roster coverage, and per-game final-pregame evidence.
  - MLB (`docs/MLB Post-Phase-6 Backlog.md`): finishing restricted-ingest
    cutover, BvP restricted validation, owner-alias replacement, API-role
    permission review, postseason game-phase authority.
- **No business model** is documented; any product/market discussion starts
  from scratch.

## 13. Where to look

- Rules: `AGENTS.md`, `docs/PROJECT_DOCTRINE.md`,
  `docs/CANONICAL_IDENTITY_DOCTRINE.md`, `docs/contracts/missingness_and_zero_semantics.md`
- Index of all docs: `docs/README.md`
- Product shape: `docs/Cross-Sport Product Plan.md`, `docs/NHL UX Contract.md`
- Operations: `docs/Quick Commands.md`, `docs/Operations Command Matrix.md`,
  `docs/MLB Prod12 Runbook.md`, `docs/NHL Comprehensive Daily Capture Contract V1.md`
- Backlogs: `docs/NHL Backlog.md`, `docs/MLB Post-Phase-6 Backlog.md`
- Results: `docs/nhl/2026_prospective_performance_evaluation_2026-10-17.md`,
  `artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md`
- History: `artifacts/analysis/research/PROJECT_JOURNAL.md` (MLB, through 2026-07)
