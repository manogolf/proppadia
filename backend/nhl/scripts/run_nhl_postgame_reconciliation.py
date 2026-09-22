#!/usr/bin/env python3
"""Operator CLI for governed NHL postgame reconciliation.

Preflight is database-read-only. Execute uses official NHL public sources plus the
existing postgame collectors; it never imports or calls a bookmaker client.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
import requests

from backend.nhl.official_request_journal import (
    ENV_CACHE,
    ENV_GAME_HASH,
    ENV_GAME_IDS,
    ENV_JOURNAL,
    ENV_REQUIRED,
    ENV_RUN_ID,
    ENV_SLATE,
    ENV_SOURCE_CACHE,
    ENV_SOURCE_JOURNAL_SHA256,
    ENV_SOURCE_RUN_ID,
    RequestContext,
    canonical_game_set_hash,
    official_get,
    summarize_journal,
    verify_preserved_response_run,
)
from backend.nhl.postgame_reconcile.core import (
    reconciliation_lock,
    publish_reconciliation,
    resolve_operational_sources,
    validate_final_slate,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DEFAULT_PREDICTION_ROOT = ROOT / "artifacts/operational/nhl/preseason_catchup"
DEFAULT_OPERATIONAL_ROOT = ROOT / "artifacts/operational/nhl"
SCRIPTS = ROOT / "backend/nhl/scripts"
SQL = ROOT / "backend/nhl/sql"
FINAL_STATES = {"FINAL", "OFF"}


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def canonical_slate(dsn: str, slate_date: str) -> pd.DataFrame:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("BEGIN READ ONLY")
        cursor.execute("""
            SELECT season AS canonical_season, game_date::text AS slate_date, game_id,
                   start_time_utc AS scheduled_start_time_utc, home_team_id, away_team_id,
                   home_team_code, away_team_code, game_type AS game_type_code,
                   upper(coalesce(status, 'SCHEDULED')) AS retained_game_state
            FROM nhl.games
            WHERE season=2026 AND game_date=%s::date
            ORDER BY start_time_utc, game_id
        """, (slate_date,))
        rows = cursor.fetchall()
        columns = [item.name for item in cursor.description]
        connection.rollback()
    return pd.DataFrame(rows, columns=columns)


def read_only_preflight(dsn: str, slate_date: str) -> tuple[dict[str, object], int]:
    slate = canonical_slate(dsn, slate_date)
    states = {str(key): int(value) for key, value in slate.retained_game_state.value_counts().items()} if len(slate) else {}
    complete = bool(len(slate)) and bool(slate.retained_game_state.isin(FINAL_STATES).all())
    payload = {
        "contract_version": "NHL_POSTGAME_RECONCILIATION_V1",
        "mode": "PREFLIGHT_READ_ONLY",
        "slate_date": slate_date,
        "canonical_games": len(slate),
        "game_ids": slate.game_id.astype(int).tolist() if len(slate) else [],
        "retained_state_counts": states,
        "all_retained_games_final": complete,
        "official_final_authority_refresh_performed": False,
        "readiness": "READY_FOR_EXECUTE" if complete else "NOT_READY_UNFINISHED_OR_UNAVAILABLE",
        "database_writes": 0,
        "external_requests": 0,
        "bookmaker_requests": 0,
        "paid_credits": 0,
    }
    return payload, 0 if complete else 2


def official_games_for_slate(payload: dict, slate_date: str) -> pd.DataFrame:
    """Flatten only the requested schedule day.

    NHL's schedule response assigns the authoritative date to each ``gameWeek``
    parent.  Child games commonly omit ``gameDate``; a child value must not move
    a game into or out of its authoritative parent day.  The legacy top-level
    ``games`` shape has no parent, so it requires an exact child date.
    """
    rows: list[dict[str, object]] = []
    for day in payload.get("gameWeek", []) or []:
        if str(day.get("date") or "") != slate_date:
            continue
        for item in day.get("games") or []:
            rows.append(item)
    for item in payload.get("games", []) or []:
        if str(item.get("gameDate") or "") == slate_date:
            rows.append(item)

    games = []
    for item in rows:
        raw_id = item.get("id") or item.get("gamePk") or item.get("gameId")
        if raw_id is None:
            raise RuntimeError("OFFICIAL_GAME_IDENTITY_MISSING")
        games.append({
            "game_id": int(raw_id),
            "game_state": str(item.get("gameState") or item.get("gameScheduleState") or "").upper(),
            "home_team_id": int((item.get("homeTeam") or {}).get("id")),
            "away_team_id": int((item.get("awayTeam") or {}).get("id")),
            "home_score": (item.get("homeTeam") or {}).get("score"),
            "away_score": (item.get("awayTeam") or {}).get("score"),
        })
    official = pd.DataFrame(games, columns=[
        "game_id", "game_state", "home_team_id", "away_team_id", "home_score", "away_score",
    ])
    if official.game_id.duplicated().any():
        duplicate_ids = sorted(official.loc[official.game_id.duplicated(False), "game_id"].astype(int).unique())
        raise RuntimeError(f"OFFICIAL_DUPLICATE_GAME_IDENTITY:{duplicate_ids}")
    return official


def fetch_official(slate_date: str, canonical_game_ids: set[int], *, reuse_authority: bool = False) -> tuple[pd.DataFrame, dict[int, dict], dict[str, int]]:
    expected_ids = {int(value) for value in canonical_game_ids}
    response = official_get(
        f"https://api-web.nhle.com/v1/schedule/{slate_date}", timeout=30,
        stage="POSTGAME_AUTHORITY", endpoint_family="SCHEDULE",
        identity={"slate_date": slate_date}, authority_boundary=True,
        preserve_response=not reuse_authority, reuse_preserved=reuse_authority,
    )
    response.raise_for_status()
    official = official_games_for_slate(response.json(), slate_date)
    actual_ids = set(official.game_id.astype(int))
    if actual_ids != expected_ids:
        missing = sorted(expected_ids - actual_ids)
        unexpected = sorted(actual_ids - expected_ids)
        raise RuntimeError(
            "OFFICIAL_GAME_SET_MISMATCH_BEFORE_BOXSCORE_REQUESTS:"
            f"expected={len(expected_ids)}:actual={len(actual_ids)}:"
            f"missing={missing}:unexpected={unexpected}"
        )
    boxes: dict[int, dict] = {}
    for gid in sorted(expected_ids):
        box = official_get(
            f"https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore", timeout=30,
            stage="POSTGAME_AUTHORITY", endpoint_family="BOXSCORE",
            identity={"slate_date": slate_date, "game_id": gid},
            authority_boundary=True, preserve_response=not reuse_authority,
            reuse_preserved=reuse_authority,
        )
        box.raise_for_status()
        boxes[gid] = box.json()
    requests_made = {
        "schedule_requests_expected": 1,
        "schedule_requests_actual": 1,
        "game_requests_expected": len(expected_ids),
        "game_requests_actual": len(boxes),
        "official_requests_expected": 1 + len(expected_ids),
        "official_requests_actual": 1 + len(boxes),
    }
    if reuse_authority:
        requests_made.update({"network_attempts": 0,
                              "preserved_response_reuses": 1 + len(boxes)})
    if requests_made["official_requests_actual"] != requests_made["official_requests_expected"]:
        raise RuntimeError(f"OFFICIAL_REQUEST_COUNT_MISMATCH:{requests_made}")
    return official, boxes, requests_made


def _run(command: list[str], slate_date: str, dsn: str) -> None:
    env = os.environ.copy()
    # Some retained collectors prefer DATABASE_URL while others prefer
    # SUPABASE_DB_URL.  Resolve both to the already validated DSN so a literal
    # shell-style alias from an env file cannot leak into a child process.
    env.update({"SLATE_DATE": slate_date, "SUPABASE_DB_URL": dsn, "DATABASE_URL": dsn})
    if env.get(ENV_REQUIRED) == "1":
        # Fail before spawning a network-capable child if the shared governed
        # context is absent, inconsistent, or has been partially overwritten.
        RequestContext.from_env(required=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def _promote_stage(dsn: str, slate_date: str) -> None:
    """Idempotently promote existing governed skater/goalie stage tables."""
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("""
            WITH latest_roster AS (
              SELECT DISTINCT ON (game_id,player_id) game_id,player_id,team_id
              FROM nhl.roster_status
              WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date=%s::date)
              ORDER BY game_id,player_id,asof_ts DESC
            ), source AS (
              SELECT s.player_id,s.game_id,s.game_date,coalesce(s.team_id,r.team_id) AS team_id,
                     s.shots_on_goal,s.shot_attempts,s.toi_minutes,s.pp_toi_minutes,
                     s.blocks,s.hits,s.fenwick_for,s.missed_shots,s.blocked_shots_taken,
                     s.rebounds_for,s.takeaways,s.giveaways,s.penalties_drawn,s.penalties_taken,
                     s.ev_shot_attempts,s.pp_shot_attempts,s.sh_shot_attempts,
                     s.ev_sog,s.pp_sog,s.sh_sog,s.goals,s.assists
              FROM nhl.import_skater_logs_stage s
              LEFT JOIN latest_roster r USING(game_id,player_id)
              WHERE s.game_date=%s::date
            )
            INSERT INTO nhl.skater_game_logs_raw
              (player_id,game_id,team_id,opponent_id,is_home,game_date,shots_on_goal,shot_attempts,
               toi_minutes,pp_toi_minutes,blocks,hits,fenwick_for,missed_shots,blocked_shots_taken,
               rebounds_for,takeaways,giveaways,penalties_drawn,penalties_taken,
               ev_shot_attempts,pp_shot_attempts,sh_shot_attempts,ev_sog,pp_sog,sh_sog,goals,assists,points)
            SELECT s.player_id,s.game_id,s.team_id,
                   CASE WHEN s.team_id=g.home_team_id THEN g.away_team_id ELSE g.home_team_id END,
                   s.team_id=g.home_team_id,s.game_date,s.shots_on_goal,s.shot_attempts,s.toi_minutes,
                   nullif(s.pp_toi_minutes,0),s.blocks,s.hits,s.fenwick_for,s.missed_shots,
                   s.blocked_shots_taken,s.rebounds_for,s.takeaways,s.giveaways,s.penalties_drawn,
                   s.penalties_taken,s.ev_shot_attempts,s.pp_shot_attempts,s.sh_shot_attempts,
                   s.ev_sog,s.pp_sog,s.sh_sog,s.goals,s.assists,
                   CASE WHEN s.goals IS NOT NULL OR s.assists IS NOT NULL THEN coalesce(s.goals,0)+coalesce(s.assists,0) END
            FROM source s JOIN nhl.games g USING(game_id)
            WHERE s.team_id IN (g.home_team_id,g.away_team_id)
            ON CONFLICT (player_id,game_id) DO UPDATE SET
              team_id=excluded.team_id,opponent_id=excluded.opponent_id,is_home=excluded.is_home,
              game_date=excluded.game_date,shots_on_goal=excluded.shots_on_goal,
              shot_attempts=coalesce(excluded.shot_attempts,nhl.skater_game_logs_raw.shot_attempts),
              toi_minutes=excluded.toi_minutes,
              pp_toi_minutes=coalesce(excluded.pp_toi_minutes,nhl.skater_game_logs_raw.pp_toi_minutes),
              blocks=excluded.blocks,hits=excluded.hits,fenwick_for=excluded.fenwick_for,
              missed_shots=excluded.missed_shots,blocked_shots_taken=excluded.blocked_shots_taken,
              rebounds_for=excluded.rebounds_for,takeaways=excluded.takeaways,giveaways=excluded.giveaways,
              penalties_drawn=excluded.penalties_drawn,penalties_taken=excluded.penalties_taken,
              ev_shot_attempts=excluded.ev_shot_attempts,pp_shot_attempts=excluded.pp_shot_attempts,
              sh_shot_attempts=excluded.sh_shot_attempts,ev_sog=excluded.ev_sog,pp_sog=excluded.pp_sog,
              sh_sog=excluded.sh_sog,goals=excluded.goals,assists=excluded.assists,points=excluded.points
        """, (slate_date, slate_date))
        cursor.execute("""
            WITH latest_roster AS (
              SELECT DISTINCT ON (game_id,player_id) game_id,player_id,team_id
              FROM nhl.roster_status
              WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date=%s::date)
              ORDER BY game_id,player_id,asof_ts DESC
            ), ranked AS (
              SELECT s.*,coalesce(s.team_id,r.team_id) AS resolved_team_id,
                     row_number() OVER (PARTITION BY s.game_id,coalesce(s.team_id,r.team_id) ORDER BY s.toi_minutes DESC NULLS LAST,s.player_id) AS rn
              FROM nhl.import_goalie_logs_stage s LEFT JOIN latest_roster r USING(game_id,player_id)
              WHERE s.game_date=%s::date
            )
            INSERT INTO nhl.goalie_game_logs_raw
              (player_id,game_id,team_id,opponent_id,is_home,game_date,saves,shots_faced,goals_allowed,
               toi_minutes,start_flag,start_prob,ev_shots_faced,pp_shots_faced,sh_shots_faced,
               high_danger_shots_faced,rebounds_allowed)
            SELECT s.player_id,s.game_id,s.resolved_team_id,
                   CASE WHEN s.resolved_team_id=g.home_team_id THEN g.away_team_id ELSE g.home_team_id END,
                   s.resolved_team_id=g.home_team_id,s.game_date,s.saves,s.shots_faced,
                   CASE WHEN s.shots_faced IS NOT NULL AND s.saves IS NOT NULL THEN s.shots_faced-s.saves END,
                   s.toi_minutes,s.rn=1,s.start_prob,s.ev_shots_faced,s.pp_shots_faced,
                   s.sh_shots_faced,s.high_danger_shots_faced,s.rebounds_allowed
            FROM ranked s JOIN nhl.games g USING(game_id)
            WHERE s.resolved_team_id IN (g.home_team_id,g.away_team_id)
            ON CONFLICT (player_id,game_id) DO UPDATE SET
              team_id=excluded.team_id,opponent_id=excluded.opponent_id,is_home=excluded.is_home,
              game_date=excluded.game_date,saves=excluded.saves,shots_faced=excluded.shots_faced,
              goals_allowed=excluded.goals_allowed,toi_minutes=excluded.toi_minutes,start_flag=excluded.start_flag,
              start_prob=excluded.start_prob,ev_shots_faced=excluded.ev_shots_faced,
              pp_shots_faced=excluded.pp_shots_faced,sh_shots_faced=excluded.sh_shots_faced,
              high_danger_shots_faced=excluded.high_danger_shots_faced,
              rebounds_allowed=excluded.rebounds_allowed
        """, (slate_date, slate_date))
        connection.commit()


def governed_collectors(dsn: str, slate_date: str) -> None:
    python = str(ROOT / ".venv/bin/python")
    _run([python, str(SCRIPTS / "import_schedule_today.py")], slate_date, dsn)
    _run([python, str(SCRIPTS / "refresh_players_and_roster_today.py")], slate_date, dsn)
    _run([python, str(SCRIPTS / "seed_goalie_logs_for_date.py")], slate_date, dsn)
    _run([python, str(SCRIPTS / "seed_skater_logs_for_date.py")], slate_date, dsn)
    _run([python, str(SCRIPTS / "ingest_shiftcharts_for_date.py"), "--date", slate_date], slate_date, dsn)
    _promote_stage(dsn, slate_date)
    _run([python, str(SCRIPTS / "backfill_game_manpower_segments.py"), "--start-date", slate_date,
          "--end-date", slate_date, "--season", "2026"], slate_date, dsn)
    _run([python, str(SCRIPTS / "fill_pp_toi_minutes_for_date.py"), "--date", slate_date, "--commit"], slate_date, dsn)
    _run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-v", f"game_date={slate_date}",
          "-f", str(SQL / "shiftcharts_pairings_for_date.sql")], slate_date, dsn)
    refresh = SCRIPTS / "refresh.sql"
    if refresh.is_file():
        _run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-f", str(refresh)], slate_date, dsn)


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--local-input-preflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--date", dest="slate_date")
    parser.add_argument("date_arg", nargs="?")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--prediction-root", type=Path, default=DEFAULT_PREDICTION_ROOT)
    parser.add_argument("--operational-root", type=Path, default=DEFAULT_OPERATIONAL_ROOT)
    parser.add_argument("--reuse-request-run-id")
    args = parser.parse_args()
    slate_date = args.slate_date or args.date_arg
    if not slate_date:
        parser.error("provide --date YYYY-MM-DD or positional YYYY-MM-DD")
    try:
        date.fromisoformat(slate_date)
    except ValueError:
        parser.error("date must be YYYY-MM-DD")
    source_binding = None
    reuse_lineage = None
    if slate_date != "2026-09-19":
        try:
            source_binding = resolve_operational_sources(
                slate_date=slate_date, operational_root=args.operational_root)
            if args.execute:
                if not args.reuse_request_run_id:
                    raise RuntimeError("EXPLICIT_REUSE_REQUEST_RUN_ID_REQUIRED")
                prior_root = (args.output_root / "request_runs" / slate_date /
                              args.reuse_request_run_id)
                reuse_lineage = verify_preserved_response_run(
                    prior_root, expected_run_id=args.reuse_request_run_id,
                    slate_date=slate_date, game_ids=source_binding["game_ids"],
                )
        except RuntimeError as error:
            print(json.dumps({"status": "FAILED_CLOSED_LOCAL_INPUT", "failure": str(error),
                              "database_requests": 0, "external_requests": 0,
                              "bookmaker_requests": 0, "paid_credits": 0},
                             indent=2, sort_keys=True))
            return 5
    if args.local_input_preflight:
        if source_binding is None:
            print(json.dumps({"status": "LEGACY_SEPTEMBER_19_BINDING_UNCHANGED",
                              "slate_date": slate_date, "database_requests": 0,
                              "external_requests": 0}, indent=2, sort_keys=True))
        else:
            print(json.dumps({"status": "LOCAL_INPUTS_VALID", "database_requests": 0,
                              "external_requests": 0, "source_binding": source_binding},
                             indent=2, sort_keys=True))
        return 0
    load_env(args.env_file)
    dsn = (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        print(json.dumps({"status": "FAILED", "failure": "DATABASE_URL_MISSING"}))
        return 3
    if not args.execute:
        payload, code = read_only_preflight(dsn, slate_date)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return code
    try:
        with reconciliation_lock(args.output_root, slate_date):
            canonical = canonical_slate(dsn, slate_date)
            canonical_ids = set(canonical.game_id.astype(int))
            if source_binding is not None and canonical_ids != set(source_binding["game_ids"]):
                raise RuntimeError("DATABASE_CANONICAL_GAME_SET_DIFFERS_FROM_IMMUTABLE_LOCAL_SPINE")
            game_hash = canonical_game_set_hash(canonical_ids)
            started = datetime.now(timezone.utc)
            run_id = f"nhlpostgame_{slate_date.replace('-', '')}_{started.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}"
            request_root = args.output_root / "request_runs" / slate_date / run_id
            context_values = {
                ENV_REQUIRED: "1", ENV_RUN_ID: run_id,
                ENV_JOURNAL: str(request_root / "official_request_journal.jsonl"),
                ENV_CACHE: str(request_root / "preserved_responses"),
                ENV_SLATE: slate_date, ENV_GAME_HASH: game_hash,
                ENV_GAME_IDS: ",".join(str(value) for value in sorted(canonical_ids)),
            }
            if reuse_lineage is not None:
                context_values.update({
                    ENV_SOURCE_CACHE: reuse_lineage["source_cache"],
                    ENV_SOURCE_RUN_ID: reuse_lineage["source_run_id"],
                    ENV_SOURCE_JOURNAL_SHA256: reuse_lineage["source_journal_sha256"],
                })
            else:
                for key in (ENV_SOURCE_CACHE, ENV_SOURCE_RUN_ID, ENV_SOURCE_JOURNAL_SHA256):
                    os.environ.pop(key, None)
            os.environ.update(context_values)
            RequestContext.from_env(required=True)
            official, boxes, request_counts = fetch_official(
                slate_date, canonical_ids, reuse_authority=reuse_lineage is not None)
            validate_final_slate(canonical, official, slate_date)
            destination, disposition = publish_reconciliation(
                canonical=canonical, official=official, boxscores=boxes, slate_date=slate_date,
                prediction_root=args.prediction_root / slate_date, output_root=args.output_root,
                collector=lambda: governed_collectors(dsn, slate_date),
                observed_at=datetime.now(timezone.utc).isoformat(),
                request_journal=Path(context_values[ENV_JOURNAL]),
                request_accounting_factory=lambda: summarize_journal(
                    Path(context_values[ENV_JOURNAL]), run_id=run_id,
                    expected_game_hash=game_hash,
                ),
                source_binding=source_binding,
                request_lineage=reuse_lineage,
            )
            accounting = json.loads((destination / "official_request_accounting.json").read_text())
        print(json.dumps({
            "status": disposition, "output": str(destination),
            "authority_boundary_request_counts": request_counts,
            "official_request_accounting": accounting,
            "bookmaker_requests": 0, "paid_credits": 0,
        }, indent=2, sort_keys=True))
        return 0
    except RuntimeError as error:
        message = str(error)
        print(json.dumps({"status": "FAILED_CLOSED", "failure": message,
                          "bookmaker_requests": 0, "paid_credits": 0}, indent=2, sort_keys=True))
        if "NOT_OFFICIAL_FINAL" in message:
            return 2
        if "IDENTITY" in message or "UNSUPPORTED" in message:
            return 3
        if "ALREADY_RUNNING" in message:
            return 4
        if "CONFLICTING_RETAINED" in message:
            return 6
        return 5
    except Exception as error:
        print(json.dumps({"status": "FAILED_CLOSED",
                          "failure": f"{type(error).__name__}:{error}",
                          "bookmaker_requests": 0, "paid_credits": 0},
                         indent=2, sort_keys=True))
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
