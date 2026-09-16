#!/usr/bin/env python3
"""Bounded WARN-only live runner for the armed NHL mainline cross-market shadow."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg

from backend.nhl.cross_market_shadow.core import PRESEASON_START, REGULAR_SEASON_START, fetch_markets, run_capture


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def export_inputs(dsn: str, slate_date: str, directory: Path) -> tuple[Path, Path, pd.DataFrame]:
    with psycopg.connect(dsn) as connection:
        schedule = pd.read_sql_query("""
            SELECT season AS canonical_season, game_date::text AS slate_date, game_id,
                   game_date::text AS game_date, start_time_utc AS scheduled_start_time_utc,
                   home_team_id, home_team_code AS home_team, away_team_id,
                   away_team_code AS away_team, upper(coalesce(status,'SCHEDULED')) AS game_status,
                   game_type AS game_type_code
            FROM nhl.games WHERE season=2026 AND game_date=%s::date
            ORDER BY start_time_utc,game_id
        """, connection, params=(slate_date,))
        history = pd.read_sql_query("""
            WITH team_totals AS (
              SELECT g.season AS canonical_season,g.game_id,g.game_date,g.start_time_utc,
                     g.home_team_id,g.home_team_code,g.away_team_id,g.away_team_code,
                     g.status,g.game_type,l.team_id,
                     sum(coalesce(l.goals,0))::int AS goals,
                     sum(coalesce(l.shots_on_goal,0))::int AS shots
              FROM nhl.games g JOIN nhl.skater_game_logs_raw l USING(game_id)
              WHERE g.season=2026 AND g.game_date < %s::date AND lower(g.status)='final'
              GROUP BY g.season,g.game_id,g.game_date,g.start_time_utc,g.home_team_id,
                       g.home_team_code,g.away_team_id,g.away_team_code,g.status,g.game_type,l.team_id
            )
            SELECT h.canonical_season,h.game_date::text AS slate_date,h.game_id,
                   h.game_date::text AS game_date,h.start_time_utc AS scheduled_start_time_utc,
                   h.home_team_id,h.home_team_code AS home_team,h.away_team_id,
                   h.away_team_code AS away_team,upper(h.status) AS game_status,h.game_type AS game_type_code,
                   h.goals AS final_home_goals,a.goals AS final_away_goals,
                   h.shots AS final_home_shots,a.shots AS final_away_shots
            FROM team_totals h JOIN team_totals a USING(game_id)
            WHERE h.team_id=h.home_team_id AND a.team_id=a.away_team_id
            ORDER BY h.start_time_utc,h.game_id
        """, connection, params=(slate_date,))
    directory.mkdir(parents=True, exist_ok=False)
    schedule_path, history_path = directory / "schedule.csv", directory / "history.csv"
    schedule.to_csv(schedule_path, index=False)
    history.to_csv(history_path, index=False)
    return schedule_path, history_path, schedule


def phase_for(schedule: pd.DataFrame, now: datetime, requested: str, force: bool) -> tuple[str | None, str]:
    if schedule.empty:
        return None, "NO_CANONICAL_2026_GAMES"
    starts = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True)
    before = starts[starts > pd.Timestamp(now)]
    if before.empty:
        return None, "NO_PRESTART_GAME"
    minutes = (before.min() - pd.Timestamp(now)).total_seconds() / 60
    if requested != "AUTO":
        return (requested, "EXPLICIT_FORCE" if force else "EXPLICIT_PHASE")
    local = now.astimezone(ZoneInfo("America/Los_Angeles"))
    if 20 <= minutes <= 75:
        return "FINAL_PREGAME", f"FIRST_START_IN_{minutes:.1f}_MINUTES"
    if local.hour == 12 and local.minute <= 30:
        return "MIDDAY", "LOCAL_MIDDAY_WINDOW"
    return None, f"OUTSIDE_CAPTURE_WINDOW_FIRST_START_IN_{minutes:.1f}_MINUTES"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", default="today")
    parser.add_argument("--phase", choices=["AUTO", "MIDDAY", "FINAL_PREGAME"], default="AUTO")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    load_env(args.env_file)
    now = utc_now()
    slate = now.astimezone(ZoneInfo("America/New_York")).date().isoformat() if args.slate_date == "today" else args.slate_date
    stamp = now.strftime("%Y%m%dT%H%M%S.%fZ")
    status_dir = args.output_root / "orchestration_status" / slate
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / f"capture_{stamp}.json"
    result: dict[str, object] = {
        "slate_date": slate, "warning_only": True, "player_prop_execution_allowed": True,
        "historical_odds_calls": 0, "live_calls": 0, "live_credits_consumed": 0,
    }
    try:
        dsn = os.environ.get("SUPABASE_DB_URL", "").strip()
        if not dsn:
            raise RuntimeError("SUPABASE_DB_URL_MISSING")
        input_dir = args.output_root / "runtime_inputs" / slate / stamp
        schedule_path, history_path, schedule = export_inputs(dsn, slate, input_dir)
        phase, reason = phase_for(schedule, now, args.phase, args.force)
        result.update({"canonical_games": len(schedule), "phase": phase, "gate_reason": reason})
        if phase is None:
            result["status"] = "NOOP_READY"
        else:
            existing = list((args.output_root / "season=2026" / f"slate_date={slate}" / f"run_type={phase}").glob("state=*"))
            if existing and not args.force:
                result.update({"status": "NOOP_ALREADY_CAPTURED", "existing_states": len(existing)})
            else:
                prior_paid_attempts = []
                for prior_path in sorted(status_dir.glob("capture_*.json")):
                    try:
                        prior = json.loads(prior_path.read_text())
                    except (OSError, json.JSONDecodeError):
                        continue
                    if prior.get("phase") == phase and int(prior.get("live_calls", 0)) > 0:
                        prior_paid_attempts.append(prior_path)
                if prior_paid_attempts and not args.force:
                    result.update({
                        "status": "NOOP_PAID_ATTEMPT_ALREADY_EXISTS",
                        "paid_attempt_status": str(prior_paid_attempts[-1]),
                        "operator_review_required_for_retry": True,
                    })
                    status_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
                    print(status_path)
                    return 0
                odds_path = input_dir / "the_odds_api_live_envelope.json"
                fetch_markets(os.environ.get("ODDS_API_KEY", "").strip(), odds_path)
                envelope = json.loads(odds_path.read_text())
                result["live_calls"] = 1
                result["live_credits_consumed"] = int(envelope["quota"]["credits_consumed"])
                result["ending_requests_remaining"] = envelope["quota"].get("requests_remaining")
                run = run_capture(
                    schedule_path, history_path, odds_path, args.output_root, slate,
                    envelope["capture_timestamp_utc"], phase,
                    canary_mode=PRESEASON_START <= slate < REGULAR_SEASON_START,
                )
                result.update({"status": "CAPTURED", "run_dir": str(run)})
    except Exception as error:
        result.update({"status": "FAILED_WARN_ONLY", "failure": f"{type(error).__name__}:{error}"})
    status_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(status_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
