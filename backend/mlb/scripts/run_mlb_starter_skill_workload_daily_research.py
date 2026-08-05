#!/usr/bin/env python3
"""LaunchAgent-safe runner for MLB starter skill/workload research artifacts.

The runner resolves the MLB slate date in America/New_York, checks whether a
slate appears to exist, invokes the no-write generator, and updates the narrow
prospective observation index. It is research-only and does not write to the
database, call OddsAPI, or alter production outputs.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.mlb.scripts.build_mlb_starter_skill_workload_research import build as build_research
from backend.mlb.scripts.update_mlb_starter_skill_workload_observation import update_observation


DEFAULT_LOGIC_ROOT = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_launchagent")
DEFAULT_NOOP_ROOT = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_daily_noop")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_env(path: Path = Path("backend/.env")) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _resolve_slate_date(explicit: str) -> str:
    if explicit:
        return explicit
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def _fetch_schedule(date_value: str, timeout: int = 20) -> dict:
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_value}"
    request = urllib.request.Request(url, headers={"User-Agent": "proppadia-starter-skill-workload-research/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _schedule_game_count(payload: dict) -> int:
    return sum(len(day.get("games", [])) for day in payload.get("dates", []))


def _write_noop(date_value: str, reason: str, root: Path, generated_at_utc: str) -> Path:
    out_dir = root / date_value
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_value,
        "generated_at_utc": generated_at_utc,
        "status": "NOOP_SUCCESS",
        "reason": reason,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
    }
    path = out_dir / "starter_skill_workload_noop.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def run(args: argparse.Namespace) -> dict:
    _load_env()
    date_value = _resolve_slate_date(args.date)
    generated_at = _utc_now()
    if args.validate_no_games:
        noop_path = _write_noop(
            date_value,
            "controlled_validate_no_games_mode",
            Path(args.noop_root),
            generated_at,
        )
        return {
            "date": date_value,
            "status": "NOOP_SUCCESS",
            "reason": "controlled_validate_no_games_mode",
            "noop_path": str(noop_path),
            "db_writes": 0,
            "oddsapi_calls": 0,
            "production_behavior_changed": False,
        }

    schedule_status = "not_checked"
    game_count = None
    if not args.skip_schedule_check:
        try:
            schedule = _fetch_schedule(date_value)
            game_count = _schedule_game_count(schedule)
            schedule_status = "available"
            if game_count == 0:
                noop_path = _write_noop(
                    date_value,
                    "statsapi_schedule_has_no_games",
                    Path(args.noop_root),
                    generated_at,
                )
                return {
                    "date": date_value,
                    "status": "NOOP_SUCCESS",
                    "reason": "statsapi_schedule_has_no_games",
                    "game_count": 0,
                    "noop_path": str(noop_path),
                    "db_writes": 0,
                    "oddsapi_calls": 0,
                    "production_behavior_changed": False,
                }
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            schedule_status = f"warn_schedule_unavailable:{type(exc).__name__}"

    run_tag = args.run_tag or f"launchagent_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    build_args = argparse.Namespace(
        date=date_value,
        run_tag=run_tag,
        output_root=args.output_root,
        environment_root=args.environment_root,
        odds_root=args.odds_root,
        bf_source_root=args.bf_source_root,
        pitcher_history_csv=args.pitcher_history_csv,
        mode=args.mode,
        latest_view=args.latest_view,
        validate_only=False,
        no_db=args.no_db,
        strict_prior_only=args.strict_prior_only,
        generated_at_utc=generated_at,
    )
    result = build_research(build_args)
    obs_args = argparse.Namespace(
        date=date_value,
        run_dir=result["run_dir"],
        daily_root=args.output_root,
        observation_root=args.observation_root,
        min_starter_match_pct=args.min_starter_match_pct,
        no_db=args.no_db,
    )
    observation = update_observation(obs_args)
    payload = {
        "date": date_value,
        "run_tag": run_tag,
        "generated_at_utc": generated_at,
        "schedule_status": schedule_status,
        "game_count": game_count,
        "runner_status": "SUCCESS",
        "generator_result": result,
        "observation_status": observation,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
    }
    logic_root = Path(args.launchagent_artifact_root) / date_value
    logic_root.mkdir(parents=True, exist_ok=True)
    (logic_root / "starter_skill_workload_runner_latest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="", help="YYYY-MM-DD; defaults to America/New_York today")
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    parser.add_argument("--run-tag", default="")
    parser.add_argument("--latest-view", action="store_true")
    parser.add_argument("--output-root", default="artifacts/analysis/model_development/mlb_starter_skill_workload_daily")
    parser.add_argument("--environment-root", default="artifacts/analysis/mlb/hits_environment_snapshots")
    parser.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    parser.add_argument(
        "--bf-source-root",
        action="append",
        default=[
            "artifacts/analysis/model_development/mlb_starter_skill_workload_daily_generator/2026-07-11/bf_expansion_2026-05-01_to_2026-07-09",
            "artifacts/analysis/mlb/starter_expected_hits_allowed/starter_only_bf_write_gate_dedupe_sim_2026-07-05",
        ],
    )
    parser.add_argument("--pitcher-history-csv", default="")
    parser.add_argument("--observation-root", default="artifacts/analysis/model_development/mlb_starter_skill_workload_prospective_observation")
    parser.add_argument("--launchagent-artifact-root", default=str(DEFAULT_LOGIC_ROOT))
    parser.add_argument("--noop-root", default=str(DEFAULT_NOOP_ROOT))
    parser.add_argument("--min-starter-match-pct", type=float, default=0.80)
    parser.add_argument("--no-db", action="store_true")
    parser.add_argument("--strict-prior-only", action="store_true")
    parser.add_argument("--skip-schedule-check", action="store_true")
    parser.add_argument("--validate-no-games", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    result = run(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
