#!/usr/bin/env python3
"""One-week 06:00 PT NHL player-prop odds capture trial (phase EARLY_TRIAL).

Operator-approved 2026-10-22 to measure early-morning prop coverage and prices
with more snapshots than the four manual 06:00 captures retained so far. Each
run captures today's Eastern slate once, odds only: the same markets, books and
regions as the daily run, for games not yet started. No predictions, no
database access, no change to the daily runs.

The morning run imports the day's schedule at 07:30 PT, so this script fetches
the official schedule itself and keeps it, with its health record, beside the
day's odds snapshots (``early_trial_schedule/``), never in the shared slate
folder. Snapshots go to the existing odds_observations layout through
daily_capture.capture_odds_observation with phase EARLY_TRIAL (one claim per
slate). Daily readers skip the phase (is_side_capture_observation).

The trial stops itself: outside TRIAL_FIRST_SLATE..TRIAL_LAST_SLATE a run does
nothing, so the LaunchAgent cannot spend credits after the week even if it is
left installed. Each run appends one line to
odds_observations/season=<s>/slate_date=<d>/early_trial_capture_log.jsonl.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.daily_capture import (  # noqa: E402
    EARLY_TRIAL_PHASE, ObservationAlreadyClaimed, RequestsOddsProvider,
    capture_odds_observation, load_canonical_slate,
)
from backend.nhl.game_phase import authorized_season_for_date  # noqa: E402

EASTERN = ZoneInfo("America/New_York")
PACIFIC = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc
# Capture only near 06:00 PT. launchd runs a missed calendar job when the Mac
# wakes, which would otherwise record a later snapshot as the 06:00 one.
CAPTURE_WINDOW_PT = ((5, 55), (6, 30))
TRIAL_FIRST_SLATE = "2026-10-23"
TRIAL_LAST_SLATE = "2026-10-29"  # seven slates
DEFAULT_OBSERVATION_ROOT = ROOT / "artifacts/operational/nhl/odds_observations"
SCHEDULE_DIR_NAME = "early_trial_schedule"


def trial_active(slate_date: str) -> bool:
    return TRIAL_FIRST_SLATE <= slate_date <= TRIAL_LAST_SLATE


def in_capture_window(now: datetime) -> bool:
    local = now.astimezone(PACIFIC)
    (open_h, open_m), (close_h, close_m) = CAPTURE_WINDOW_PT
    return (open_h, open_m) <= (local.hour, local.minute) <= (close_h, close_m)


def captured_today(day_root: Path) -> str | None:
    """Name of today's completed EARLY_TRIAL snapshot, if one exists."""
    for observation in sorted(Path(day_root).glob("observation=*")):
        try:
            summary = json.loads((observation / "observation_summary.json").read_text())
        except (OSError, ValueError):
            continue
        if (summary.get("phase") == EARLY_TRIAL_PHASE
                and (observation / "RUN_COMPLETE.json").is_file()
                and summary.get("classification") == "CAPTURED_NONEMPTY"):
            return observation.name
    return None


def fetch_trial_schedule(slate_date: str, schedule_root: Path) -> tuple[Path, Path]:
    """Fetch and keep the official schedule under the trial's own folder.

    Reuses import_schedule_today's fetch and health record with its
    NHL_SLATE_HEALTH_ROOT redirect (the same one reconciliation uses); no
    database write is made.
    """
    os.environ["NHL_SLATE_HEALTH_ROOT"] = str(schedule_root)
    from backend.nhl.scripts import import_schedule_today as schedule

    schedule.HEALTH_ROOT = Path(schedule_root)
    games = schedule.fetch_schedule_for_date(slate_date)
    for game in games:
        home, _ = schedule._extract_provider_team_id_and_teamobj(game, "home")
        away, _ = schedule._extract_provider_team_id_and_teamobj(game, "away")
        if (not (game.get("id") or game.get("gamePk") or game.get("gameId"))
                or not (game.get("startTimeUTC") or game.get("gameDate"))
                or not home or not away or game.get("gameType") not in {1, 2, 3}):
            schedule._write_slate_health(slate_date, games, "PARTIAL", False,
                                         "incomplete/unsupported schedule identity")
            raise RuntimeError("EARLY_TRIAL_SCHEDULE_IDENTITY_INCOMPLETE")
    schedule._write_slate_health(slate_date, games, "READY" if games else "VALID_EMPTY_SLATE", True)
    day = Path(schedule_root) / slate_date
    return day / "raw_schedule_response.json", day / "slate_health.json"


def run(*, now: datetime, observation_root: Path, dry_run: bool,
        provider=None, authorized: bool | None = None,
        fetch_schedule=fetch_trial_schedule) -> dict[str, object]:
    now = now.astimezone(UTC)
    slate_date = now.astimezone(EASTERN).date().isoformat()
    record: dict[str, object] = {"run_timestamp_utc": now.isoformat().replace("+00:00", "Z"),
                                 "slate_date": slate_date, "phase": EARLY_TRIAL_PHASE}
    if not trial_active(slate_date):
        return {**record, "decision": "TRIAL_INACTIVE",
                "trial_window": f"{TRIAL_FIRST_SLATE}..{TRIAL_LAST_SLATE}"}
    season = authorized_season_for_date(slate_date)
    day_root = observation_root / f"season={season}" / f"slate_date={slate_date}"
    existing = captured_today(day_root)
    if existing:
        return {**record, "decision": "SKIPPED_ALREADY_CAPTURED", "observation": existing}
    if not dry_run and not in_capture_window(now):
        record["decision"] = "OUTSIDE_CAPTURE_WINDOW"
        day_root.mkdir(parents=True, exist_ok=True)
        with (day_root / "early_trial_capture_log.jsonl").open("a") as log:
            log.write(json.dumps(record, sort_keys=True) + "\n")
        return record
    raw_path, health_path = fetch_schedule(slate_date, day_root / SCHEDULE_DIR_NAME)
    games = load_canonical_slate(slate_date=slate_date, raw_schedule_path=raw_path,
                                 slate_health_path=health_path)
    pregame = [game for game in games
               if datetime.fromisoformat(str(game.start_time_utc).replace("Z", "+00:00")) > now]
    record.update({"scheduled_games": len(games), "pregame_games": len(pregame)})
    if not pregame:
        record["decision"] = "NO_PREGAME_GAMES"
    elif dry_run:
        record["decision"] = "WOULD_CAPTURE"
    else:
        if provider is None:
            key = os.environ.get("ODDS_API_KEY", "").strip()
            provider = RequestsOddsProvider(key) if key else None
            authorized = bool(key)
        try:
            result = capture_odds_observation(
                root=observation_root, season=season, slate_date=slate_date,
                phase=EARLY_TRIAL_PHASE,
                parent_daily_run_id=f"nhlearlytrial_{slate_date.replace('-', '')}",
                canonical_games=pregame, provider=provider, authorized=bool(authorized),
                compatibility_dir=None, now=now)
            record.update({"decision": "CAPTURE_ATTEMPTED",
                           "classification": result.classification,
                           "observation": result.observation_dir.name})
        except ObservationAlreadyClaimed as error:
            record.update({"decision": "SKIPPED_ALREADY_CAPTURED", "detail": str(error)})
    if not dry_run:
        day_root.mkdir(parents=True, exist_ok=True)
        with (day_root / "early_trial_capture_log.jsonl").open("a") as log:
            log.write(json.dumps(record, sort_keys=True) + "\n")
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", help="UTC ISO time to evaluate (dry runs only)")
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch the schedule into a temporary folder and report; no odds request")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--observation-root", type=Path, default=DEFAULT_OBSERVATION_ROOT)
    args = parser.parse_args()
    if args.env_file.is_file():
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    if args.now and not args.dry_run:
        raise SystemExit("--now is for --dry-run only; live captures use the real clock")
    now = (datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now
           else datetime.now(UTC))
    root = args.observation_root
    if args.dry_run:
        import tempfile
        root = Path(tempfile.mkdtemp(prefix="nhl_early_trial_dry_run_"))
    print(json.dumps(run(now=now, observation_root=root, dry_run=args.dry_run), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
