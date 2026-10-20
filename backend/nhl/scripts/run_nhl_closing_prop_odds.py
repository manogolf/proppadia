#!/usr/bin/env python3
"""Scheduled per-game near-puck NHL player-prop odds capture (phase CLOSING).

Each run looks at today's Eastern slate and captures player-prop odds (SOG,
Points, Saves; main and alternate lines, the same books and regions as the
daily run) once for every game whose own scheduled start is 10–30 minutes
away. Games that have started, or are closer than the window, are never
requested. Snapshots go to the existing odds_observations layout through
daily_capture.capture_odds_observation with phase CLOSING and a per-game claim.

Each run appends one line per considered game to
odds_observations/season=<s>/slate_date=<d>/closing_capture_log.jsonl and
prints binding failures, so a provider event that fails to bind is visible the
same day. Retries: an attempt that failed or did not bind the game is retried
while the game is still in the window (discovery is free; a paid request is
made only for a bound event), up to MAX_ATTEMPTS per game.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.daily_capture import (  # noqa: E402
    CLOSING_PHASE, ObservationAlreadyClaimed, RequestsOddsProvider,
    capture_odds_observation, load_canonical_slate,
)

EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
WINDOW_OPEN_MINUTES = 30
WINDOW_CLOSE_MINUTES = 10
MAX_ATTEMPTS = 3
DEFAULT_OBSERVATION_ROOT = ROOT / "artifacts/operational/nhl/odds_observations"
DEFAULT_SLATE_ROOT = ROOT / "artifacts/operational/nhl/slates"


def _season(slate_date: str) -> int:
    year, month = int(slate_date[:4]), int(slate_date[5:7])
    return year if month >= 7 else year - 1


def _start(game) -> datetime:
    return datetime.fromisoformat(str(game.start_time_utc).replace("Z", "+00:00")).astimezone(UTC)


def _prior_attempts(day_root: Path, claim_root: Path, game_id: int) -> tuple[int, bool]:
    """Return (attempt count, whether a prior attempt bound and captured this game)."""
    base = f"phase={CLOSING_PHASE}.game={game_id}"
    claims = sorted(claim_root.glob(f"{base}.claim.json")) + sorted(claim_root.glob(f"{base}.retry-*.claim.json"))
    captured = False
    for claim_path in claims:
        try:
            observation = day_root / str(json.loads(claim_path.read_text()).get("observation_name") or "")
        except (OSError, ValueError):
            continue
        binding = observation / "game_binding.jsonl"
        if (observation / "RUN_COMPLETE.json").is_file() and binding.is_file():
            rows = [json.loads(line) for line in binding.read_text().splitlines() if line.strip()]
            if any(int(row.get("canonical_game_id") or 0) == game_id
                   and row.get("binding_status") == "MATCHED" for row in rows):
                captured = True
    return len(claims), captured


def _binding_report(observation_dir: Path, game_id: int) -> dict[str, object]:
    path = observation_dir / "game_binding.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.is_file() else []
    matched = any(int(row.get("canonical_game_id") or 0) == game_id and row.get("binding_status") == "MATCHED"
                  for row in rows)
    unbound = sorted({str(row.get("provider_event_id")) for row in rows
                      if row.get("binding_status") != "MATCHED" and row.get("provider_event_id")})
    return {"game_bound": matched, "unbound_provider_events": unbound}


def run(*, now: datetime, observation_root: Path, slate_root: Path, dry_run: bool,
        provider=None, authorized: bool | None = None) -> list[dict[str, object]]:
    now = now.astimezone(UTC)
    slate_date = now.astimezone(EASTERN).date().isoformat()
    season = _season(slate_date)
    day_root = observation_root / f"season={season}" / f"slate_date={slate_date}"
    claim_root = observation_root / ".claims" / f"season={season}" / f"slate_date={slate_date}"
    games = load_canonical_slate(
        slate_date=slate_date,
        raw_schedule_path=slate_root / slate_date / "raw_schedule_response.json",
        slate_health_path=slate_root / slate_date / "slate_health.json")
    if provider is None and not dry_run:
        key = os.environ.get("ODDS_API_KEY", "").strip()
        provider = RequestsOddsProvider(key) if key else None
        authorized = bool(key)
    records: list[dict[str, object]] = []
    for game in games:
        start = _start(game)
        minutes = (start - now).total_seconds() / 60
        record: dict[str, object] = {
            "run_timestamp_utc": now.isoformat().replace("+00:00", "Z"), "slate_date": slate_date,
            "game_id": int(game.game_id), "scheduled_start_utc": game.start_time_utc,
            "minutes_to_start": round(minutes, 1),
        }
        if minutes <= WINDOW_CLOSE_MINUTES:
            # Never request inside 10 minutes or after the start. Record only a
            # game that just left its window without a bound closing capture.
            if minutes <= WINDOW_CLOSE_MINUTES - 15:
                continue
            if _prior_attempts(day_root, claim_root, int(game.game_id))[1]:
                continue
            record["decision"] = "MISSED_NO_CLOSING_CAPTURE"
        elif minutes > WINDOW_OPEN_MINUTES:
            continue  # not yet in its window; nothing to record
        else:
            attempts, captured = _prior_attempts(day_root, claim_root, int(game.game_id))
            if captured:
                record["decision"] = "SKIPPED_ALREADY_CAPTURED"
            elif attempts >= MAX_ATTEMPTS:
                record["decision"] = "SKIPPED_ATTEMPT_LIMIT"
            elif dry_run:
                record["decision"] = "WOULD_CAPTURE"
            else:
                try:
                    result = capture_odds_observation(
                        root=observation_root, season=season, slate_date=slate_date,
                        phase=CLOSING_PHASE, claim_scope=f"game={int(game.game_id)}",
                        parent_daily_run_id=f"nhlclosing_{slate_date.replace('-', '')}",
                        canonical_games=[game], provider=provider, authorized=bool(authorized),
                        compatibility_dir=None, now=now)
                    record.update({"decision": "CAPTURE_ATTEMPTED", "classification": result.classification,
                                   "observation": result.observation_dir.name,
                                   **_binding_report(result.observation_dir, int(game.game_id))})
                except ObservationAlreadyClaimed as error:
                    record.update({"decision": "SKIPPED_CONCURRENT_CLAIM", "detail": str(error)})
        records.append(record)
    if records and not dry_run:
        day_root.mkdir(parents=True, exist_ok=True)
        with (day_root / "closing_capture_log.jsonl").open("a") as log:
            for record in records:
                log.write(json.dumps(record, sort_keys=True) + "\n")
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", help="UTC ISO time to evaluate (testing); default is the current time")
    parser.add_argument("--dry-run", action="store_true", help="show decisions without any provider request")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--observation-root", type=Path, default=DEFAULT_OBSERVATION_ROOT)
    parser.add_argument("--slate-root", type=Path, default=DEFAULT_SLATE_ROOT)
    args = parser.parse_args()
    if args.env_file.is_file():
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    now = (datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now
           else datetime.now(UTC))
    if args.now and not args.dry_run:
        raise SystemExit("--now is for --dry-run only; live captures use the real clock")
    for record in run(now=now, observation_root=args.observation_root, slate_root=args.slate_root,
                      dry_run=args.dry_run):
        print(json.dumps(record, sort_keys=True))
        if record.get("game_bound") is False:
            print(f"CLOSING_BINDING_FAILED game_id={record['game_id']} "
                  f"unbound_events={record.get('unbound_provider_events')}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
