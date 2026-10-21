#!/usr/bin/env python3
"""Per game: minutes between its CLOSING odds snapshot and puck drop (read-only).

Reads the slate's official schedule and every CLOSING snapshot in
odds_observations/season=<s>/slate_date=<d>/, and prints one row per game:
scheduled start, closing snapshot time, minutes before puck, whether the
provider event bound to the game, and how many prop rows the snapshot holds.
Games with no bound CLOSING snapshot are listed as missing.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.daily_capture import is_closing_observation, load_canonical_slate  # noqa: E402
from backend.nhl.scripts.run_nhl_closing_prop_odds import WINDOW_CLOSE_MINUTES, WINDOW_OPEN_MINUTES  # noqa: E402


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def report(slate_date: str, operational_root: Path) -> list[dict[str, object]]:
    year, month = int(slate_date[:4]), int(slate_date[5:7])
    season = year if month >= 7 else year - 1
    slates = operational_root / "slates" / slate_date
    games = load_canonical_slate(slate_date=slate_date,
                                 raw_schedule_path=slates / "raw_schedule_response.json",
                                 slate_health_path=slates / "slate_health.json")
    day = operational_root / "odds_observations" / f"season={season}" / f"slate_date={slate_date}"
    closing: dict[int, list[tuple[datetime, int]]] = {}
    for obs in sorted(day.glob("observation=*")) if day.is_dir() else []:
        if not is_closing_observation(obs) or not (obs / "RUN_COMPLETE.json").is_file():
            continue
        observed = _ts(json.loads((obs / "observation_summary.json").read_text())["observation_timestamp_utc"])
        bound = {}
        for line in (obs / "game_binding.jsonl").read_text().splitlines():
            row = json.loads(line)
            if row.get("binding_status") == "MATCHED":
                bound[str(row["provider_event_id"])] = int(row["canonical_game_id"])
        counts: dict[int, int] = {}
        for line in (obs / "normalized_odds.jsonl").read_text().splitlines():
            row = json.loads(line)
            game_id = bound.get(str(row.get("provider_event_id")))
            if game_id is not None and str(row.get("market", "")).startswith("player_"):
                counts[game_id] = counts.get(game_id, 0) + 1
        for game_id in bound.values():
            closing.setdefault(game_id, []).append((observed, counts.get(game_id, 0)))
    rows = []
    for game in games:
        start = _ts(game.start_time_utc)
        snaps = [(t, n) for t, n in closing.get(int(game.game_id), []) if t < start]
        if snaps:
            observed, n = max(snaps)
            rows.append({"game_id": int(game.game_id), "scheduled_start_utc": game.start_time_utc,
                         "closing_snapshot_utc": observed.isoformat().replace("+00:00", "Z"),
                         "minutes_before_puck": round((start - observed).total_seconds() / 60, 1),
                         "prop_rows": n, "status": "CAPTURED"})
        else:
            rows.append({"game_id": int(game.game_id), "scheduled_start_utc": game.start_time_utc,
                         "closing_snapshot_utc": None, "minutes_before_puck": None,
                         "prop_rows": 0, "status": "MISSING"})
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Eastern slate date, YYYY-MM-DD")
    parser.add_argument("--operational-root", type=Path, default=ROOT / "artifacts/operational/nhl")
    args = parser.parse_args()
    rows = report(args.date, args.operational_root)
    print(f"{'game_id':<11} {'start (UTC)':<21} {'closing snapshot':<28} {'min before':>10} {'prop rows':>9}  status")
    for r in rows:
        print(f"{r['game_id']:<11} {r['scheduled_start_utc']:<21} {str(r['closing_snapshot_utc'] or '-'):<28} "
              f"{str(r['minutes_before_puck'] if r['minutes_before_puck'] is not None else '-'):>10} "
              f"{r['prop_rows']:>9}  {r['status']}")
    captured = [r for r in rows if r["status"] == "CAPTURED"]
    print(f"\n{len(captured)} of {len(rows)} games have a pregame CLOSING snapshot; "
          f"{sum(1 for r in captured if WINDOW_CLOSE_MINUTES <= r['minutes_before_puck'] <= WINDOW_OPEN_MINUTES)} "
          f"inside {WINDOW_CLOSE_MINUTES}-{WINDOW_OPEN_MINUTES} minutes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
