from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/raw/mlb_statsapi"


def get(url: str, path: Path) -> tuple[str, str]:
    if path.exists() and path.stat().st_size > 50:
        return str(path), "CACHED"
    path.parent.mkdir(parents=True, exist_ok=True)
    last = ""
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ProppadiaResearch/1.0"})
            with urllib.request.urlopen(req, timeout=45) as response:
                payload = response.read()
            parsed = json.loads(payload)
            if not isinstance(parsed, dict):
                raise ValueError("non-object JSON")
            path.write_bytes(payload)
            return str(path), "ACQUIRED"
        except Exception as exc:
            last = f"{type(exc).__name__}:{exc}"
            time.sleep(0.4 * (attempt + 1))
    return str(path), f"FAILED:{last}"


def games(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    return [g for d in data.get("dates", []) for g in d.get("games", []) if g.get("gameType") == "R"]


def main() -> None:
    g25 = games(BASE / "schedule_2025_regular.json")
    g26 = games(BASE / "schedule_2026_through_0813.json")
    jobs: list[tuple[str, Path]] = []
    for g in g25 + g26:
        pk = int(g["gamePk"])
        jobs.append((f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore", BASE / "boxscores" / f"boxscore_{pk}.json"))
    for g in g25 + g26:
        date = str(g.get("officialDate"))
        for side in ("away", "home"):
            tid = int(g["teams"][side]["team"]["id"])
            jobs.append((f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active&date={date}", BASE / "rosters" / date / f"active_roster_{tid}.json"))
    # Remove same team/date duplicates caused by doubleheaders.
    unique = {str(path): (url, path) for url, path in jobs}
    jobs = list(unique.values())
    counts = {"ACQUIRED": 0, "CACHED": 0, "FAILED": 0}
    failures = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(get, u, p): (u, p) for u, p in jobs}
        for i, fut in enumerate(as_completed(futures), 1):
            path, status = fut.result()
            key = status if status in counts else "FAILED"
            counts[key] += 1
            if key == "FAILED": failures.append({"path": path, "status": status, "url": futures[fut][0]})
            if i % 250 == 0 or i == len(jobs):
                print(json.dumps({"completed": i, "total": len(jobs), **counts}), flush=True)
    (BASE / "acquisition_failures.json").write_text(json.dumps(failures, indent=2) + "\n")
    if failures:
        raise SystemExit(f"{len(failures)} acquisition failures")


if __name__ == "__main__":
    main()
