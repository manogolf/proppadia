# File: nhl/scripts/tools/get_fixture.py
#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sys, time
from pathlib import Path
from typing import Tuple
import requests

BOX_URL_TPL = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
PBP_URL_TPL = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"

def fetch(url: str, retries: int = 3, timeout: int = 12) -> dict:
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(1.2 * attempt)
            else:
                raise last

def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")

def maybe_load(path: Path) -> Tuple[bool, dict | None]:
    if path.exists():
        try:
            return True, json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return True, None  # file exists but unreadable -> treat as stale
    return False, None

def main() -> None:
    ap = argparse.ArgumentParser(description="Download & cache NHL boxscore + PBP as local fixtures")
    ap.add_argument("--game-id", type=int, required=True, help="NHL gamePk, e.g., 2025010041")
    ap.add_argument("--out-dir", default="nhl/fixtures", help="Directory to save fixtures")
    ap.add_argument("--force", action="store_true", help="Re-download even if files exist")
    args = ap.parse_args()

    gid = args.game_id
    out = Path(args.out_dir)
    box_path = out / f"{gid}_box.json"
    pbp_path = out / f"{gid}_pbp.json"

    # Load or fetch boxscore
    loaded_box, box = maybe_load(box_path)
    if not loaded_box or args.force or box is None:
        box = fetch(BOX_URL_TPL.format(gid=gid))
        save_json(box_path, box)

    # Load or fetch PBP (optional; some preseason games may be sparse)
    loaded_pbp, pbp = maybe_load(pbp_path)
    if not loaded_pbp or args.force or pbp is None:
        try:
            pbp = fetch(PBP_URL_TPL.format(gid=gid))
        except Exception:
            pbp = {"_note": "PBP fetch failed or unavailable for this game"}
        save_json(pbp_path, pbp)

    # Tiny sanity print
    home = (box.get("homeTeam") or {}).get("abbrev") or "UNK"
    away = (box.get("awayTeam") or {}).get("abbrev") or "UNK"
    date = (box.get("gameDate") or box.get("startTimeUTC") or "").split("T")[0]
    print(json.dumps({
        "saved": {
            "boxscore": str(box_path),
            "play_by_play": str(pbp_path)
        },
        "summary": {
            "game_id": gid,
            "date": date,
            "home": home,
            "away": away,
            "pbp_keys": list(pbp.keys())[:5] if isinstance(pbp, dict) else "n/a"
        }
    }, indent=2))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
