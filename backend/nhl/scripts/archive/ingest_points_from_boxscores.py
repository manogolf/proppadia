#!/usr/bin/env python3
# backend/nhl/scripts/ingest_points_from_boxscores.py

from __future__ import annotations
import os, json, sys, urllib.request
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = ROOT / "backend" / "nhl" / "site" / "data" / "raw" / "gamecenter"
SITE    = ROOT / "backend" / "nhl" / "site" / "data"
OUT_DIR = ROOT / "backend" / "exports"

def _fetch_gamecenter(game_pk: int) -> dict:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_pk}/boxscore"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nhl.com/",
            "Connection": "keep-alive",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def _iter_points_from_box(box: dict, game_id: int, game_date: str):
    """Yield dicts: player_id, game_id, game_date, goals, assists, points."""
    pstats = (box.get("playerByGameStats") or {})
    for side in ("homeTeam", "awayTeam"):
        t = pstats.get(side) or {}
        for grp in ("forwards", "defense"):  # skip goalies for points
            arr = t.get(grp) or []
            if not isinstance(arr, list):
                continue
            for p in arr:
                pid = p.get("playerId") or p.get("id")
                if not pid:
                    continue
                stats = p.get("skaterFullStatistics") or p.get("skaterStats") or {}
                try:
                    g = int(stats.get("goals") or 0)
                except Exception:
                    g = 0
                try:
                    a = int(stats.get("assists") or 0)
                except Exception:
                    a = 0
                yield {
                    "player_id": int(pid),
                    "game_id": int(game_id),
                    "game_date": str(game_date),
                    "goals": g,
                    "assists": a,
                    "points": g + a,
                }

def _load_local_boxes_for_slate(slate: str) -> list[tuple[dict,int,str]]:
    """Return list of (box, game_id, game_date) for files matching slate."""
    out = []
    if not RAW_DIR.exists():
        return out
    for f in sorted(RAW_DIR.glob("*.boxscore.json")):
        try:
            box = json.loads(f.read_text())
        except Exception:
            continue
        game_date = str(box.get("gameDate") or "").split("T")[0]
        if game_date != slate:
            continue
        game_id = box.get("id") or box.get("gamePk")
        if not game_id:
            continue
        out.append((box, int(game_id), game_date))
    return out

def _load_games_today() -> list[dict]:
    """Expect objects with at least {game_id, game_pk, game_date}."""
    p = SITE / "games_today.json"
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text())
    except Exception:
        return []

def main():
    slate = os.environ.get("SLATE_DATE", "").strip()
    if not slate:
        print("FATAL: SLATE_DATE env var required (YYYY-MM-DD)", file=sys.stderr)
        sys.exit(2)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUT_DIR / f"points_stage_{slate}.csv"

    rows = []

    # 1) Try local cache first
    local = _load_local_boxes_for_slate(slate)
    if local:
        for (box, game_id, game_date) in local:
            rows.extend(_iter_points_from_box(box, game_id, game_date))
        mode = "local-cache"
    else:
        # 2) Fallback: fetch live using games_today.json (needs game_pk)
        games = [g for g in _load_games_today() if str(g.get("game_date","")) == slate]
        if not games:
            print("⚠️ No local boxscores and no games_today.json entries for slate; writing header-only CSV.", file=sys.stderr)
            pd.DataFrame(columns=["player_id","game_id","game_date","goals","assists","points"]).to_csv(out_csv, index=False)
            print(f"✅ Wrote {out_csv} rows=0 (empty)")
            return
        mode = "fetched"
        for g in games:
            game_pk = g.get("game_pk")
            game_id = g.get("game_id")
            if not (game_pk and game_id):
                continue
            try:
                box = _fetch_gamecenter(int(game_pk))
            except Exception as e:
                print(f"⚠️ fetch failed for game_pk={game_pk}: {e}", file=sys.stderr)
                continue
            rows.extend(_iter_points_from_box(box, int(game_id), slate))

    # 3) Write CSV (always include header; keep zeros; include points)
    df = pd.DataFrame(rows, columns=["player_id","game_id","game_date","goals","assists","points"])
    df.to_csv(out_csv, index=False)
    print(f"✅ Wrote {out_csv} mode={mode} rows={len(df)}")

if __name__ == "__main__":
    main()
