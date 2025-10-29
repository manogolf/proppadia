# ============================
# FILE: backend/nhl/scripts/ingest_points_from_boxscores.py
# ============================
#!/usr/bin/env python3
"""
Pull goals/assists per player from NHL gamecenter payloads for SLATE_DATE (ET)
and write a CSV we can \copy into nhl.import_skater_logs_stage.

Inputs:
  - SLATE_DATE env (YYYY-MM-DD, ET) or --date
  - nhl/site/data/events_today.json (used to discover game IDs if DB lookup fails)

Outputs:
  - exports/points_stage_<date>.csv with columns:
      player_id,game_id,game_date,goals,assists
"""
from __future__ import annotations
import os, sys, json, argparse
from pathlib import Path
from datetime import datetime
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "nhl" / "site" / "data"
EXPORTS = ROOT / "exports"
EXPORTS.mkdir(parents=True, exist_ok=True)

def load_events():
    p = SITE / "events_today.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return []
    return []

# backend/nhl/scripts/ingest_points_from_boxscores.py
def fetch_gamecenter(game_pk: int) -> dict:
    # api-web.nhle.com v1 gamecenter (needs a UA header or returns 403)
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_pk}/boxscore"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def norm_int(x):
    try:
        return int(x)
    except Exception:
        return 0

def iter_skaters(box: dict):
    pstats = (box.get("playerByGameStats") or {})
    for side in ("homeTeam","awayTeam"):
        t = pstats.get(side) or {}
        for k in ("forwards","defense"):
            arr = t.get(k) or []
            if not isinstance(arr, list): continue
            for p in arr:
                nhl_id = p.get("playerId") or p.get("id")
                if not nhl_id: continue
                sk = p.get("skaterFullStatistics") or p.get("skaterStats") or {}
                goals = norm_int(sk.get("goals"))
                assists = norm_int(sk.get("assists"))
                yield nhl_id, goals, assists

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=os.environ.get("SLATE_DATE",""))
    ap.add_argument("--gamepk-json", default=str(SITE / "events_today.json"),
                    help="Optional: events file used to discover game ids")
    ap.add_argument("--games-sql", default="", help="Optional: JSON mapping of game_id->gamePk if present")
    args = ap.parse_args()

    slate = args.date.strip()
    if not slate:
        print("FATAL: SLATE_DATE not set", file=sys.stderr); sys.exit(2)

    # discover gamePks from events_today.json (Odds API carries the id used by The Odds API; not NHL gamePk)
    # Prefer DB mapping if you’ve built it; otherwise we expect you already have import_schedule_today.py populating nhl.games with game_id and external pk
    # For safety here, we attempt best-effort by reading nhl.games via CSV if you export it, else bail to events (user may already have helpers mapping).
    # Minimal viable: require a games list exported to SITE/games_today.json {game_id, game_pk, game_date}
    games_hint = SITE / "games_today.json"
    game_rows = []
    if games_hint.exists():
        try:
            game_rows = json.loads(games_hint.read_text())
        except Exception:
            game_rows = []
    if not game_rows:
        print("⚠️  games_today.json missing; please ensure import_schedule_today.py writes game_pk mapping.", file=sys.stderr)
        print("    Skipping ingest; continuing (pipeline can still run if goals/assists already exist).")
        return

    out = EXPORTS / f"points_stage_{slate}.csv"
    with out.open("w") as f:
        f.write("player_id,game_id,game_date,goals,assists\n")
        for row in game_rows:
            if str(row.get("game_date","")) != slate: continue
            game_pk = row.get("game_pk")
            game_id = row.get("game_id")
            if not (game_pk and game_id): continue
            try:
                box = fetch_gamecenter(int(game_pk))
            except Exception as e:
                print(f"⚠️  fetch boxscore failed for {game_pk}: {e}", file=sys.stderr)
                continue
            for nhl_id, g, a in iter_skaters(box):
                f.write(f"{nhl_id},{game_id},{slate},{g},{a}\n")
    print(f"✅ Wrote {out}")

if __name__ == "__main__":
    main()
