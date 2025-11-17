#!/usr/bin/env python3
import argparse
import datetime as dt
import sys
from pathlib import Path

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# --- repo root + env -------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

# If you keep backend/.env (per your quickstart), load it:
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / "backend" / ".env", override=True)
except Exception:
    pass


def _http() -> requests.Session:
    r = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-schedule/1.0"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s


S = _http()


def fetch_games_for_date(ds: str):
    """
    Use the new NHL web API schedule endpoint.

    Shape (per new-api.md):
      GET /v1/schedule/{date} -> { gameWeek: [ { date, games: [ { id, season, gameType, ... } ] }, ... ] }

    We collect all games whose block.date == ds.
    """
    url = f"https://api-web.nhle.com/v1/schedule/{ds}"
    try:
        r = S.get(url, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[schedule] {ds} ERROR fetching schedule: {e}", file=sys.stderr)
        return []

    try:
        js = r.json()
    except Exception as e:
        print(f"[schedule] {ds} ERROR decoding JSON: {e}", file=sys.stderr)
        return []

    games = []
    gw_list = js.get("gameWeek") or []
    for block in gw_list:
        if block.get("date") != ds:
            continue
        for g in block.get("games") or []:
            gid = g.get("id")
            if not gid:
                continue
            games.append({
                "id": int(gid),
                "season": g.get("season"),
                "gameType": g.get("gameType"),
            })

    return games


def main():
    ap = argparse.ArgumentParser(
        description="Backfill NHL games into nhl.* tables using new api-web.nhle.com schedule."
    )
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    if end < start:
        raise SystemExit("--end must be on/after --start")

    # import here so running from repo root works: `python backend/nhl/scripts/import_schedule_range.py ...`
    sys.path.insert(0, str(ROOT))
    from backend.nhl.scripts.ingest_boxscore import ingest_game

    cur = start
    while cur <= end:
        ds = cur.isoformat()
        games = fetch_games_for_date(ds)
        if not games:
            print(f"[schedule] {ds} no games")
        else:
            print(f"[schedule] {ds} {len(games)} game(s)")
    for g in games:
        gid = g["id"]
        gtype = str(g.get("gameType") or "")

        # Only regular season (2) and playoffs (3); skip preseason (1) and anything weird
        if gtype not in ("2", "3"):
            continue

        try:
            ingest_game(gid)
        except Exception as e:
            print(f"[schedule] {ds} game {gid} FAILED: {e}", file=sys.stderr)
        cur += dt.timedelta(days=1)


if __name__ == "__main__":
    main()
