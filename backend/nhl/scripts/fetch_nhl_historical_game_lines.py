#!/usr/bin/env python3
"""Historical NHL game lines (moneyline + totals) at the first daily run, 2023-24..2025-26 (P2.55).

Operator-authorized pull (2026-11-16, "Yes, fetch them"; sized at about 570 dates x about 20
credits). One Odds API historical snapshot per regular-season game date at 14:00 UTC (about
06:00 PT, the information the 05:45 first daily run has), markets h2h + totals, US region.
Raw responses are kept gzipped with a manifest (SHA-256, credits used from the response
headers). Resumable: a date with a stored response is not fetched again. No database writes.
The API key is read from backend/.env and never written or printed.

    ./bin/python backend/nhl/scripts/fetch_nhl_historical_game_lines.py
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
OUT = ROOT / "artifacts/analysis/nhl/game_lines_history_2026-11-16"
URL = "https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl/odds"
SNAPSHOT_UTC = "T14:00:00Z"


def game_dates() -> list[str]:
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("""SELECT DISTINCT game_date::text FROM nhl.games
                        WHERE season BETWEEN 2023 AND 2025 AND game_type = 2 AND game_date IS NOT NULL ORDER BY 1""")
        return [r[0] for r in cur.fetchall()]


def main() -> int:
    from dotenv import dotenv_values
    key = (dotenv_values(ROOT / "backend/.env").get("ODDS_API_KEY") or os.environ.get("ODDS_API_KEY") or "").strip()
    if not key:
        raise SystemExit("ODDS_API_KEY_MISSING")
    raw = OUT / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    man_path = OUT / "manifest.json"
    manifest = json.loads(man_path.read_text()) if man_path.is_file() else {}
    session = requests.Session()
    session.headers["User-Agent"] = "research-client/1.0"
    dates = game_dates()
    for i, day in enumerate(dates):
        path = raw / f"date={day}.json.gz"
        if day in manifest and path.is_file():
            continue
        params = {"apiKey": key, "regions": "us", "markets": "h2h,totals", "oddsFormat": "american",
                  "dateFormat": "iso", "date": day + SNAPSHOT_UTC}
        for attempt in range(6):
            try:
                r = session.get(URL, params=params, timeout=60)
                if r.status_code == 429:
                    time.sleep(5 * (attempt + 1)); continue
                r.raise_for_status()
                body = r.content
                payload = json.loads(body)
                break
            except Exception as e:  # message without the URL (which carries the key)
                if attempt == 5:
                    raise SystemExit(f"FETCH_FAILED:{day}:{type(e).__name__}")
                time.sleep(3 * (attempt + 1))
        path.write_bytes(gzip.compress(body))
        manifest[day] = {"snapshot_requested": day + SNAPSHOT_UTC, "snapshot_returned": payload.get("timestamp"),
                         "events": len(payload.get("data") or []), "sha256": hashlib.sha256(body).hexdigest(),
                         "credits_last": r.headers.get("x-requests-last"), "credits_used_total": r.headers.get("x-requests-used"),
                         "credits_remaining": r.headers.get("x-requests-remaining"),
                         "fetched_at_utc": datetime.now(timezone.utc).isoformat()}
        man_path.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
        if i % 50 == 0:
            print(f"{i + 1}/{len(dates)} {day} events={manifest[day]['events']} last={manifest[day]['credits_last']} "
                  f"remaining={manifest[day]['credits_remaining']}", flush=True)
        time.sleep(0.3)
    spent = sum(int(v.get("credits_last") or 0) for v in manifest.values())
    print(f"DONE dates={len(manifest)} of {len(dates)} credits_spent={spent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
