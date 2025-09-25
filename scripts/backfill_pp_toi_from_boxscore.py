#  scripts/backfill_pp_toi_from_boxscore

#!/usr/bin/env python3
import os, sys, time, re
from typing import Dict, Any
import psycopg2, psycopg2.extras
import requests

API = "https://api-web.nhle.com/v1/gamecenter"
MMSS = re.compile(r"^\d{1,2}:\d{2}$")

def mmss_to_minutes(s: str) -> float:
    if not s or not MMSS.match(s): return None
    m, sec = s.split(":")
    return round(int(m) + int(sec)/60.0, 2)

def fetch_pp_toi_map(game_id: int) -> Dict[int, float]:
    """
    Return {player_id: pp_toi_minutes} for a game, using boxscore.
    """
    url = f"{API}/{game_id}/boxscore"
    for _ in range(3):
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            j = r.json()
            out: Dict[int, float] = {}

            def walk(obj: Any):
                if isinstance(obj, dict):
                    # common places
                    pid = obj.get("playerId") or obj.get("id")
                    # pp time in a few shapes:
                    pp = obj.get("powerPlayTimeOnIce") or obj.get("ppTimeOnIce")
                    if isinstance(pp, str) and MMSS.match(pp) and isinstance(pid, int):
                        mins = mmss_to_minutes(pp)
                        if mins is not None:
                            out.setdefault(pid, mins)
                    # nested forms like { "player": { firstName, ... } }
                    for v in obj.values():
                        if isinstance(v, (dict, list)): walk(v)
                elif isinstance(obj, list):
                    for v in obj:
                        if isinstance(v, (dict, list)): walk(v)

            walk(j)
            return out
        except Exception:
            time.sleep(0.3)
    return {}

def main():
    dsn = os.environ.get("DB_URL")
    if not dsn:
        # fallback to db_url.txt in project root
        here = os.path.dirname(os.path.abspath(__file__))
        proj = os.path.dirname(here)
        with open(os.path.join(proj, "db_url.txt"), "r") as f:
            dsn = f.read().strip()

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT game_id
            FROM nhl.skater_game_logs_raw
            WHERE pp_toi_minutes IS NULL
            ORDER BY game_id
        """)
        game_ids = [r[0] for r in cur.fetchall()]

    total_rows = 0
    for i, gid in enumerate(game_ids, 1):
        pp_map = fetch_pp_toi_map(gid)
        if not pp_map:
            if i % 25 == 0:
                print(f"[{i}/{len(game_ids)}] game_id={gid}: no PP data", flush=True)
            time.sleep(0.05)
            continue

        rows = [(mins, pid, gid) for pid, mins in pp_map.items()]
        if not rows:
            time.sleep(0.05)
            continue

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                UPDATE nhl.skater_game_logs_raw AS s
                SET pp_toi_minutes = data.mins
                FROM (VALUES %s) AS data(mins, player_id, game_id)
                WHERE s.player_id = data.player_id
                  AND s.game_id   = data.game_id
                  AND s.pp_toi_minutes IS NULL
            """, rows, template=None, page_size=500)
        conn.commit()
        total_rows += len(rows)

        if i % 25 == 0:
            print(f"[{i}/{len(game_ids)}] game_id={gid}: updated {len(rows)} (total {total_rows})", flush=True)
        time.sleep(0.08)

    print(f"✅ Done. Updated rows: {total_rows}")
    conn.close()

if __name__ == "__main__":
    main()
