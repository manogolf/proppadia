#!/usr/bin/env python3
import argparse, os, time, json, math, sys
from typing import List, Dict, Any, Tuple
import psycopg2, psycopg2.extras
import requests

API_WEB_BASE   = "https://api-web.nhle.com/v1"
STATSAPI_BASE  = "https://statsapi.web.nhl.com/api/v1"

def http_get_json(url: str, timeout: int = 12) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_people_apiweb(pid: int) -> Dict[str, Any]:
    """
    api-web: player landing has good basics: name, position, shoots, team.
    Typical endpoint: /player/{pid}/landing
    """
    url = f"{API_WEB_BASE}/player/{pid}/landing"
    try:
        j = http_get_json(url)
    except Exception:
        return {}
    if not isinstance(j, dict): 
        return {}
    # Try common locations for fields; structure can vary by season/status
    first = j.get("firstName") or j.get("firstNameDefault") or j.get("firstNameShort")
    last  = j.get("lastName")  or j.get("lastNameDefault")
    full  = j.get("fullName")  or (" ".join([first or "", last or ""]).strip() or None)

    pos   = j.get("positionAbbrev") or j.get("position") or (j.get("positionCode") if isinstance(j.get("positionCode"), str) else None)
    shoots= j.get("shootsCatches") or j.get("shoots") or j.get("catches")

    # team: sometimes present as teamId or nested currentTeam
    team_id = None
    if isinstance(j.get("currentTeam"), dict):
        team_id = j["currentTeam"].get("id") or j["currentTeam"].get("teamId")
    team_id = team_id or j.get("teamId")

    active = j.get("active", True)
    return {
        "first_name": first,
        "last_name": last,
        "full_name": full,
        "position": pos,
        "shoots_catches": shoots,
        "current_team_id": team_id,
        "active": bool(active)
    }

def fetch_people_statsapi(pid: int) -> Dict[str, Any]:
    """
    statsapi: /people/{pid}
    """
    url = f"{STATSAPI_BASE}/people/{pid}"
    try:
        j = http_get_json(url)
    except Exception:
        return {}
    people = j.get("people") if isinstance(j, dict) else None
    if not isinstance(people, list) or not people:
        return {}
    p = people[0]
    pos = None
    pp = p.get("primaryPosition")
    if isinstance(pp, dict):
        pos = pp.get("abbreviation") or pp.get("code")
    return {
        "first_name": p.get("firstName"),
        "last_name":  p.get("lastName"),
        "full_name":  p.get("fullName") or (" ".join([p.get("firstName") or "", p.get("lastName") or ""]).strip() or None),
        "position":   pos,
        "shoots_catches": p.get("shootsCatches"),
        "current_team_id": (p.get("currentTeam") or {}).get("id"),
        "active": bool(p.get("active", True)),
    }

def fetch_people(ids: List[int], source: str = "auto", sleep_sec: float = 0.15) -> Dict[int, Dict[str, Any]]:
    """
    Fetch each player id individually with small pacing.
    source = 'apiweb' | 'statsapi' | 'auto' (apiweb first, fallback to statsapi)
    """
    out: Dict[int, Dict[str, Any]] = {}
    for pid in ids:
        data: Dict[str, Any] = {}
        for attempt in range(3):
            try:
                if source == "apiweb":
                    data = fetch_people_apiweb(pid)
                elif source == "statsapi":
                    data = fetch_people_statsapi(pid)
                else:
                    # auto
                    data = fetch_people_apiweb(pid)
                    if not data:
                        data = fetch_people_statsapi(pid)
                break
            except Exception:
                time.sleep(0.4 * (attempt + 1))
        if data:
            out[pid] = data
        time.sleep(sleep_sec)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", required=False,
                    help="Postgres DSN/URL. If omitted, reads from db_url.txt in project root.")
    ap.add_argument("--project", default=".", help="Project root for db_url.txt fallback.")
    ap.add_argument("--limit", type=int, default=0, help="Cap players to backfill (0 = all).")
    ap.add_argument("--source", choices=["auto","apiweb","statsapi"], default="auto",
                    help="Which API to use for player details (default: auto).")
    ap.add_argument("--batch", type=int, default=500, help="Upsert page size.")
    args = ap.parse_args()

    dsn = args.db_url
    if not dsn:
        dsn_path = os.path.join(args.project, "db_url.txt")
        with open(dsn_path, "r") as f:
            dsn = f.read().strip()

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    # 1) Collect candidate player_ids
    cur.execute("""
      WITH cand AS (
        SELECT player_id FROM nhl.import_skater_logs_stage
        UNION SELECT player_id FROM nhl.import_goalie_logs_stage
        UNION SELECT player_id FROM nhl.skater_game_logs_raw
        UNION SELECT player_id FROM nhl.goalie_game_logs_raw
      )
      SELECT DISTINCT p.player_id
      FROM cand c
      LEFT JOIN nhl.players p ON p.player_id = c.player_id
      WHERE p.full_name IS NULL
         OR p.full_name ILIKE 'Unknown %'
         OR p.position IS NULL
         OR p.shoots_catches IS NULL
      ORDER BY 1
    """)
    ids = [r[0] for r in cur.fetchall()]
    if args.limit and len(ids) > args.limit:
        ids = ids[:args.limit]

    if not ids:
        print("Nothing to backfill. ✅")
        cur.close(); conn.close()
        return

    print(f"Found {len(ids)} players to backfill… (source={args.source})")

    # 2) Fetch from API(s)
    info = fetch_people(ids, source=args.source)

    # 3) Upsert into nhl.players
    rows: List[Tuple] = []
    for pid, d in info.items():
        rows.append((
            pid,
            d.get("full_name"),
            d.get("first_name"),
            d.get("last_name"),
            d.get("position"),
            d.get("shoots_catches"),
            d.get("current_team_id"),
            d.get("active", True),
        ))

    if not rows:
        print("No API data returned (network or ID coverage). Skipping upsert.")
        # Don’t run a “sanity” query when there was no upsert: nothing to report.
        cur.close(); conn.close()
        return

    upsert_sql = """
      INSERT INTO nhl.players AS p
        (player_id, full_name, first_name, last_name, position, shoots_catches, current_team_id, active)
      VALUES %s
      ON CONFLICT (player_id) DO UPDATE
      SET
        full_name       = COALESCE(EXCLUDED.full_name, p.full_name),
        first_name      = COALESCE(EXCLUDED.first_name, p.first_name),
        last_name       = COALESCE(EXCLUDED.last_name, p.last_name),
        position        = COALESCE(EXCLUDED.position, p.position),
        shoots_catches  = COALESCE(EXCLUDED.shoots_catches, p.shoots_catches),
        current_team_id = COALESCE(EXCLUDED.current_team_id, p.current_team_id),
        active          = COALESCE(EXCLUDED.active, p.active),
        updated_at      = now();
    """
    # upsert in pages to keep packets small
    total = 0
    for i in range(0, len(rows), args.batch):
        chunk = rows[i:i+args.batch]
        psycopg2.extras.execute_values(cur, upsert_sql, chunk, page_size=min(len(chunk), 500))
        conn.commit()
        total += len(chunk)
        print(f"  • upserted {total}/{len(rows)}")

    print(f"Upserted {total} players. ✅")

    # 4) Quick sanity (only for the IDs we actually fetched successfully)
    good_ids = [pid for pid in ids if pid in info]
    if good_ids:
        cur.execute("""
          SELECT
            COUNT(*) FILTER (WHERE full_name IS NULL OR full_name ILIKE 'Unknown %') AS still_unknown,
            COUNT(*) FILTER (WHERE position IS NULL) AS missing_position,
            COUNT(*) FILTER (WHERE shoots_catches IS NULL) AS missing_shoots
          FROM nhl.players
          WHERE player_id = ANY(%(ids)s)
        """, {"ids": good_ids})
        print("Post-backfill holes:", cur.fetchone())
    else:
        print("No fetched IDs to sanity-check.")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
