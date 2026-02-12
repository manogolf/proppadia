#!/usr/bin/env python3
import os, sys, datetime as dt
from zoneinfo import ZoneInfo

os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")
import re
PLACEHOLDER_RE = re.compile(r'(?i)^(player|unknown)\s+\d+$')
import psycopg
from psycopg.rows import dict_row
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

DB = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB and "&sslmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "sslmode=require"
if "?gssencmode=" not in DB and "&gssencmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "gssencmode=disable"

BASE = "https://api-web.nhle.com/v1"

def _session():
    r = Retry(total=6, connect=6, read=4, backoff_factor=0.5,
              status_forcelist=[429,500,502,503,504], allowed_methods=frozenset({"GET"}))
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia/backfill-player-names"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s

S = _session()

def fetch_name(pid: int) -> tuple[str|None, str|None, str|None]:
    try:
        resp = S.get(f"{BASE}/player/{pid}/landing", timeout=8)
        if resp.status_code == 404:
            return (None, None, None)
        resp.raise_for_status()
        j = resp.json() or {}
        full = (j.get("fullName") or j.get("playerName") or j.get("name") or "").strip() or None
        first = (j.get("firstName") or "").strip() or None
        last  = (j.get("lastName")  or "").strip() or None
        if not full and (first or last):
            full = f"{first or ''} {last or ''}".strip() or None
        return (first, last, full)
    except Exception:
        return (None, None, None)

def is_placeholder(s: str|None) -> bool:
    if not s or not s.strip(): return True
    t = s.strip().lower()
    if t.startswith("player ") or t.startswith("unknown "):
        # also handle “Player 847xxxx”
        parts = t.split()
        return len(parts) == 2 and parts[1].isdigit()
    return False

def main():
    with psycopg.connect(DB, prepare_threshold=0, row_factory=dict_row) as conn:
        # 1) Gather worklist
        with conn.cursor() as cur:
            cur.execute("""
                SELECT player_id, full_name
                FROM nhl.players
                WHERE full_name IS NULL
                   OR btrim(full_name) = ''
                   OR full_name ~* '^(player|unknown)\\s+\\d+$'
            """)
            todo = cur.fetchall()

        print(f"Backfilling names for {len(todo)} players…")

        updated = 0
        with conn.transaction():
            with conn.cursor() as cur:
                for row in todo:
                    pid = int(row["player_id"])

                    # fetch from API
                    first, last, full = fetch_name(pid)  # e.g. returns ('Joe','Pavelski','Joe Pavelski') or similar

                    # normalize
                    first = (first or "").strip()
                    last  = (last  or "").strip()
                    full  = (full  or "").strip()

                    # if API didn't give full, compose from first+last
                    if not full and (first or last):
                        full = f"{first} {last}".strip()

                    # still nothing? skip safely
                    if not full:
                        # nothing to write; leave as-is
                        continue

                    # DO NOT write placeholders — check BEFORE update
                    if PLACEHOLDER_RE.match(full):
                        # log and skip; never write placeholders
                        # print(f"[{pid}] got placeholder '{full}' — skip")
                        continue

                    # write: keep existing first/last if already present
                    cur.execute("""
                        UPDATE nhl.players
                           SET first_name = COALESCE(NULLIF(first_name,''), %s),
                               last_name  = COALESCE(NULLIF(last_name,''),  %s),
                               full_name  = %s,
                               updated_at = now()
                         WHERE player_id = %s
                           AND (full_name IS NULL
                                OR btrim(full_name) = ''
                                OR full_name ~* '^(player|unknown)\\s+\\d+$')
                    """, (first or None, last or None, full, pid))

                    if cur.rowcount:
                        updated += 1
                        # optional: print each success
                        # print(f"[{pid}] → '{full}' (first='{first}', last='{last}')")

        print(f"✅ updated {updated} player names")
if __name__ == "__main__":
    main()
