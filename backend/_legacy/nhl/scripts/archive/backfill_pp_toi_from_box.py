# backend/nhl/scripts/backfill_pp_toi_from_box.py
#!/usr/bin/env python3
"""
Backfill nhl.skater_game_logs_raw.pp_toi_minutes from NHL boxscore 'powerPlayToi'.

- Iterates by date range via api-web.nhle.com/v1/schedule/YYYY-MM-DD
- For each game: fetches gamecenter/{gamePk}/boxscore
- Reads skaters under playerByGameStats.{homeTeam,awayTeam}.{forwards,defense}
- Maps NHL playerId -> internal player_id using nhl.player_external_ids (provider='nhl')
- Updates pp_toi_minutes where currently NULL or 0, using powerPlayToi > 0

Usage:
  python backend/nhl/scripts/backfill_pp_toi_from_box.py \
    --db-url "$SUPABASE_DB_URL" \
    --start 2023-10-10 --end 2023-12-31 \
    --commit-every 200 --verbose
"""

from __future__ import annotations

import argparse, os, sys, time, datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Iterable

import psycopg
from psycopg.rows import dict_row
from psycopg import sql as psql

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_SCHED = "https://api-web.nhle.com/v1/schedule"
API_GC    = "https://api-web.nhle.com/v1/gamecenter"

def _session() -> requests.Session:
    r = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-pptoi-backfill"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://",  HTTPAdapter(max_retries=r))
    return s

S = _session()

def toi_to_minutes(s: Any) -> float:
    """Accepts 'MM:SS', 'H:MM:SS', or numeric seconds; returns float minutes."""
    if s is None or s == "":
        return 0.0
    if isinstance(s, (int, float)):
        return float(s) / 60.0
    parts = str(s).split(":")
    try:
        if len(parts) == 3:
            h, m, sec = map(int, parts)
            return 60*h + m + sec/60.0
        m, sec = map(int, parts)
        return m + sec/60.0
    except Exception:
        return 0.0

def get_schedule(date_str: str) -> List[int]:
    """Return list of gamePk for the exact ET date (API returns 'games' or 'gameWeek')."""
    url = f"{API_SCHED}/{date_str}"
    r = S.get(url, timeout=15)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    data = r.json() or {}
    games = []
    if "gameWeek" in data:
        for day in data.get("gameWeek", []):
            games.extend(day.get("games", []) or [])
    else:
        games.extend(data.get("games", []) or [])

    out = []
    seen = set()
    for g in games:
        gid = g.get("id") or g.get("gamePk") or g.get("gameId")
        if gid is None: 
            continue
        # The schedule endpoint already filters by ET date in this form.
        try:
            if int(gid) not in seen:
                out.append(int(gid))
                seen.add(int(gid))
        except Exception:
            continue
    return out

def get_boxscore(game_pk: int) -> Optional[Dict[str, Any]]:
    url = f"{API_GC}/{game_pk}/boxscore"
    r = S.get(url, timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return None

def iter_box_skaters(box: Dict[str, Any]) -> Iterable[Tuple[Optional[int], float]]:
    """
    Yields (nhl_id, pp_minutes) for skaters in the boxscore.
    Only yields when pp_minutes > 0.
    """
    pstats = (box.get("playerByGameStats") or {})
    for side_key in ("homeTeam", "awayTeam"):
        team = pstats.get(side_key) or {}
        for bucket in ("forwards", "defense"):
            arr = team.get(bucket) or []
            if not isinstance(arr, list): 
                continue
            for p in arr:
                nhl_id = p.get("playerId") or p.get("id")
                try:
                    nhl_id = int(nhl_id) if nhl_id is not None else None
                except Exception:
                    nhl_id = None
                stats = p.get("stats") or {}
                pp_toi = stats.get("powerPlayToi") or stats.get("powerPlayTimeOnIce")
                pp_min = toi_to_minutes(pp_toi)
                if nhl_id is not None and pp_min > 0.0:
                    yield nhl_id, pp_min

def external_id_map(conn, nhl_ids: List[int]) -> Dict[int, int]:
    """Map NHL numeric ID -> internal player_id using nhl.player_external_ids."""
    if not nhl_ids:
        return {}
    # De-dup & keep ints
    ids = sorted({int(x) for x in nhl_ids if isinstance(x, (int,))})
    q = """
      SELECT provider_player_id::bigint AS nhl_id, player_id
      FROM nhl.player_external_ids
      WHERE provider='nhl'
        AND provider_player_id ~ '^[0-9]+$'
        AND provider_player_id::bigint = ANY(%s)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(q, (ids,))
        return {int(r["nhl_id"]): int(r["player_id"]) for r in cur.fetchall()}

def update_pp_minutes(conn, updates: List[Tuple[float, int, int]]) -> int:
    """
    updates: list of (pp_minutes, player_id, game_id)
    Only update where current pp_toi_minutes IS NULL or = 0.
    """
    if not updates:
        return 0
    # Use VALUES to bulk-update with a guard on existing value.
    sql = """
      UPDATE nhl.skater_game_logs_raw AS s
      SET pp_toi_minutes = data.pp_min
      FROM (VALUES %s) AS data(pp_min, player_id, game_id)
      WHERE s.player_id = data.player_id
        AND s.game_id   = data.game_id
        AND COALESCE(s.pp_toi_minutes, 0) = 0
    """
    # psycopg3: use execute with mogrify style via psql.SQL
    with conn.cursor() as cur:
        # Build VALUES list safely
        values_sql = b",".join(
            cur.mogrify("(%s,%s,%s)", (float(pm), int(pid), int(gid)))
            for (pm, pid, gid) in updates
        )
        q = psql.SQL(sql).format()
        cur.execute(q.as_string(cur) % values_sql.decode("utf-8"))
        return cur.rowcount

def date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = start
    step = dt.timedelta(days=1)
    while d <= end:
        yield d
        d += step

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", required=False, default=os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"),
                    help="Postgres connection URL")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--commit-every", type=int, default=200)
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()

def main():
    args = parse_args()
    if not args.db_url:
        print("Set --db-url or SUPABASE_DB_URL/DATABASE_URL", file=sys.stderr); sys.exit(2)

    # Ensure sslmode in URL
    db_url = args.db_url
    if "?sslmode=" not in db_url and "&sslmode=" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"

    try:
        start = dt.date.fromisoformat(args.start)
        end   = dt.date.fromisoformat(args.end)
    except Exception:
        print("Bad --start/--end date format (expected YYYY-MM-DD).", file=sys.stderr)
        sys.exit(2)
    if end < start:
        print("--end must be >= --start", file=sys.stderr); sys.exit(2)

    processed_games = 0
    updated_rows = 0

    with psycopg.connect(db_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            # safer to set per-session without params
            try:
                cur.execute("SET statement_timeout = '900s'")
            except Exception:
                pass

        for day in date_range(start, end):
            games = get_schedule(day.isoformat())
            for gpk in games:
                processed_games += 1
                try:
                    box = get_boxscore(gpk)
                    if not isinstance(box, dict):
                        if args.verbose:
                            print(f"[{processed_games}] {gpk}: boxscore 404/empty; skip", flush=True)
                        continue

                    pairs = list(iter_box_skaters(box))  # (nhl_id, pp_min) where pp_min>0
                    if not pairs:
                        if args.verbose:
                            print(f"[{processed_games}] {gpk}: no PP skater minutes > 0; skip", flush=True)
                        continue

                    nhl_ids = [nhl for (nhl, _) in pairs]
                    xmap = external_id_map(conn, nhl_ids)
                    updates: List[Tuple[float,int,int]] = []
                    missing = 0

                    for nhl_id, pp_min in pairs:
                        pid = xmap.get(int(nhl_id))
                        if pid is None:
                            missing += 1
                            continue
                        updates.append((round(float(pp_min), 2), int(pid), int(gpk)))

                    if updates:
                        changed = update_pp_minutes(conn, updates)
                        updated_rows += changed

                    if (processed_games % args.commit_every) == 0:
                        conn.commit()
                        if args.verbose:
                            print(f"… committed @ games={processed_games}, rows_updated_total={updated_rows}", flush=True)

                    if args.verbose:
                        print(f"[{processed_games}] {gpk}: pairs={len(pairs)} mapped={len(updates)} updated_now={changed if updates else 0} missing_map={missing}", flush=True)

                except Exception as e:
                    # don’t abort entire run on one game
                    try: conn.rollback()
                    except Exception: pass
                    if args.verbose:
                        print(f"[{processed_games}] {gpk}: ERROR {e}", file=sys.stderr)

        conn.commit()

    print(f"✅ Done. Games scanned: {processed_games}, rows updated: {updated_rows}")

if __name__ == "__main__":
    main()
