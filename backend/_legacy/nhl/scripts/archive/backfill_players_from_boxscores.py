#!/usr/bin/env python3
import os, argparse, time, requests, psycopg2, psycopg2.extras
from typing import Dict, Any, List, Tuple

API = "https://api-web.nhle.com/v1/gamecenter"

def gj(url: str) -> Dict[str, Any]:
    for _ in range(3):
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.4)
    return {}

def harvest_from_boxscore(bx: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}

    def safe_merge(pid: int, **kvs):
        d = out.setdefault(pid, {})
        for k, v in kvs.items():
            if v not in (None, "", []):
                d.setdefault(k, v)

    def walk(x):
        if isinstance(x, dict):
            pid = x.get("playerId") or x.get("id")
            if isinstance(pid, int):
                first = last = full = pos = shoots = None
                player = x.get("player") if isinstance(x.get("player"), dict) else None
                if player:
                    first  = player.get("firstName") or player.get("first")
                    last   = player.get("lastName")  or player.get("last")
                    full   = player.get("fullName")
                    shoots = player.get("shootsCatches")

                first = first or x.get("firstName")
                last  = last  or x.get("lastName")
                full  = full  or x.get("fullName") or (" ".join([first or "", last or ""]).strip() or None)

                posd = x.get("position") if isinstance(x.get("position"), dict) else None
                if posd:
                    pos = posd.get("abbrev") or posd.get("code")
                else:
                    pos = x.get("positionCode") if isinstance(x.get("positionCode"), str) else (
                          x.get("position")     if isinstance(x.get("position"), str)     else None)

                safe_merge(pid,
                           first_name=first, last_name=last,
                           full_name=full, position=pos, shoots_catches=shoots)

            for v in x.values():
                if isinstance(v, (dict, list)): walk(v)
        elif isinstance(x, list):
            for v in x:
                if isinstance(v, (dict, list)): walk(v)

    walk(bx)
    return out

def need_players(cur) -> List[int]:
    cur.execute("""
      WITH cand AS (
        SELECT player_id FROM nhl.import_skater_logs_stage
        UNION
        SELECT player_id FROM nhl.import_goalie_logs_stage
        UNION
        SELECT player_id FROM nhl.skater_game_logs_raw
        UNION
        SELECT player_id FROM nhl.goalie_game_logs_raw
      )
      SELECT DISTINCT c.player_id
      FROM cand c
      LEFT JOIN nhl.players p ON p.player_id = c.player_id
      WHERE p.full_name IS NULL
         OR p.full_name ILIKE 'Unknown %%'
         OR p.position IS NULL
         OR p.shoots_catches IS NULL
      ORDER BY 1
    """)
    return [r[0] for r in cur.fetchall()]

def games_covering(cur, pids: List[int]) -> List[int]:
    if not pids:
        return []
    cur.execute("""
      WITH want(pid) AS (SELECT unnest(%s::bigint[]))
      SELECT DISTINCT s.game_id
      FROM want w
      JOIN (
        SELECT player_id, game_id FROM nhl.import_skater_logs_stage
        UNION ALL
        SELECT player_id, game_id FROM nhl.import_goalie_logs_stage
        UNION ALL
        SELECT player_id, game_id FROM nhl.skater_game_logs_raw
        UNION ALL
        SELECT player_id, game_id FROM nhl.goalie_game_logs_raw
      ) s ON s.player_id = w.pid
      ORDER BY 1
    """, (pids,))
    return [r[0] for r in cur.fetchall()]

def chunk(it: List[Any], n: int) -> List[List[Any]]:
    for i in range(0, len(it), n):
        yield it[i:i+n]

def upsert_batch(cur, conn, rows: List[Tuple], max_retries: int = 2):
    """
    Resilient batch upsert:
      - Try execute_values
      - On error: rollback and fall back to smaller chunks / row-by-row
      - Sanitize NULLs for NOT NULL columns (full_name, position) with safe defaults
    """
    if not rows:
        return

    def sanitize(r: Tuple) -> Tuple:
        # (player_id, full_name, first_name, last_name, position, shoots_catches, active)
        pid, full, first, last, pos, shoots, active = r
        if not full:
            # Prefer "First Last"; else "Unknown <pid>"
            nm = " ".join([first or "", last or ""]).strip()
            full = nm if nm else f"Unknown {pid}"
        if not pos:
            pos = "U"  # Unknown position safe default
        return (pid, full, first, last, pos, shoots, bool(active))

    clean_rows = [sanitize(r) for r in rows]

    sql = """
      INSERT INTO nhl.players AS p
        (player_id, full_name, first_name, last_name, position, shoots_catches, active)
      VALUES %s
      ON CONFLICT (player_id) DO UPDATE
      SET
        full_name      = COALESCE(EXCLUDED.full_name, p.full_name),
        first_name     = COALESCE(EXCLUDED.first_name, p.first_name),
        last_name      = COALESCE(EXCLUDED.last_name, p.last_name),
        position       = COALESCE(EXCLUDED.position, p.position),
        shoots_catches = COALESCE(EXCLUDED.shoots_catches, p.shoots_catches),
        active         = COALESCE(EXCLUDED.active, p.active),
        updated_at     = now();
    """

    # First, try as a single execute_values
    try:
        psycopg2.extras.execute_values(cur, sql, clean_rows, page_size=150)
        return
    except Exception as e:
        # Roll back the failed statement so the transaction leaves "aborted" state
        conn.rollback()

    # Fall back: split into halves (simple bisection). If still failing, row-by-row.
    def insert_chunk(chunk: List[Tuple]):
        try:
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=50)
            return True
        except Exception:
            conn.rollback()
            return False

    def insert_bisect(chunk: List[Tuple]):
        if not chunk:
            return 0
        if insert_chunk(chunk):
            return len(chunk)
        # Too big / a bad row inside — split
        if len(chunk) == 1:
            # row-by-row fallback with explicit single-row insert
            try:
                cur.execute(
                    """
                    INSERT INTO nhl.players AS p
                      (player_id, full_name, first_name, last_name, position, shoots_catches, active)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (player_id) DO UPDATE
                    SET
                      full_name      = COALESCE(EXCLUDED.full_name, p.full_name),
                      first_name     = COALESCE(EXCLUDED.first_name, p.first_name),
                      last_name      = COALESCE(EXCLUDED.last_name, p.last_name),
                      position       = COALESCE(EXCLUDED.position, p.position),
                      shoots_catches = COALESCE(EXCLUDED.shoots_catches, p.shoots_catches),
                      active         = COALESCE(EXCLUDED.active, p.active),
                      updated_at     = now();
                    """,
                    chunk[0]
                )
                return 1
            except Exception:
                conn.rollback()
                # skip truly toxic row, but keep going
                return 0
        mid = len(chunk) // 2
        return insert_bisect(chunk[:mid]) + insert_bisect(chunk[mid:])

    inserted = insert_bisect(clean_rows)
    if inserted < len(clean_rows):
        # Some rows were skipped; continue with the rest of the run.
        pass

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", required=True,
                    help="Use your Session Pooler URI; add gssencmode=disable if needed.")
    ap.add_argument("--limit-games", type=int, default=0,
                    help="Optional cap on number of games scanned.")
    ap.add_argument("--batch-size", type=int, default=300,
                    help="Players per upsert batch (smaller = safer for pooler).")
    args = ap.parse_args()

    # optional: keepalive to help long runs
    dsn = args.db_url
    if "keepalives" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn += f"{sep}connect_timeout=20&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5"

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    pids = need_players(cur)
    if not pids:
        print("Nothing to backfill. ✅")
        cur.close(); conn.close(); return

    games = games_covering(cur, pids)
    if args.limit_games and len(games) > args.limit_games:
        games = games[:args.limit_games]

    print(f"Need info for {len(pids)} players; scanning {len(games)} games for boxscores…")

    # stream-find and stream-upsert
    pending: Dict[int, Dict[str, Any]] = {}
    total_upserted = 0
    for i, gid in enumerate(games, 1):
        bx = gj(f"{API}/{gid}/boxscore")
        if bx:
            found = harvest_from_boxscore(bx)
            # merge into pending (only fill missing keys)
            for pid, d in found.items():
                if pid not in pids:
                    continue
                dst = pending.setdefault(pid, {})
                for k in ("full_name", "first_name", "last_name", "position", "shoots_catches"):
                    v = d.get(k)
                    if v not in (None, "", []):
                        dst.setdefault(k, v)

        # upsert every 25 games or at the end
        if (i % 25 == 0) or (i == len(games)):
            rows = []
            for pid, d in list(pending.items()):
                # require at least a name OR position to be useful
                if not any(d.get(k) for k in ("full_name", "first_name", "last_name", "position")):
                    continue
                rows.append((
                    pid,
                    d.get("full_name"),
                    d.get("first_name"),
                    d.get("last_name"),
                    d.get("position"),
                    d.get("shoots_catches"),
                    True
                ))
            # send in small chunks to avoid pooler overload
            for chunk_rows in chunk(rows, args.batch_size):
                if not chunk_rows:
                    continue
                upsert_batch(cur, conn, chunk_rows)
                conn.commit()
                total_upserted += len(chunk_rows)
            pending.clear()

        if i % 50 == 0:
            print(f"  …{i}/{len(games)} (upserted so far: {total_upserted})", flush=True)
        time.sleep(0.05)

    print(f"Upserted {total_upserted} players. ✅")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
