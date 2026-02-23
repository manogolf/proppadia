#!/usr/bin/env python3
"""
backfill_game_manpower_segments.py

Backfill team-level PP/PK manpower segments into Postgres from NHL play-by-play
(api-web.nhle.com), using situationCode skater counts (both goalies in net).

Writes to: nhl.game_manpower_segments
  (game_id, period, start_sec, end_sec, pp_team_id, pk_team_id, source)

IMPORTANT:
- shiftcharts_shifts uses period-relative seconds (0..1200). This script stores segments
  as (period, start_sec, end_sec) to match that.
- Segments are inferred from situationCode transitions (diagnostic-grade but effective).
- NON_STD states (goalie pulled, etc.) are excluded.

Usage examples:
  # By date range (ET date stored in nhl.games.game_date)
  SUPABASE_DB_URL="postgresql://..." \
    python backend/nhl/scripts/backfill_game_manpower_segments.py \
      --start-date 2025-10-07 --end-date 2026-01-08 --season 2025

  # By explicit game_ids
  SUPABASE_DB_URL="postgresql://..." \
    python backend/nhl/scripts/backfill_game_manpower_segments.py \
      --game-ids 2025020694 2025020695

  # Dry run (no DB writes)
  python ... --start-date 2025-12-01 --end-date 2025-12-07 --season 2025 --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg
import requests

API = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
SRC = "pbp_situation_breakdown"

SECONDS_PER_PERIOD = 20 * 60  # 1200


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def mmss_to_sec(mmss: str) -> Optional[int]:
    try:
        m, s = mmss.strip().split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None


def abs_sec(period: int, mmss: str) -> Optional[int]:
    t = mmss_to_sec(mmss)
    if t is None:
        return None
    return (int(period) - 1) * SECONDS_PER_PERIOD + t


def pbp_event_abs_sec(ev: Dict[str, Any]) -> Optional[int]:
    pd = ev.get("periodDescriptor") or {}
    per = pd.get("number")
    tip = ev.get("timeInPeriod")
    if per is None or not tip:
        return None
    return abs_sec(int(per), str(tip))


@dataclass(frozen=True)
class SitCtx:
    away_skaters: int
    home_skaters: int
    away_goalie: int
    home_goalie: int


def parse_situation_code(sc: str) -> Optional[SitCtx]:
    """
    situationCode digits: AG AS HS HG
      AG: away goalie present (1) / pulled (0)
      AS: away skaters (3-6)
      HS: home skaters (3-6)
      HG: home goalie present (1) / pulled (0)
    """
    if not sc or len(sc) != 4 or not sc.isdigit():
        return None
    ag = int(sc[0])
    a_sk = int(sc[1])
    h_sk = int(sc[2])
    hg = int(sc[3])
    return SitCtx(away_skaters=a_sk, away_goalie=ag, home_skaters=h_sk, home_goalie=hg)


def advantage_label(ctx: SitCtx) -> str:
    # Only treat manpower advantage when both goalies present
    if ctx.away_goalie != 1 or ctx.home_goalie != 1:
        return "NON_STD"
    if ctx.home_skaters > ctx.away_skaters:
        return "HOME_ADV"
    if ctx.away_skaters > ctx.home_skaters:
        return "AWAY_ADV"
    return "EVEN"


def fetch_pbp(game_id: int, timeout_s: int = 30) -> Dict[str, Any]:
    url = API.format(game_id=game_id)
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def get_team_ids(pbp: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    home = (pbp.get("homeTeam") or {}).get("id")
    away = (pbp.get("awayTeam") or {}).get("id")
    return home, away


def iter_plays(pbp: Dict[str, Any]) -> List[Dict[str, Any]]:
    plays = pbp.get("plays")
    if not isinstance(plays, list):
        return []
    return plays


def segments_for_team_advantage_abs(
    plays: List[Dict[str, Any]],
    which: str,  # {"HOME_ADV","AWAY_ADV"}
) -> List[Tuple[int, int]]:
    """
    Returns list of [start_abs_sec, end_abs_sec) advantage segments, both-goalies-only.
    Built by diffing consecutive events (same basis as your diagnostics).
    """
    rows: List[Tuple[int, str]] = []
    for ev in plays:
        t = pbp_event_abs_sec(ev)
        if t is None:
            continue
        sc = ev.get("situationCode")
        if not sc:
            continue
        rows.append((t, str(sc)))

    rows.sort(key=lambda x: x[0])
    if len(rows) < 2:
        return []

    segs: List[Tuple[int, int]] = []
    cur_start: Optional[int] = None
    cur_on = False

    for i in range(len(rows) - 1):
        t0, sc0 = rows[i]
        t1, _ = rows[i + 1]
        dt = max(0, t1 - t0)
        if dt <= 0:
            continue

        ctx = parse_situation_code(sc0)
        if ctx is None:
            continue
        lab = advantage_label(ctx)
        on = (lab == which)

        if on and not cur_on:
            cur_start = t0
            cur_on = True
        if (not on) and cur_on:
            segs.append((cur_start if cur_start is not None else t0, t0))
            cur_start = None
            cur_on = False

    # Close open segment at last known timestamp (best-effort)
    if cur_on and cur_start is not None:
        last_t = rows[-1][0]
        if last_t > cur_start:
            segs.append((cur_start, last_t))

    # Merge overlaps/adjacent
    segs.sort()
    merged: List[Tuple[int, int]] = []
    for s, e in segs:
        if e <= s:
            continue
        if not merged:
            merged.append((s, e))
            continue
        ps, pe = merged[-1]
        if s <= pe:
            merged[-1] = (ps, max(pe, e))
        elif s - pe <= 1:
            merged[-1] = (ps, e)
        else:
            merged.append((s, e))
    return merged


def split_abs_segment_to_period_rows(
    abs_start: int,
    abs_end: int,
) -> List[Tuple[int, int, int]]:
    """
    Convert [abs_start, abs_end) into a list of (period, start_sec, end_sec) rows,
    where start_sec/end_sec are within-period seconds [0..1200].
    """
    out: List[Tuple[int, int, int]] = []
    s = abs_start
    e = abs_end
    while s < e:
        period = (s // SECONDS_PER_PERIOD) + 1
        period_start_abs = (period - 1) * SECONDS_PER_PERIOD
        period_end_abs = period * SECONDS_PER_PERIOD
        seg_end_abs = min(e, period_end_abs)

        start_sec = s - period_start_abs
        end_sec = seg_end_abs - period_start_abs
        # Clamp end_sec at 1200 for exact boundary
        if end_sec > SECONDS_PER_PERIOD:
            end_sec = SECONDS_PER_PERIOD

        if end_sec > start_sec:
            out.append((period, int(start_sec), int(end_sec)))

        s = seg_end_abs
    return out


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nhl.game_manpower_segments (
              game_id      bigint   NOT NULL,
              period       int      NOT NULL,
              start_sec    int      NOT NULL,
              end_sec      int      NOT NULL,
              pp_team_id   int      NOT NULL,
              pk_team_id   int      NOT NULL,
              source       text     NOT NULL DEFAULT 'pbp',
              created_at   timestamptz NOT NULL DEFAULT now(),
              PRIMARY KEY (game_id, period, start_sec, end_sec, pp_team_id)
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_game_manpower_segments_game ON nhl.game_manpower_segments (game_id);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_game_manpower_segments_team ON nhl.game_manpower_segments (pp_team_id, pk_team_id);"
        )
    conn.commit()


def fetch_game_ids_from_db(conn, start_date: str, end_date: str, season: int, limit: Optional[int]) -> List[int]:
    q = """
        SELECT g.game_id::bigint
        FROM nhl.games g
        WHERE g.season::int = %s
          AND g.game_date >= %s::date
          AND g.game_date <= %s::date
        ORDER BY g.game_date, g.game_id
    """
    if limit is not None and limit > 0:
        q += " LIMIT %s"
        params = (season, start_date, end_date, limit)
    else:
        params = (season, start_date, end_date)

    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def upsert_segments(conn, rows: List[Tuple[int, int, int, int, int, int, str]]) -> int:
    """
    rows: (game_id, period, start_sec, end_sec, pp_team_id, pk_team_id, source)
    Returns inserted rowcount (best-effort; ON CONFLICT DO NOTHING means rowcount may underreport).
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO nhl.game_manpower_segments
          (game_id, period, start_sec, end_sec, pp_team_id, pk_team_id, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        for i in range(0, len(rows), 5000):
            cur.executemany(sql, rows[i : i + 5000])
        # rowcount is only rows affected by last execute; good enough for logging
        rc = cur.rowcount
    conn.commit()
    return int(rc)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", help="YYYY-MM-DD (inclusive; uses nhl.games.game_date)")
    ap.add_argument("--end-date", help="YYYY-MM-DD (inclusive; uses nhl.games.game_date)")
    ap.add_argument("--season", type=int, default=2025, help="season int as stored in nhl.games.season (default: 2025)")
    ap.add_argument("--game-ids", nargs="*", type=int, help="explicit game_ids (overrides date range if provided)")
    ap.add_argument("--limit", type=int, default=None, help="limit games (date-range mode)")
    ap.add_argument("--sleep", type=float, default=0.0, help="sleep seconds between API calls (rate-limit cushion)")
    ap.add_argument("--dry-run", action="store_true", help="do not write to DB")
    args = ap.parse_args()

    dsn = os.environ.get("SUPABASE_DB_URL")
    needs_db_for_game_list = (not args.game_ids)

    # In date-range mode we need DB access even in --dry-run to discover game_ids.
    if needs_db_for_game_list and not dsn:
        die("SUPABASE_DB_URL is required for date-range mode (even with --dry-run).")

    conn = None
    if dsn and (needs_db_for_game_list or (not args.dry_run)):
        conn = psycopg.connect(dsn, prepare_threshold=None)
        try:
            conn.prepare_threshold = None  # type: ignore[attr-defined]
        except Exception:
            pass

    # Only ensure/create table when we will write.
    if conn is not None and (not args.dry_run):
        ensure_table(conn)

    game_ids: List[int]
    if args.game_ids:
        game_ids = list(dict.fromkeys(args.game_ids))  # dedupe, keep order
    else:
        assert conn is not None
        game_ids = fetch_game_ids_from_db(conn, args.start_date, args.end_date, args.season, args.limit)

    if not game_ids:
        print("No game_ids found. Nothing to do.")
        return

    print(f"games_to_process={len(game_ids)} season={args.season} dry_run={args.dry_run}")

    total_rows_prepared = 0
    total_rows_inserted = 0
    total_games_ok = 0
    total_games_err = 0

    for i, gid in enumerate(game_ids, 1):
        try:
            pbp = fetch_pbp(gid)
            plays = iter_plays(pbp)
            home_id, away_id = get_team_ids(pbp)
            if home_id is None or away_id is None:
                raise RuntimeError(f"Missing home/away team ids for game {gid}")

            # HOME_ADV => home team is on PP
            home_abs = segments_for_team_advantage_abs(plays, "HOME_ADV")
            # AWAY_ADV => away team is on PP
            away_abs = segments_for_team_advantage_abs(plays, "AWAY_ADV")

            rows: List[Tuple[int, int, int, int, int, int, str]] = []

            for s_abs, e_abs in home_abs:
                for period, s_sec, e_sec in split_abs_segment_to_period_rows(s_abs, e_abs):
                    rows.append((gid, period, s_sec, e_sec, int(home_id), int(away_id), SRC))

            for s_abs, e_abs in away_abs:
                for period, s_sec, e_sec in split_abs_segment_to_period_rows(s_abs, e_abs):
                    rows.append((gid, period, s_sec, e_sec, int(away_id), int(home_id), SRC))

            total_rows_prepared += len(rows)

            if args.dry_run:
                print(f"[{i}/{len(game_ids)}] game_id={gid} plays={len(plays)} seg_rows={len(rows)} (dry-run)")
                total_games_ok += 1
            else:
                assert conn is not None
                inserted = upsert_segments(conn, rows)
                total_rows_inserted += inserted
                print(
                    f"[{i}/{len(game_ids)}] game_id={gid} plays={len(plays)} seg_rows={len(rows)} inserted={inserted}"
                )
                total_games_ok += 1

            if args.sleep and args.sleep > 0:
                time.sleep(args.sleep)

        except Exception as e:
            total_games_err += 1
            print(f"[{i}/{len(game_ids)}] game_id={gid} ERROR: {e}", file=sys.stderr)
            # continue processing other games

    if conn is not None:
        conn.close()

    print("\n--- done ---")
    print(
        f"games_ok={total_games_ok} games_err={total_games_err} "
        f"rows_prepared={total_rows_prepared} rows_inserted={total_rows_inserted}"
    )
    if args.dry_run:
        print("NOTE: dry-run mode does not write to DB.")


if __name__ == "__main__":
    main()
