#!/usr/bin/env python3
"""
build_game_manpower_segments_for_date.py

Daily producer for team-level PP/PK manpower segments into Postgres from NHL play-by-play
(api-web.nhle.com), using situationCode skater counts (both goalies in net).

Writes to: nhl.game_manpower_segments
  (game_id, period, start_sec, end_sec, pp_team_id, pk_team_id, source)

Design notes (kept compatible with existing schema & downstream):
- Segments are stored as (period, start_sec, end_sec) with start/end in [0..1200],
  to match SHIFTCHARTS period-relative seconds.
- Segments are inferred from situationCode transitions (both goalies present).
- NON_STD states (goalie pulled, etc.) are excluded.

Typical daily usage:
  SUPABASE_DB_URL="postgresql://..." \
    SLATE_DATE=2026-01-10 \
    python backend/nhl/scripts/build_game_manpower_segments_for_date.py --commit --rebuild

Or:
  python ... --date 2026-01-10 --commit --rebuild

Dry run:
  python ... --date 2026-01-10
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
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
    if not sc or len(sc) != 4 or (not sc.isdigit()):
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


def iter_plays(pbp: Dict[str, Any]) -> List[Dict[str, Any]]:
    plays = pbp.get("plays")
    if not isinstance(plays, list):
        return []
    return plays


def get_team_ids(pbp: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    home = (pbp.get("homeTeam") or {}).get("id")
    away = (pbp.get("awayTeam") or {}).get("id")
    return home, away


def segments_for_team_advantage_abs(
    plays: List[Dict[str, Any]],
    which: str,  # {"HOME_ADV","AWAY_ADV"}
) -> List[Tuple[int, int]]:
    """
    Returns list of [start_abs_sec, end_abs_sec) advantage segments, both-goalies-only.
    Built by diffing consecutive events.
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
        dt = t1 - t0
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


def split_abs_segment_to_period_rows(abs_start: int, abs_end: int) -> List[Tuple[int, int, int]]:
    """
    Convert [abs_start, abs_end) into (period, start_sec, end_sec) where start/end are within-period [0..1200].
    """
    out: List[Tuple[int, int, int]] = []
    s, e = abs_start, abs_end
    while s < e:
        period = (s // SECONDS_PER_PERIOD) + 1
        period_start_abs = (period - 1) * SECONDS_PER_PERIOD
        period_end_abs = period * SECONDS_PER_PERIOD
        seg_end_abs = min(e, period_end_abs)

        start_sec = s - period_start_abs
        end_sec = seg_end_abs - period_start_abs
        if end_sec > SECONDS_PER_PERIOD:
            end_sec = SECONDS_PER_PERIOD

        if end_sec > start_sec:
            out.append((int(period), int(start_sec), int(end_sec)))
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
        cur.execute("CREATE INDEX IF NOT EXISTS ix_game_manpower_segments_game ON nhl.game_manpower_segments (game_id);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_game_manpower_segments_team ON nhl.game_manpower_segments (pp_team_id, pk_team_id);"
        )
    conn.commit()


def fetch_game_ids_for_date(conn, game_date: str, season: Optional[int]) -> List[int]:
    q = """
        SELECT g.game_id::bigint
        FROM nhl.games g
        WHERE g.game_date = %s::date
    """
    params: List[Any] = [game_date]
    if season is not None:
        q += " AND g.season::int = %s"
        params.append(int(season))
    q += " ORDER BY g.game_id"
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def delete_segments_for_date(conn, game_date: str) -> int:
    sql = """
        DELETE FROM nhl.game_manpower_segments m
        USING nhl.games g
        WHERE g.game_id = m.game_id
          AND g.game_date = %s::date;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (game_date,))
        rc = cur.rowcount
    conn.commit()
    return int(rc)


def fetch_pbp_with_retry(session: requests.Session, game_id: int, timeout_s: int, retries: int, sleep_s: float) -> Dict[str, Any]:
    url = API.format(game_id=game_id)
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout_s)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(sleep_s * (attempt + 1))
                continue
            raise
    assert last_err is not None
    raise last_err


def insert_segments(conn, rows: List[Tuple[int, int, int, int, int, int, str]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO nhl.game_manpower_segments
          (game_id, period, start_sec, end_sec, pp_team_id, pk_team_id, source)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=5000)
        rc = cur.rowcount
    return int(rc)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (ET date stored in nhl.games.game_date). If omitted, uses SLATE_DATE env.")
    ap.add_argument("--season", type=int, default=None, help="optional season filter (matches nhl.games.season)")
    ap.add_argument("--commit", action="store_true", help="write to DB (default: dry-run)")
    ap.add_argument("--rebuild", action="store_true", help="delete existing rows for this date first (recommended for daily)")
    ap.add_argument("--sleep", type=float, default=0.0, help="sleep seconds between API calls")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds (default: 30)")
    ap.add_argument("--retries", type=int, default=2, help="HTTP retries per game (default: 2)")
    ap.add_argument("--retry-sleep", type=float, default=1.5, help="base sleep between retries (default: 1.5)")
    ap.add_argument("--fail-if-empty", action="store_true", help="exit non-zero if segments inserted=0 while games>0")
    args = ap.parse_args()

    game_date = args.date or os.environ.get("SLATE_DATE")
    if not game_date:
        die("Provide --date YYYY-MM-DD or set SLATE_DATE env (ET).")

    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        die("SUPABASE_DB_URL is required.")

    conn = psycopg2.connect(dsn)

    if args.commit:
        ensure_table(conn)

    game_ids = fetch_game_ids_for_date(conn, game_date, args.season)
    if not game_ids:
        print(f"[manpower] date={game_date} games=0 (nothing to do)")
        conn.close()
        return

    if args.commit and args.rebuild:
        deleted = delete_segments_for_date(conn, game_date)
        print(f"[manpower] date={game_date} delete_first deleted_rows={deleted}")

    session = requests.Session()

    total_prepared = 0
    total_inserted = 0
    games_ok = 0
    games_err = 0

    all_rows: List[Tuple[int, int, int, int, int, int, str]] = []

    print(f"[manpower] date={game_date} games={len(game_ids)} commit={args.commit} rebuild={args.rebuild}")

    for i, gid in enumerate(game_ids, 1):
        try:
            pbp = fetch_pbp_with_retry(session, gid, args.timeout, args.retries, args.retry_sleep)
            plays = iter_plays(pbp)
            home_id, away_id = get_team_ids(pbp)
            if home_id is None or away_id is None:
                raise RuntimeError("missing home/away team ids in pbp")

            # HOME_ADV => home team on PP
            home_abs = segments_for_team_advantage_abs(plays, "HOME_ADV")
            # AWAY_ADV => away team on PP
            away_abs = segments_for_team_advantage_abs(plays, "AWAY_ADV")

            rows: List[Tuple[int, int, int, int, int, int, str]] = []

            for s_abs, e_abs in home_abs:
                for period, s_sec, e_sec in split_abs_segment_to_period_rows(s_abs, e_abs):
                    rows.append((gid, period, s_sec, e_sec, int(home_id), int(away_id), SRC))

            for s_abs, e_abs in away_abs:
                for period, s_sec, e_sec in split_abs_segment_to_period_rows(s_abs, e_abs):
                    rows.append((gid, period, s_sec, e_sec, int(away_id), int(home_id), SRC))

            total_prepared += len(rows)
            all_rows.extend(rows)

            print(f"[manpower] [{i}/{len(game_ids)}] game_id={gid} plays={len(plays)} seg_rows={len(rows)}")
            games_ok += 1

            if args.sleep and args.sleep > 0:
                time.sleep(args.sleep)

        except Exception as e:
            games_err += 1
            print(f"[manpower] [{i}/{len(game_ids)}] game_id={gid} ERROR: {e}", file=sys.stderr)

    if args.commit:
        inserted = insert_segments(conn, all_rows)
        conn.commit()
        total_inserted = inserted

    conn.close()

    print(f"[manpower] done date={game_date} games_ok={games_ok} games_err={games_err} rows_prepared={total_prepared} rows_inserted={total_inserted}")

    if args.fail_if_empty and (len(game_ids) > 0) and (total_inserted == 0) and args.commit:
        die(f"[manpower] FAIL: games={len(game_ids)} but rows_inserted=0 for date={game_date}")

    if not args.commit:
        print("[manpower] NOTE: dry-run mode (no DB writes). Use --commit to write.")


if __name__ == "__main__":
    main()
