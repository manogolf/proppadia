#!/usr/bin/env python3
"""Backfill NHL skater `blocks` from the NHL gamecenter boxscore feed."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from typing import Iterator, List, Tuple

import psycopg
import requests
from psycopg.rows import dict_row
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass


DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
if not DB_URL:
    raise SystemExit("Set SUPABASE_DB_URL or DATABASE_URL")

if "?sslmode=" not in DB_URL and "&sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
if "?gssencmode=" not in DB_URL and "&gssencmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "gssencmode=disable"

BASE_SCHEDULE = "https://api-web.nhle.com/v1/schedule"
BASE_BOXSCORE = "https://api-web.nhle.com/v1/gamecenter"


def _session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-blocks-backfill"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


S = _session()


def _iter_dates(start: dt.date, end: dt.date) -> Iterator[str]:
    cur = start
    while cur <= end:
        yield cur.isoformat()
        cur += dt.timedelta(days=1)


def _get_schedule(date_str: str) -> List[int]:
    r = S.get(f"{BASE_SCHEDULE}/{date_str}", timeout=20)
    r.raise_for_status()
    data = r.json()
    games = []
    if isinstance(data, dict) and "gameWeek" in data:
        for day in data.get("gameWeek", []):
            if str(day.get("date") or "") != date_str:
                continue
            games.extend(day.get("games", []))
    else:
        games.extend((data or {}).get("games") or [])
    out: List[int] = []
    seen = set()
    for g in games:
        gid = g.get("id") or g.get("gamePk") or g.get("gameId")
        if gid is None:
            continue
        gid_i = int(gid)
        if gid_i in seen:
            continue
        seen.add(gid_i)
        out.append(gid_i)
    return out


def _get_boxscore(game_id: int) -> dict | None:
    r = S.get(f"{BASE_BOXSCORE}/{game_id}/boxscore", timeout=25)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def _iter_block_rows(box: dict) -> Iterator[Tuple[int, int]]:
    pstats = box.get("playerByGameStats") or {}
    for side_key in ("homeTeam", "awayTeam"):
        team = pstats.get(side_key) or {}
        for bucket in ("forwards", "defense"):
            for p in (team.get(bucket) or []):
                pid = p.get("playerId") or p.get("id")
                blocks = p.get("blockedShots")
                if pid is None or blocks is None:
                    continue
                try:
                    yield int(pid), int(blocks)
                except Exception:
                    continue


def _stage_has_blocks(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'nhl'
              AND table_name = 'import_skater_logs_stage'
              AND column_name = 'blocks'
            """
        )
        return cur.fetchone() is not None


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill nhl.skater_game_logs_raw.blocks from NHL boxscore blockedShots.")
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument(
        "--raw-only",
        action="store_true",
        help="Update nhl.skater_game_logs_raw only and skip import_skater_logs_stage.",
    )
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.from_date)
    end = dt.date.fromisoformat(args.to_date)
    if start > end:
        raise SystemExit("--from-date must be <= --to-date")

    game_count = 0
    updated_raw = 0
    updated_stage = 0

    with psycopg.connect(
        DB_URL,
        autocommit=False,
        row_factory=dict_row,
        prepare_threshold=None,
        options="-c statement_timeout=0 -c lock_timeout=5000",
    ) as conn:
        stage_has_blocks = (not args.raw_only) and _stage_has_blocks(conn)
        with conn.cursor() as cur:
            for date_str in _iter_dates(start, end):
                day_games = 0
                day_updated_raw = 0
                day_updated_stage = 0
                print(f"[blocks_backfill] begin date={date_str}", flush=True)
                for game_id in _get_schedule(date_str):
                    game_count += 1
                    day_games += 1
                    box = _get_boxscore(game_id)
                    if not box:
                        continue
                    rows = list(_iter_block_rows(box))
                    if not rows:
                        continue
                    for player_id, blocks in rows:
                        cur.execute(
                            """
                            UPDATE nhl.skater_game_logs_raw
                               SET blocks = %s
                             WHERE game_id = %s
                               AND player_id = %s
                            """,
                            (blocks, game_id, player_id),
                        )
                        raw_rows = cur.rowcount or 0
                        updated_raw += raw_rows
                        day_updated_raw += raw_rows
                        if stage_has_blocks:
                            cur.execute(
                                """
                                UPDATE nhl.import_skater_logs_stage
                                   SET blocks = %s
                                 WHERE game_id = %s
                                   AND player_id = %s
                                """,
                                (blocks, game_id, player_id),
                            )
                            stage_rows = cur.rowcount or 0
                            updated_stage += stage_rows
                            day_updated_stage += stage_rows
                conn.commit()
                print(
                    f"[blocks_backfill] date={date_str} games={day_games} "
                    f"updated_raw={day_updated_raw} updated_stage={day_updated_stage}",
                    flush=True,
                )

    print(
        f"[blocks_backfill] games={game_count} updated_raw={updated_raw} updated_stage={updated_stage} "
        f"range={args.from_date}..{args.to_date}",
        flush=True,
    )


if __name__ == "__main__":
    main()
