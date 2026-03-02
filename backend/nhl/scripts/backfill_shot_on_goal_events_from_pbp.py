#!/usr/bin/env python3
"""Backfill exact NHL shot-on-goal events from gamecenter play-by-play."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

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

BASE_PBP = "https://api-web.nhle.com/v1/gamecenter"
CACHE_DIR = Path("backend/nhl/site/data/raw/gamecenter")
SECONDS_PER_PERIOD = 20 * 60

GAMES_SQL = """
SELECT game_id::bigint, game_date::date, season::int, home_team_id::int, away_team_id::int
FROM nhl.games
WHERE season = %s
  AND (%s::date IS NULL OR game_date >= %s::date)
  AND (%s::date IS NULL OR game_date <= %s::date)
ORDER BY game_date, game_id
"""

POS_SQL = """
SELECT player_id::bigint AS player_id,
       CASE WHEN COALESCE(NULLIF(BTRIM(position), ''), 'F') = 'D' THEN 'D'
            WHEN COALESCE(NULLIF(BTRIM(position), ''), 'F') = 'G' THEN 'G'
            ELSE 'F'
       END AS position_bucket
FROM nhl.players
"""


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
    s.headers.update({"User-Agent": "proppadia-nhl-shot-on-goal-backfill"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


S = _session()


def _load_or_fetch_pbp(game_id: int) -> dict | None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{game_id}.pbp.json"
    if path.exists():
        return json.loads(path.read_text())
    r = S.get(f"{BASE_PBP}/{game_id}/play-by-play", timeout=25)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    path.write_text(json.dumps(data))
    return data


def _mmss_to_sec(mmss: str | None) -> int | None:
    if not mmss or ":" not in mmss:
        return None
    try:
        mm, ss = mmss.split(":", 1)
        return int(mm) * 60 + int(ss)
    except Exception:
        return None


def _abs_sec(period: int | None, mmss: str | None) -> int | None:
    if period is None:
        return None
    t = _mmss_to_sec(mmss)
    if t is None:
        return None
    return (int(period) - 1) * SECONDS_PER_PERIOD + t


def _fetch_games(conn, season: int, from_date: str | None, to_date: str | None) -> List[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(GAMES_SQL, (season, from_date, from_date, to_date, to_date))
        return list(cur.fetchall() or [])


def _fetch_positions(conn) -> Dict[int, str]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(POS_SQL)
        return {int(r["player_id"]): str(r["position_bucket"]) for r in cur.fetchall() or []}


def _iter_sog_events(pbp: dict, game_row: dict, pos_map: Dict[int, str]) -> Iterator[Tuple]:
    plays = pbp.get("plays") or []
    home_team_id = int(game_row["home_team_id"])
    away_team_id = int(game_row["away_team_id"])
    for play in plays:
        if play.get("typeDescKey") != "shot-on-goal":
            continue
        details = play.get("details") or {}
        shooter_id = details.get("shootingPlayerId")
        shooting_team_id = details.get("eventOwnerTeamId")
        event_id = play.get("eventId")
        if shooter_id is None or shooting_team_id is None or event_id is None:
            continue
        try:
            shooter_id = int(shooter_id)
            shooting_team_id = int(shooting_team_id)
            event_id = int(event_id)
        except Exception:
            continue
        defending_team_id = away_team_id if shooting_team_id == home_team_id else home_team_id
        period_number = int((play.get("periodDescriptor") or {}).get("number") or 0) or None
        time_in_period = play.get("timeInPeriod")
        yield (
            int(game_row["game_id"]),
            event_id,
            int(game_row["season"]),
            game_row["game_date"],
            period_number,
            time_in_period,
            _abs_sec(period_number, time_in_period),
            play.get("situationCode"),
            details.get("shotType"),
            details.get("zoneCode"),
            shooter_id,
            shooting_team_id,
            pos_map.get(shooter_id, "F"),
            defending_team_id,
            details.get("goalieInNetId"),
        )


INSERT_SQL = """
INSERT INTO nhl.shot_on_goal_events (
  game_id,
  event_id,
  season,
  game_date,
  period_number,
  time_in_period,
  event_abs_sec,
  situation_code,
  shot_type,
  zone_code,
  shooting_player_id,
  shooting_team_id,
  shooter_position_bucket,
  defending_team_id,
  goalie_in_net_id
)
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (game_id, event_id) DO UPDATE
SET
  season = EXCLUDED.season,
  game_date = EXCLUDED.game_date,
  period_number = EXCLUDED.period_number,
  time_in_period = EXCLUDED.time_in_period,
  event_abs_sec = EXCLUDED.event_abs_sec,
  situation_code = EXCLUDED.situation_code,
  shot_type = EXCLUDED.shot_type,
  zone_code = EXCLUDED.zone_code,
  shooting_player_id = EXCLUDED.shooting_player_id,
  shooting_team_id = EXCLUDED.shooting_team_id,
  shooter_position_bucket = EXCLUDED.shooter_position_bucket,
  defending_team_id = EXCLUDED.defending_team_id,
  goalie_in_net_id = EXCLUDED.goalie_in_net_id
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill nhl.shot_on_goal_events from NHL play-by-play.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--limit-games", type=int, default=None)
    args = ap.parse_args()

    with psycopg.connect(
        DB_URL,
        autocommit=False,
        row_factory=dict_row,
        prepare_threshold=None,
        options="-c statement_timeout=0 -c lock_timeout=5000",
    ) as conn:
        games = _fetch_games(conn, args.season, args.from_date, args.to_date)
        if args.limit_games:
            games = games[: args.limit_games]
        pos_map = _fetch_positions(conn)
        processed_games = 0
        upserted = 0
        for game in games:
            pbp = _load_or_fetch_pbp(int(game["game_id"]))
            if not pbp:
                continue
            rows = list(_iter_sog_events(pbp, game, pos_map))
            if not rows:
                processed_games += 1
                continue
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, rows)
            conn.commit()
            processed_games += 1
            upserted += len(rows)
            print(
                f"[shot_on_goal_events] game_id={int(game['game_id'])} "
                f"date={game['game_date']} events={len(rows)}",
                flush=True,
            )

    print(
        f"[shot_on_goal_events] season={args.season} from={args.from_date} to={args.to_date} "
        f"processed_games={processed_games} upserted={upserted}",
        flush=True,
    )


if __name__ == "__main__":
    main()
