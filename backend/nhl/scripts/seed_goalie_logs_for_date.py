#!/usr/bin/env python3
import os, sys, math, datetime as dt
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple, Optional

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import psycopg

ET = ZoneInfo("America/New_York")
DATE = os.getenv("SLATE_DATE") or dt.datetime.now(ET).date().isoformat()

DB = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB and "&sslmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "sslmode=require"
if "?gssencmode=" not in DB and "&gssencmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "gssencmode=disable"

BOX = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
S = requests.Session()
S.headers.update({"User-Agent": "proppadia-nhl-goalie-seed"})
S.mount("https://", HTTPAdapter(max_retries=Retry(
    total=4, connect=4, read=4, backoff_factor=0.4,
    status_forcelist=[429,500,502,503,504],
    allowed_methods=frozenset({"GET"}),
    raise_on_status=False,
)))

def _toi_to_minutes(s: Optional[str]) -> Optional[float]:
    if not s or ":" not in s: return None
    try:
        mm, ss = s.split(":")
        return round(int(mm) + int(ss)/60.0, 2)
    except Exception:
        return None

def _split_pair(val: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    # API strings like "21/24" (saves/shots). Return (saves, shots)
    if not val or "/" not in val: return (None, None)
    a, b = val.split("/", 1)
    try: return (int(a), int(b))
    except Exception: return (None, None)

def fetch_goalies(gid: int) -> Tuple[List[Dict], List[Dict], int, int]:
    r = S.get(BOX.format(gid=gid), timeout=10)
    r.raise_for_status()
    js = r.json()

    pbgs = js.get("playerByGameStats") or {}
    home = pbgs.get("homeTeam") or {}
    away = pbgs.get("awayTeam") or {}

    home_id = (js.get("homeTeam") or {}).get("id") or home.get("id")
    away_id = (js.get("awayTeam") or {}).get("id") or away.get("id")

    h_goalies = home.get("goalies") or []
    a_goalies = away.get("goalies") or []
    return h_goalies, a_goalies, int(home_id), int(away_id)

def rows_for_game(gid: int, game_date: str) -> List[Tuple]:
    h_goalies, a_goalies, home_tid, away_tid = fetch_goalies(gid)
    out: List[Tuple] = []

    def add(side: str, g: Dict, team_id: int, opp_id: int, is_home: bool):
        pid = int(g.get("playerId"))
        saves = int(g.get("saves") or 0)
        shots = int(g.get("shotsAgainst") or 0)
        ga    = int(g.get("goalsAgainst") or 0)
        toi_m = _toi_to_minutes(g.get("toi"))
        # Per-strength shots (denominators)
        _, ev_shots = _split_pair(g.get("evenStrengthShotsAgainst"))
        _, pp_shots = _split_pair(g.get("powerPlayShotsAgainst"))
        _, sh_shots = _split_pair(g.get("shorthandedShotsAgainst"))
        started = bool(g.get("starter", False))
        start_prob = 1.0 if started else 0.0

        out.append((
            pid, gid, team_id, opp_id, is_home,
            shots, saves, ga, toi_m, start_prob, game_date,
            ev_shots, pp_shots, sh_shots, None, None
        ))

    for g in (h_goalies or []):
        add("HOME", g, home_tid, away_tid, True)
    for g in (a_goalies or []):
        add("AWAY", g, away_tid, home_tid, False)
    return out

def main():
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT game_id, home_team_id, away_team_id
            FROM nhl.games
            WHERE game_date = %s::date
              AND status = 'final'
            ORDER BY game_id
        """, (DATE,))
        games = [r[0] for r in cur.fetchall()]
        if not games:
            print(f"[seed_goalies] No FINAL games for {DATE}")
            return

        # stage → merge (stage table columns match this order)  ⬇︎
        # player_id, game_id, team_id, opponent_id, is_home, shots_faced, saves,
        # goals_allowed, toi_minutes, start_prob, game_date, ev_shots_faced,
        # pp_shots_faced, sh_shots_faced, high_danger_shots_faced, rebounds_allowed
        # (as defined in your schema). :contentReference[oaicite:0]{index=0}
        cur.execute("TRUNCATE nhl.import_goalie_logs_stage")
        staged = 0
        for gid in games:
            try:
                rows = rows_for_game(gid, DATE)
                if not rows:
                    print(f"[{gid}] no goalie rows in boxscore")
                    continue
                cur.executemany("""
                    INSERT INTO nhl.import_goalie_logs_stage
                    (player_id, game_id, team_id, opponent_id, is_home,
                     shots_faced, saves, goals_allowed, toi_minutes,
                     start_prob, game_date,
                     ev_shots_faced, pp_shots_faced, sh_shots_faced,
                     high_danger_shots_faced, rebounds_allowed)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, rows)
                staged += len(rows)
                print(f"[{gid}] staged {len(rows)} goalie rows")
            except Exception as e:
                print(f"[{gid}] error: {e}")

        # merge into raw with upsert on (player_id, game_id)  ⬇︎
        # (that’s the PK in your schema). :contentReference[oaicite:1]{index=1}
        cur.execute("""
            INSERT INTO nhl.goalie_game_logs_raw
            (player_id, game_id, team_id, opponent_id, is_home,
             shots_faced, saves, goals_allowed, toi_minutes,
             start_prob, game_date,
             ev_shots_faced, pp_shots_faced, sh_shots_faced,
             high_danger_shots_faced, rebounds_allowed)
            SELECT
             s.player_id, s.game_id, s.team_id, s.opponent_id, s.is_home,
             s.shots_faced, s.saves, s.goals_allowed, s.toi_minutes,
             s.start_prob, s.game_date,
             s.ev_shots_faced, s.pp_shots_faced, s.sh_shots_faced,
             s.high_danger_shots_faced, s.rebounds_allowed
            FROM nhl.import_goalie_logs_stage s
            ON CONFLICT (player_id, game_id) DO UPDATE SET
             team_id = EXCLUDED.team_id,
             opponent_id = EXCLUDED.opponent_id,
             is_home = EXCLUDED.is_home,
             shots_faced = EXCLUDED.shots_faced,
             saves = EXCLUDED.saves,
             goals_allowed = EXCLUDED.goals_allowed,
             toi_minutes = EXCLUDED.toi_minutes,
             start_prob = COALESCE(EXCLUDED.start_prob, nhl.goalie_game_logs_raw.start_prob),
             game_date = EXCLUDED.game_date,
             ev_shots_faced = COALESCE(EXCLUDED.ev_shots_faced, nhl.goalie_game_logs_raw.ev_shots_faced),
             pp_shots_faced = COALESCE(EXCLUDED.pp_shots_faced, nhl.goalie_game_logs_raw.pp_shots_faced),
             sh_shots_faced = COALESCE(EXCLUDED.shots_faced, nhl.goalie_game_logs_raw.sh_shots_faced),
             high_danger_shots_faced = COALESCE(EXCLUDED.high_danger_shots_faced, nhl.goalie_game_logs_raw.high_danger_shots_faced),
             rebounds_allowed = COALESCE(EXCLUDED.rebounds_allowed, nhl.goalie_game_logs_raw.rebounds_allowed)
        """)
        print(f"done. upserted total {staged} staged rows for {DATE}")
        conn.commit()

if __name__ == "__main__":
    main()
