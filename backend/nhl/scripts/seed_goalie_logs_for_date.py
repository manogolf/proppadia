#!/usr/bin/env python3
"""
Seed goalie boxscore stats for SLATE_DATE into nhl.import_goalie_logs_stage.

Sources (in this order):
- Games (primary):   nhl.games for SLATE_DATE (DB)
- Games (fallback):  https://api-web.nhle.com/v1/schedule/YYYY-MM-DD

- Boxscore (primary & only): https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore
  (No statsapi.* usage)

Mapping:
- Prefer name match against nhl.roster_status for that game
- Fallback to nhl.player_external_ids (provider='nhl', provider_player_id)
"""

import os, sys, datetime as dt
from typing import Iterable, Tuple, Dict, Any, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg

# ---------------- Env & date ----------------
DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
if not DB_URL:
    print("Set SUPABASE_DB_URL or DATABASE_URL", file=sys.stderr); sys.exit(2)

SLATE_DATE = os.environ.get("SLATE_DATE")
if not SLATE_DATE:
    print("Set SLATE_DATE=YYYY-MM-DD", file=sys.stderr); sys.exit(2)
try:
    _ = dt.date.fromisoformat(SLATE_DATE)
except ValueError:
    print(f"Bad SLATE_DATE: {SLATE_DATE}", file=sys.stderr); sys.exit(2)

# Ensure ssl params for cloud PG if missing
if "?sslmode=" not in DB_URL and "&sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
if "?gssencmode=" not in DB_URL and "&gssencmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "gssencmode=disable"

API_SCHEDULE = "https://api-web.nhle.com/v1/schedule"
API_BOXSCORE = "https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore"

# ---------------- HTTP ----------------
def _session() -> requests.Session:
    r = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-goalie-seed"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s

S = _session()

# ---------------- Helpers ----------------
def toi_to_minutes(s: Optional[str]) -> Optional[float]:
    """
    Convert 'MM:SS' or 'HH:MM:SS' to minutes (float). Accept numeric minutes too.
    """
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if not s or s == "0":
        return 0.0
    parts = [p for p in s.split(":") if p != ""]
    try:
        if len(parts) == 3:
            h, m, sec = map(int, parts)
            return 60*h + m + sec/60.0
        if len(parts) == 2:
            m, sec = map(int, parts)
            return m + sec/60.0
        # fallback: plain number
        return float(s)
    except Exception:
        return None

def normalize_name(s: Optional[str]) -> str:
    return (s or "").strip().lower().replace("  ", " ")

# ---------------- DB lookups ----------------
def roster_name_map(conn, game_id: int) -> Dict[str, Tuple[int, int]]:
    """
    Return {normalized_full_name -> (player_id, team_id)} from roster for this game.
    """
    sql = """
      SELECT r.player_id, r.team_id, LOWER(REGEXP_REPLACE(p.full_name, '\s+', ' ', 'g')) AS nm
      FROM nhl.roster_status r
      JOIN nhl.players p ON p.player_id = r.player_id
      WHERE r.game_id = %s
    """
    mp = {}
    with conn.cursor() as cur:
        cur.execute(sql, (game_id,))
        for pid, tid, nm in cur.fetchall():
            mp[nm] = (int(pid), int(tid))
    return mp

def external_map(conn, nhl_ids: List[int]) -> Dict[int, int]:
    """
    Map NHL numeric IDs -> internal player_id using nhl.player_external_ids
    (provider='nhl', provider_player_id TEXT).
    """
    if not nhl_ids:
        return {}
    sql = """
      SELECT provider_player_id, player_id
      FROM nhl.player_external_ids
      WHERE provider = 'nhl' AND provider_player_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (list(map(str, nhl_ids)),))
        mp = {}
        for ext, pid in cur.fetchall():
            try:
                mp[int(ext)] = int(pid)
            except (TypeError, ValueError):
                pass
        return mp

def game_ids_from_db(conn, date_str: str) -> List[int]:
    sql = "SELECT game_id::bigint FROM nhl.games WHERE game_date = %s ORDER BY game_id"
    with conn.cursor() as cur:
        cur.execute(sql, (date_str,))
        return [int(r[0]) for r in cur.fetchall()]

def game_ids_from_api(date_str: str) -> List[int]:
    url = f"{API_SCHEDULE}/{date_str}"
    r = S.get(url, timeout=12); r.raise_for_status()
    data = r.json()
    games = []
    if isinstance(data, dict) and "gameWeek" in data:
        for day in data.get("gameWeek", []):
            for g in day.get("games", []):
                gid = g.get("id") or g.get("gamePk") or g.get("gameId")
                if gid: games.append(int(gid))
    elif isinstance(data, dict) and isinstance(data.get("games"), list):
        for g in data["games"]:
            gid = g.get("id") or g.get("gamePk") or g.get("gameId")
            if gid: games.append(int(gid))
    return sorted(set(games))

# ---------------- Boxscore (api-web) ----------------
def fetch_boxscore_api(game_pk: int) -> Dict[str, Any]:
    url = API_BOXSCORE.format(gamePk=int(game_pk))
    r = S.get(url, timeout=15); r.raise_for_status()
    return r.json()

def iter_goalies_from_box(box: Dict[str, Any]) -> Iterable[Tuple[Optional[int], str, Optional[int], Optional[int], Optional[float]]]:
    """
    Yield tuples: (nhl_player_id, full_name, saves, shots_against, toi_minutes)

    JSON shape (api-web) varies a bit; we target:
      box["playerByGameStats"]["homeTeam"]["goalies"]  (list/dict)
      box["playerByGameStats"]["awayTeam"]["goalies"]
    Each goalie item should have: playerId, timeOnIce, saves or shotsAgainst(+goalsAgainst)
    """
    pbgs = box.get("playerByGameStats") or {}
    for side_key in ("homeTeam", "awayTeam"):
        team = pbgs.get(side_key) or {}
        goalies = team.get("goalies") or team.get("goalie") or []
        if isinstance(goalies, dict):
            goalies = list(goalies.values())
        for g in goalies:
            nhl_id = g.get("playerId") or (g.get("player") or {}).get("id")
            # Try multiple name locations
            full_name = (
                (g.get("name") or {}).get("default") or
                (g.get("player") or {}).get("fullName") or
                g.get("fullName") or
                ""
            )
            saves = g.get("saves")
            shots_against = g.get("shotsAgainst") or g.get("shots")
            goals_against = g.get("goalsAgainst") or g.get("ga")
            if saves is None and shots_against is not None and goals_against is not None:
                try:
                    saves = int(shots_against) - int(goals_against)
                except Exception:
                    pass
            toi = toi_to_minutes(g.get("timeOnIce") or g.get("toi"))
            try:
                nhl_id_int = int(nhl_id) if nhl_id is not None else None
            except Exception:
                nhl_id_int = None
            yield nhl_id_int, full_name, (None if saves is None else int(saves)), \
                  (None if shots_against is None else int(shots_against)), toi

# ---------------- Upsert ----------------
def upsert_rows(conn, rows: List[Tuple[int,int,str,Optional[int],Optional[int],Optional[float]]]) -> int:
    """
    rows of: (player_id, game_id, game_date, saves, shots_faced, toi_minutes)
    """
    if not rows: return 0
    sql = """
    INSERT INTO nhl.import_goalie_logs_stage
      (player_id, game_id, game_date, saves, shots_faced, toi_minutes)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (player_id, game_id) DO UPDATE SET
      saves       = EXCLUDED.saves,
      shots_faced = EXCLUDED.shots_faced,
      toi_minutes = EXCLUDED.toi_minutes
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)

# ---------------- Main ----------------
def main():
    inserted_total = 0
    skipped_no_map = 0

    with psycopg.connect(DB_URL, autocommit=False) as conn:
        game_ids = game_ids_from_db(conn, SLATE_DATE)
        if not game_ids:
            game_ids = game_ids_from_api(SLATE_DATE)

        if not game_ids:
            print(f"No games on {SLATE_DATE}")
            return

        for gpk in game_ids:
            try:
                box = fetch_boxscore_api(gpk)
            except Exception as e:
                print(f"[{gpk}] boxscore fetch failed: {e}", file=sys.stderr)
                continue

            roster_map = roster_name_map(conn, gpk)  # name -> (player_id, team_id)

            # collect raw goalie rows from API
            raw_goalies = list(iter_goalies_from_box(box))
            nhl_ids = [int(x[0]) for x in raw_goalies if x[0] is not None]
            ext_map = external_map(conn, nhl_ids)

            rows = []
            for nhl_id, full_name, saves, shots_against, toi_min in raw_goalies:
                pid = None
                nm = normalize_name(full_name)
                # first: roster match
                if nm in roster_map:
                    pid = roster_map[nm][0]
                # fallback: external id
                if pid is None and nhl_id is not None:
                    pid = ext_map.get(int(nhl_id))
                if pid is None:
                    skipped_no_map += 1
                    continue
                rows.append((
                    int(pid), int(gpk), SLATE_DATE,
                    saves if saves is not None else None,
                    shots_against if shots_against is not None else None,
                    toi_min if toi_min is not None else None
                ))

            inserted = upsert_rows(conn, rows)
            conn.commit()
            inserted_total += inserted
            print(f"[{gpk}] upserted {inserted} goalie rows; skipped_no_map_so_far={skipped_no_map}")

    print(f"Done. Upserted total {inserted_total} goalie rows for {SLATE_DATE}; skipped_no_map={skipped_no_map}")

if __name__ == "__main__":
    main()
