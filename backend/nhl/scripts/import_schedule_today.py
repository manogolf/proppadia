#!/usr/bin/env python3
"""
Import NHL schedule for SLATE_DATE (ET) into staging and merge.
- Endpoint: https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
- Interprets the day in America/New_York (ET).
- Self-heals: upserts real team rows into nhl.teams and seeds nhl.team_external_ids.
"""

import os, requests, time
import sys
import json
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Iterable

# Optional: load .env (so SUPABASE_DB_URL / DATABASE_URL works locally)
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

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

BASE_URL = os.getenv("NHL_API_BASE", "https://api-web.nhle.com") + "/v1/schedule"


# ---------------- HTTP helpers ----------------

def _session() -> requests.Session:
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-cron"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _to_et_date(iso_utc: str) -> str | None:
    if not iso_utc:
        return None
    try:
        return dt.datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(ET).date().isoformat()
    except Exception:
        return None

def fetch_schedule_for_date(date_str: str) -> list[dict]:
    """Fetch the schedule and keep only games whose start time falls on date_str in ET."""
    s = _session()
    candidates = [
        f"{BASE_URL}/{date_str}",
        f"{BASE_URL}?date={date_str}",
        # stats REST fallback (kept last)
        f"https://api.nhle.com/stats/rest/en/schedule?cayenneExp=gameDate=%22{date_str}%22",
    ]
    data = None
    last_err = None
    for url in candidates:
        try:
            r = s.get(url, timeout=12); r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            last_err = e
            continue
    if data is None:
        raise RuntimeError(f"No schedule JSON for {date_str} (last error: {last_err})")

    # flatten games from gameWeek[] or top-level games
    games_in: list[dict] = []
    if isinstance(data, dict) and "gameWeek" in data:
        for day in data.get("gameWeek", []):
            games_in.extend(day.get("games", []))
    elif isinstance(data, dict) and isinstance(data.get("games"), list):
        games_in = data["games"]

    out, seen = [], set()
    # Handle both api-web shape and stats REST fallback
    # api-web: items sit in `games_in` (from gameWeek[...] or top-level games)
    # stats REST: when we hit the fallback URL, `data.get("data")` has rows
    if isinstance(data, dict) and "data" in data:
        games_in = data["data"]
        for g in games_in:
            start_iso = g.get("startTimeUTC") or g.get("gameDate")
            if _to_et_date(start_iso) != date_str:
                continue
            gid = g.get("gameId") or g.get("id")
            if gid in seen: 
                continue
            out.append({
                "id": gid,
                "startTimeUTC": start_iso,
                "homeTeam": {"abbrev": g.get("homeTeamAbbrev") or g.get("homeTeamCode")},
                "awayTeam": {"abbrev": g.get("awayTeamAbbrev") or g.get("awayTeamCode")},
                "gameState": g.get("gameState") or g.get("state"),
            })
            seen.add(gid)
    else:
        for g in games_in:
            start_iso = g.get("startTimeUTC") or g.get("gameDate")
            if _to_et_date(start_iso) != date_str:
                continue
            gid = g.get("id") or g.get("gamePk") or g.get("gameId")
            if gid in seen:
                continue
            out.append(g)
            seen.add(gid)
    return out

def get_schedule(date_str: str) -> list[dict]:
    """
    Normalize to legacy-like shape used elsewhere:
      [{"gamePk": <int>, "gameDate": <iso>, 
        "teams": {"home":{"team":{"abbreviation": "XYZ"}},
                  "away":{"team":{"abbreviation": "ABC"}}},
        "gameState": "..."}]
    """
    games = fetch_schedule_for_date(date_str)
    norm = []
    for g in games:
        gid = g.get("gamePk") or g.get("id") or g.get("gameId")
        try:
            gid_int = int(str(gid))
        except Exception:
            gid_int = gid
        home_abbr = (g.get("homeTeam") or {}).get("abbrev") \
                    or ((g.get("teams") or {}).get("home") or {}).get("team", {}).get("abbreviation")
        away_abbr = (g.get("awayTeam") or {}).get("abbrev") \
                    or ((g.get("teams") or {}).get("away") or {}).get("team", {}).get("abbreviation")
        norm.append({
            "gamePk": gid_int,
            "gameDate": g.get("startTimeUTC") or g.get("gameDate"),
            "gameState": g.get("gameState") or g.get("state"),
            "teams": {
                "home": {"team": {"abbreviation": home_abbr}},
                "away": {"team": {"abbreviation": away_abbr}},
            },
        })
    return norm

def _map_game_state(g: dict) -> str | None:
    """
    Map NHL schedule state -> your games.status enum
    Allowed: scheduled | live | final | postponed | canceled
    """
    s = (g.get("gameState") or g.get("state") or "").upper()
    if s in {"FUT", "PRE"}:       return "scheduled"
    if s in {"LIVE"}:             return "live"
    if s in {"FINAL", "END", "OFF"}:  # OFF/END seen post-game; treat as final
                                   return "final"
    if s in {"POSTPONED"}:        return "postponed"
    if s in {"CANCELED", "CANCELLED"}: return "canceled"
    return None

# --------------- Team helpers (schema-aware) ---------------

def _team_block(g: dict, role: str) -> dict:
    # role in {"home","away"}; supports both new and legacy shapes
    if f"{role}Team" in g:
        blk = g.get(f"{role}Team") or {}
        return blk.get("team", blk) if isinstance(blk, dict) else {}
    teams = g.get("teams") or {}
    side = teams.get(role) or {}
    return side.get("team", side) if isinstance(side, dict) else {}

def _extract_provider_team_id_and_teamobj(g: dict, role: str) -> tuple[str | None, dict]:
    t = _team_block(g, role)
    pid = t.get("id")
    return (str(pid) if pid is not None else None, t)

def _api_team_abbr(team: dict) -> str | None:
    return team.get("abbrev") or team.get("triCode") or team.get("code")

def _api_team_full_name(team: dict) -> str | None:
    """
    Prefer 'name' when present; otherwise compose place + teamName.
    Handles multi-language fields like { default: "Vegas" }.
    """
    val = team.get("name")
    if isinstance(val, str) and val.strip():
        return val.strip()

    def _ml(obj, *keys):
        if not isinstance(obj, dict):
            return None
        for k in keys:
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    place = _ml(team.get("placeName") or {}, "default", "en", "English")
    tname = team.get("teamName")
    if isinstance(tname, str) and tname.strip():
        tname = tname.strip()
    else:
        tname = None
    if place and tname:
        return f"{place} {tname}"
    return tname or place  # last resort

def _ensure_teams_exist(cur, games: Iterable[dict]) -> int:
    """
    Ensure nhl.teams has correct rows (team_id, name, abbr) for any teams seen in `games`.
    Uses provider id as team_id. Returns count of upserts performed.
    """
    seen: set[int] = set()
    upserts = 0
    for g in games:
        for role in ("home", "away"):
            pid_str, team_obj = _extract_provider_team_id_and_teamobj(g, role)
            if not pid_str:
                continue
            pid = int(pid_str)
            if pid in seen:
                continue
            seen.add(pid)

            abbr = _api_team_abbr(team_obj)
            name = _api_team_full_name(team_obj)
            if not abbr or not name:
                # if we can't determine both, skip; mapping may still succeed if already present
                continue

            # Upsert into nhl.teams; this will convert your placeholder rows (e.g., team_id=7, abbr='T7')
            # into real rows (team_id=7, abbr='BUF', name='Buffalo Sabres'), satisfying the UNIQUE(abbr) constraint.
            cur.execute("""
                insert into nhl.teams (team_id, name, abbr)
                values (%s, %s, %s)
                on conflict (team_id)
                do update set name = excluded.name, abbr = excluded.abbr
            """, (pid, name, abbr))
            upserts += 1
    if upserts:
        print(f"🔧 Upserted/confirmed {upserts} team rows in nhl.teams")
    return upserts

def _ensure_team_mappings(cur, games: Iterable[dict]) -> dict[str, int]:
    """
    Ensure nhl.team_external_ids has provider->internal mappings.
    After _ensure_teams_exist, internal team_id == provider id; we simply upsert mapping.
    Returns provider_id(text) -> team_id(int).
    """
    cur.execute("""
        select provider_team_id::text, team_id
        from nhl.team_external_ids
        where provider = 'nhl'
    """)
    team_map = {pid: tid for (pid, tid) in cur.fetchall()}

    seeded = 0
    for g in games:
        for role in ("home", "away"):
            pid_str, _team_obj = _extract_provider_team_id_and_teamobj(g, role)
            if not pid_str:
                continue
            if pid_str in team_map:
                continue

            # Internal team_id equals provider id (we just upserted/updated that row in nhl.teams)
            tid = int(pid_str)
            cur.execute("select 1 from nhl.teams where team_id=%s limit 1", (tid,))
            if cur.fetchone() is None:
                # Shouldn't happen, but be defensive
                continue

            cur.execute("""
                insert into nhl.team_external_ids (team_id, provider, provider_team_id)
                values (%s, 'nhl', %s)
                on conflict (provider, provider_team_id)
                do update set team_id = excluded.team_id
            """, (tid, pid_str))
            team_map[pid_str] = tid
            seeded += 1

    if seeded:
        print(f"🔄 Seeded {seeded} team mappings into nhl.team_external_ids (provider='nhl')")
    return team_map


# --------------------- Main ---------------------

def main():
    games = fetch_schedule_for_date(DATE)
    if not games:
        print(f"ℹ️ No NHL games for {DATE} (ET)")
        return

    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # 1) Ensure real team rows exist (fixes placeholder T1/T2… rows)
        _ensure_teams_exist(cur, games)

        # 2) Ensure provider→internal mappings
        team_map = _ensure_team_mappings(cur, games)

        # 3) Stage (fresh each run)
        cur.execute("truncate nhl.import_games_stage")
        cur.execute("truncate nhl.import_game_external_ids_stage")

        rows = 0
        state_updates: list[tuple[int, str]] = [] 

        for g in games:
            gid_raw = g.get("id") or g.get("gamePk") or g.get("gameId")
            if gid_raw is None:
                print("⚠️ Skipping game with no id:", json.dumps(g)[:200])
                continue
            gid = int(gid_raw)

            season = g.get("season")
            if season is None:
                try:
                    season = int(str(gid)[:4])
                except Exception:
                    season = None

            game_type = g.get("gameType")
            start_iso = g.get("startTimeUTC") or g.get("gameDate")

            # provider team ids
            home_pid_str, _ = _extract_provider_team_id_and_teamobj(g, "home")
            away_pid_str, _ = _extract_provider_team_id_and_teamobj(g, "away")
            if not home_pid_str or not away_pid_str:
                print(f"⚠️ Missing team ids for game {gid}: home={home_pid_str}, away={away_pid_str}")
                continue

            home_team_id = team_map.get(home_pid_str) or int(home_pid_str)
            away_team_id = team_map.get(away_pid_str) or int(away_pid_str)

            # NEW: compute mapped status
            st = _map_game_state(g)
            if st:
                state_updates.append((gid, st))     # NEW

            # Stage rows (PLAIN INSERTS — no ON CONFLICT in stage tables)
            cur.execute("""
                insert into nhl.import_games_stage
                  (game_id, game_date, start_time_utc, season, game_type, home_team_id, away_team_id)
                values (%s, %s, %s, %s, %s, %s, %s)
            """, (gid, DATE, start_iso, season, game_type, home_team_id, away_team_id))

            cur.execute("""
                insert into nhl.import_game_external_ids_stage
                  (game_id, provider, provider_game_id)
                values (%s, 'nhl', %s)
            """, (gid, str(gid)))

            rows += 1

                    # NEW: apply status updates without changing schema
        for gid, st in state_updates:
            cur.execute("""
                update nhl.games
                set status = %s
                where game_id = %s
                  and (status is distinct from %s)
            """, (st, gid, st))


        # 4) Merge stage → base
        cur.execute("""
            insert into nhl.games (game_id, game_date, start_time_utc, season, game_type, home_team_id, away_team_id)
            select distinct game_id, game_date, start_time_utc, season, game_type, home_team_id, away_team_id
            from nhl.import_games_stage
            on conflict (game_id) do update
              set game_date      = excluded.game_date,
                  start_time_utc = excluded.start_time_utc,
                  season         = excluded.season,
                  game_type      = excluded.game_type,
                  home_team_id   = excluded.home_team_id,
                  away_team_id   = excluded.away_team_id
        """)

        cur.execute("""
            insert into nhl.game_external_ids (game_id, provider, provider_game_id)
            select distinct game_id, provider, provider_game_id
            from nhl.import_game_external_ids_stage
            on conflict (game_id, provider) do update
              set provider_game_id = excluded.provider_game_id
        """)

        conn.commit()
        print(f"✅ Staged & merged {rows} games for {DATE} (ET)")


if __name__ == "__main__":
    main()
