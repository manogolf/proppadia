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
from pathlib import Path
import hashlib

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
HEALTH_ROOT = Path(os.getenv("NHL_SLATE_HEALTH_ROOT", "artifacts/operational/nhl/slates"))
LAST_FETCH_EVIDENCE: dict = {}


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
    global LAST_FETCH_EVIDENCE
    data = None
    selected_url = None
    last_err = None
    for url in candidates:
        try:
            r = s.get(url, timeout=12); r.raise_for_status()
            data = r.json()
            selected_url = url
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
    raw_bytes = (json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n").encode()
    LAST_FETCH_EVIDENCE = {
        "source_url": selected_url,
        "fetch_timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "raw_source": data,
        "raw_source_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "raw_game_count": len(games_in),
    }
    return out


def _write_slate_health(date_str: str, games: list[dict], completion_status: str,
                        downstream_ready: bool, error: str | None = None) -> Path:
    """Atomically publish date-bound source evidence and the completion gate."""
    dest = HEALTH_ROOT / date_str
    dest.mkdir(parents=True, exist_ok=True)
    raw = LAST_FETCH_EVIDENCE.get("raw_source")
    if raw is not None:
        raw_path = dest / "raw_schedule_response.json"
        tmp = raw_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n")
        tmp.replace(raw_path)
    ids = [g.get("id") or g.get("gamePk") or g.get("gameId") for g in games]
    types = [g.get("gameType") for g in games]
    states: dict[str, int] = {}
    for g in games:
        state = str(g.get("gameState") or g.get("state") or "UNKNOWN")
        states[state] = states.get(state, 0) + 1
    normalized = [{
        "game_id": gid,
        "start_time_utc": g.get("startTimeUTC") or g.get("gameDate"),
        "game_type": g.get("gameType"),
        "game_state": g.get("gameState") or g.get("state"),
    } for gid, g in zip(ids, games)]
    canonical_hash = hashlib.sha256(
        (json.dumps(normalized, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    health = {
        "canonical_season": int(date_str[:4]) if int(date_str[5:7]) >= 7 else int(date_str[:4]) - 1,
        "slate_date": date_str,
        "fetch_timestamp_utc": LAST_FETCH_EVIDENCE.get("fetch_timestamp_utc"),
        "source": LAST_FETCH_EVIDENCE.get("source_url"),
        "raw_game_count": LAST_FETCH_EVIDENCE.get("raw_game_count", len(games)),
        "normalized_game_count": len(games),
        "duplicate_count": len(ids) - len(set(ids)),
        "identity_error_count": sum(x is None for x in ids),
        "game_type_error_count": sum(x not in {1, 2, 3} for x in types),
        "schedule_status_breakdown": states,
        "raw_source_hash": LAST_FETCH_EVIDENCE.get("raw_source_sha256"),
        "canonical_output_hash": canonical_hash,
        "completion_status": completion_status,
        "valid_empty_slate": completion_status == "VALID_EMPTY_SLATE",
        "downstream_ready": downstream_ready,
        "error": error,
    }
    health_path = dest / "slate_health.json"
    tmp = health_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(health, indent=2, sort_keys=True) + "\n")
    tmp.replace(health_path)
    return health_path

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
    This value is stored in nhl.teams.full_team_name.
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


def _ensure_teams_exist(cur, games):
    """
    Ensure all teams from today's schedule exist in nhl.teams.

    Idempotent: if a team (by abbreviation) already exists, we update its
    team_id/name; otherwise we insert it.
    """
    # Collect unique teams from schedule payload (supports api-web + legacy)
    # Idempotent: if a team (by abbreviation) already exists, we update its
    # team_id/full_team_name; otherwise we insert it.
    teams: dict[str, tuple[int, str | None]] = {}  # key: abbr, value: (team_id, full_team_name)

    def _name_text(v) -> str | None:
        # api-web sometimes returns multi-language objects: {"default": "Vegas"}
        if v is None:
            return None
        if isinstance(v, str):
            return v.strip() or None
        if isinstance(v, dict):
            for k in ("default", "en", "English", "name"):
                s = v.get(k)
                if isinstance(s, str) and s.strip():
                    return s.strip()
        return None

    for g in games:
        for role in ("home", "away"):
            pid_str, team_obj = _extract_provider_team_id_and_teamobj(g, role)
            if not pid_str:
                continue

            abbr = _api_team_abbr(team_obj)
            if not abbr and isinstance(team_obj, dict):
                # legacy-ish fallback
                abbr = team_obj.get("abbreviation")
            if not abbr:
                continue

            try:
                tid = int(pid_str)
            except Exception:
                continue

            name = None
            if isinstance(team_obj, dict):
                name = _name_text(team_obj.get("name"))
            name = name or _api_team_full_name(team_obj)

            teams[str(abbr)] = (tid, name)

    if not teams:
        print("[import_schedule_today] WARNING: no teams found in schedule payload")
        return

    params = [(abbr, tid, name) for abbr, (tid, name) in teams.items()]

    # Upsert teams by abbreviation (team)
    cur.executemany(
        """
        INSERT INTO nhl.teams (team, team_id, full_team_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (team) DO UPDATE
        SET team_id        = EXCLUDED.team_id,
            full_team_name = COALESCE(EXCLUDED.full_team_name, nhl.teams.full_team_name);
        """,
        params,
    )

    print(f"[import_schedule_today] ensured {len(params)} team rows in nhl.teams")

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
    try:
        games = fetch_schedule_for_date(DATE)
    except Exception as exc:
        _write_slate_health(DATE, [], "FAILED", False, str(exc))
        raise
    if not games:
        health_path = _write_slate_health(DATE, games, "VALID_EMPTY_SLATE", True)
        print(f"ℹ️ No NHL games for {DATE} (ET)")
        print(f"✅ Slate health: {health_path}")
        return

    # Completeness is a publish gate: do not authorize a slate containing a
    # silently skipped or unsupported identity. Unknown game types fail closed.
    for g in games:
        gid = g.get("id") or g.get("gamePk") or g.get("gameId")
        start = g.get("startTimeUTC") or g.get("gameDate")
        home_pid, _ = _extract_provider_team_id_and_teamobj(g, "home")
        away_pid, _ = _extract_provider_team_id_and_teamobj(g, "away")
        if gid is None or not start or not home_pid or not away_pid or g.get("gameType") not in {1, 2, 3}:
            error = f"incomplete/unsupported schedule identity for game_id={gid}"
            _write_slate_health(DATE, games, "PARTIAL", False, error)
            raise RuntimeError(error)

    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # Optional: stop psycopg from auto-creating server-side prepared stmts
        try:
            conn.prepare_threshold = None  # psycopg3
        except Exception:
            pass

        # Safety: drop any leftover prepared statements in this session
        try:
            cur.execute("DEALLOCATE ALL;")
        except Exception:
            pass
        # 1) Ensure real team rows exist (fixes placeholder T1/T2… rows)
        _ensure_teams_exist(cur, games)

        # 2) Ensure provider→internal mappings
        team_map = _ensure_team_mappings(cur, games)

        # 3) JSON bridge (no stage tables)
        payload = []
        rows = 0

        for g in games:
            gid_raw = g.get("id") or g.get("gamePk") or g.get("gameId")
            if gid_raw is None:
                print("⚠️ Skipping game with no id:", json.dumps(g)[:200])
                continue
            gid = int(gid_raw)

            # Project rule: season is 4-digit season start year (int).
            # Never trust API-provided season fields (some sources use 8-digit like 20252026).
            season = None
            try:
                season = int(str(gid)[:4])
            except Exception:
                season = None

            if season is None:
                # Fallback: derive from start/game date if id parsing fails
                gd = g.get("startTimeUTC") or g.get("gameDate")
                if isinstance(gd, str) and len(gd) >= 10:
                    y, m, _ = map(int, gd[:10].split("-"))
                    season = y if m >= 7 else y - 1

            if season is None:
                raise RuntimeError(f"Could not derive 4-digit season for game_id={gid}")

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

            # mapped status (optional)
            st = _map_game_state(g)

            payload.append({
                "game_id": gid,
                "game_date": str(DATE),              # ET date key you’re using
                "start_time_utc": start_iso,         # ISO string
                "season": season,
                "game_type": game_type,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "status": st
            })
            rows += 1

        # 4) Upsert directly into base using jsonb_to_recordset + join teams (no stage tables)
        cur.execute(
            """
            WITH src AS (
              SELECT *
              FROM jsonb_to_recordset(%s::jsonb) AS s(
                game_id       bigint,
                game_date     date,
                start_time_utc text,
                season        int,
                game_type     int,
                home_team_id  int,
                away_team_id  int,
                status        text
              )
            )
            INSERT INTO nhl.games (
              game_id,
              game_date,
              start_time_utc,
              season,
              game_type,
              home_team_code,
              away_team_code,
              home_team_id,
              away_team_id,
              status
            )
            SELECT
              s.game_id,
              s.game_date,
              NULLIF(s.start_time_utc, '')::timestamptz,
              s.season,
              s.game_type,
              th.team AS home_team_code,
              ta.team AS away_team_code,
              s.home_team_id,
              s.away_team_id,
              s.status
            FROM src s
            JOIN nhl.teams th ON th.team_id = s.home_team_id
            JOIN nhl.teams ta ON ta.team_id = s.away_team_id
            WHERE th.team IS NOT NULL
              AND ta.team IS NOT NULL
            ON CONFLICT (game_id) DO UPDATE
              SET game_date      = EXCLUDED.game_date,
                  start_time_utc = EXCLUDED.start_time_utc,
                  season         = EXCLUDED.season,
                  game_type      = EXCLUDED.game_type,
                  home_team_code = EXCLUDED.home_team_code,
                  away_team_code = EXCLUDED.away_team_code,
                  home_team_id   = EXCLUDED.home_team_id,
                  away_team_id   = EXCLUDED.away_team_id,
                  status         = COALESCE(EXCLUDED.status, nhl.games.status)
            """,
            (json.dumps(payload),)
        )

        if cur.rowcount != len(payload):
            error = f"canonical write count mismatch: expected={len(payload)} stored={cur.rowcount}"
            _write_slate_health(DATE, games, "PARTIAL", False, error)
            raise RuntimeError(error)

        conn.commit()
        health_path = _write_slate_health(DATE, games, "READY", True)
        print(f"✅ Upserted {rows} games for {DATE} (ET) (no stage tables)")
        print(f"✅ Slate health: {health_path}")


if __name__ == "__main__":
    main()
