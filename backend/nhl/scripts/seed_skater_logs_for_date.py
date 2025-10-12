#!/usr/bin/env python3
# Ingest skater boxscore stats for SLATE_DATE into nhl.import_skater_logs_stage
# Robust mapping: prefer roster->players(full_name) for the same game; fall back to player_external_ids(provider='nhl').
# Uses new NHL endpoints:
#   - Schedule:  https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
#   - Boxscore:  https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore

import os, sys, datetime as dt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg
from zoneinfo import ZoneInfo

# ---------------- Env / args ----------------
DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
if not DB_URL:
    print("Set SUPABASE_DB_URL or DATABASE_URL", file=sys.stderr); sys.exit(2)

SLATE_DATE = os.environ.get("SLATE_DATE")
if not SLATE_DATE:
    print("Set SLATE_DATE=YYYY-MM-DD", file=sys.stderr); sys.exit(2)
try:
    target_date = dt.date.fromisoformat(SLATE_DATE)
except ValueError:
    print(f"Bad SLATE_DATE: {SLATE_DATE}", file=sys.stderr); sys.exit(2)

ET = ZoneInfo("America/New_York")

BASE_SCHEDULE = "https://api-web.nhle.com/v1/schedule"
BASE_BOXSCORE = "https://api-web.nhle.com/v1/gamecenter"

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

def _et_date_from_utc(iso_utc: str | None) -> str | None:
    if not iso_utc:
        return None
    try:
        # api returns "2025-10-11T17:30:00Z"
        return dt.datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(ET).date().isoformat()
    except Exception:
        return None

# ---------------- Utilities ----------------
def toi_to_minutes(s):
    """Accepts 'MM:SS', 'H:MM:SS', or numeric seconds; returns minutes (float)."""
    if s is None or s == "":
        return 0.0
    if isinstance(s, (int, float)):
        return float(s) / 60.0
    parts = [p for p in str(s).split(":")]
    try:
        if len(parts) == 3:
            h, m, sec = map(int, parts)
            return 60*h + m + sec/60.0
        m, sec = map(int, parts)
        return m + sec/60.0
    except Exception:
        return 0.0

def normalize_name(s):
    """
    Normalize a player's name into a lowercased, single-spaced string.
    Be defensive: the caller might pass a dict instead of a string.
    """
    # If an object/dict sneaks in, try common name fields
    if isinstance(s, dict):
        s = (
            s.get("fullName")
            or s.get("displayName")
            or s.get("firstLastName")
            or ((s.get("firstName") or "") + " " + (s.get("lastName") or ""))
        )
    if s is None:
        s = ""
    # Collapse whitespace and lowercase
    return " ".join(str(s).split()).lower()

# ---------------- Data fetch (new endpoints) ----------------
def get_schedule(date_str: str):
    """
    https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
    Response may be {"games":[...]} OR {"gameWeek":[{"games":[...]}...]}.
    We also ensure the start time falls on date_str in ET.
    """
    s = _session()
    url = f"{BASE_SCHEDULE}/{date_str}"
    r = s.get(url, timeout=15); r.raise_for_status()
    data = r.json()

    games_in = []
    if isinstance(data, dict) and "gameWeek" in data:
        for day in data.get("gameWeek", []):
            games_in.extend(day.get("games", []))
    else:
        games_in = list(data.get("games") or [])

    out = []
    seen = set()
    for g in games_in:
        gid = g.get("id") or g.get("gamePk") or g.get("gameId")
        if not gid:
            continue
        # start time normalization
        start_iso = g.get("startTimeUTC") or g.get("gameDate")
        if _et_date_from_utc(start_iso) != date_str:
            continue
        if gid in seen:
            continue
        seen.add(gid)
        out.append(int(gid))
    return out

def get_boxscore(game_pk: int):
    """
    https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore
    Returns gamecenter JSON. Skater stats live under playerByGameStats.{homeTeam,awayTeam}.
    """
    s = _session()
    url = f"{BASE_BOXSCORE}/{game_pk}/boxscore"
    r = s.get(url, timeout=15); r.raise_for_status()
    return r.json()

# ---------------- DB helpers ----------------
def roster_name_map(conn, game_id: int):
    # returns {norm_full_name -> (player_id, team_id)}
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
            mp[nm] = (pid, tid)
    return mp

def external_map(conn, nhl_ids):
    """Map NHL numeric IDs -> internal player_id using nhl.player_external_ids(provider, provider_player_id)."""
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
                # ignore non-numeric provider ids
                pass
        return mp
    
def upsert_external_id(conn, player_id: int, nhl_id: int):
    """
    Learn NHL external id whenever we successfully identify a player via roster mapping.
    Requires UNIQUE (player_id, provider) constraint on nhl.player_external_ids.
    """
    if nhl_id is None:
        return
    sql = """
      INSERT INTO nhl.player_external_ids (player_id, provider, provider_player_id)
      VALUES (%s, 'nhl', %s)
      ON CONFLICT (player_id, provider) DO UPDATE
        SET provider_player_id = EXCLUDED.provider_player_id
        WHERE nhl.player_external_ids.provider_player_id IS DISTINCT FROM EXCLUDED.provider_player_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (int(player_id), str(int(nhl_id))))

def upsert_rows(conn, rows):
    if not rows:
        return 0
    sql = """
    INSERT INTO nhl.import_skater_logs_stage
      (player_id, game_id, game_date, shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (player_id, game_id) DO UPDATE SET
      shots_on_goal   = EXCLUDED.shots_on_goal,
      shot_attempts   = COALESCE(EXCLUDED.shot_attempts, nhl.import_skater_logs_stage.shot_attempts),
      toi_minutes     = EXCLUDED.toi_minutes,
      pp_toi_minutes  = EXCLUDED.pp_toi_minutes
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)

# ---------------- Parse skaters from new boxscore ----------------
def _iter_skaters_from_box(box: dict, game_pk: int, game_date_iso: str):
    """
    Yields tuples (nhl_id, full_name, sog, attempts, toi_min, pp_min)
    reading from api-web.nhle.com gamecenter payload.
    """
    pstats = (box.get("playerByGameStats") or {})
    sides = (
        ("homeTeam", True),
        ("awayTeam", False),
    )
    for side_key, _is_home in sides:
        team = pstats.get(side_key) or {}
        # Buckets we care about for skaters
        buckets = []
        for k in ("forwards", "defense"):  # goalies excluded here
            arr = team.get(k) or []
            if isinstance(arr, list):
                buckets.extend(arr)
        for p in buckets:
            # IDs & names
            nhl_id = p.get("playerId") or p.get("id")
            name = (
                p.get("name")
                or (f"{p.get('firstName','')} {p.get('lastName','')}".strip())
                or p.get("lastFirstName")
            )

            stats = p.get("stats") or {}
            # SOG
            sog = stats.get("shotsOnGoal")
            if sog is None:
                sog = stats.get("shots")

            # Attempts (best-effort): explicit or compose shots+missed+blocked
            attempts = (
                stats.get("shotsAttempted")
                if "shotsAttempted" in stats
                else (
                    (stats.get("shotsOnGoal") or stats.get("shots") or 0)
                    + (stats.get("missedShots") or 0)
                    + (stats.get("blockedShots") or 0)
                )
            )
            # TOI
            toi_s = stats.get("toi") or stats.get("timeOnIce")
            pp_toi_s = stats.get("powerPlayToi") or stats.get("powerPlayTimeOnIce")

            yield (
                nhl_id if nhl_id is not None else None,
                name or "",
                sog if sog is not None else None,
                attempts if attempts is not None else None,
                toi_to_minutes(toi_s),
                toi_to_minutes(pp_toi_s),
            )

# ---------------- Main ----------------
def main():
    games = get_schedule(SLATE_DATE)
    if not games:
        print(f"No games on {SLATE_DATE}")
        return

    inserted_total = 0
    skipped_no_map = 0
    unmapped_rows = []  # accumulate for optional audit table

    with psycopg.connect(DB_URL, autocommit=False) as conn:
        for gpk in games:
            try:
                box = get_boxscore(gpk)
            except Exception as e:
                print(f"[{gpk}] boxscore fetch failed: {e}", file=sys.stderr)
                continue

            roster_map = roster_name_map(conn, gpk)  # name -> (player_id, team_id)

            # Collect rows
            skaters = list(_iter_skaters_from_box(box, gpk, SLATE_DATE))
            nhl_ids = [int(s[0]) for s in skaters if s[0] is not None]
            ext_map = external_map(conn, nhl_ids)

            rows = []
            for nhl_id, fullName, sog, attempts, toi_min, pp_min in skaters:
                pid = None

            # preferred: roster name match for THIS game
                nm = normalize_name(fullName)
                if nm in roster_map:
                    pid = roster_map[nm][0]
                    # learn NHL external id for future lookups
                    if nhl_id is not None:
                        upsert_external_id(conn, pid, nhl_id)
                # fallback: external id mapping
                if pid is None and nhl_id is not None:
                    pid = ext_map.get(int(nhl_id))

                if pid is None:
                    skipped_no_map += 1
                    # keep a record so we can fix mappings later (audit table is optional)
                    unmapped_rows.append((int(gpk), int(nhl_id) if nhl_id is not None else None, fullName, None))
                    continue

                rows.append((
                    int(pid), int(gpk), SLATE_DATE,
                    int(sog) if sog is not None else None,
                    int(attempts) if attempts is not None else None,
                    float(toi_min) if toi_min is not None else None,
                    float(pp_min) if pp_min is not None else None
                ))

            inserted = upsert_rows(conn, rows)
            # persist any unmapped rows for this game (if the audit table exists)
            if unmapped_rows:
                try:
                    with conn.cursor() as cur:
                        cur.executemany(
                            "INSERT INTO nhl.import_skater_logs_unmapped (game_id, nhl_id, full_name, team_side) VALUES (%s,%s,%s,%s)",
                            unmapped_rows
                        )
                except Exception:
                    # table might not exist; keep pipeline resilient
                    pass
                finally:
                    unmapped_rows.clear()
            conn.commit()
            inserted_total += inserted
            print(f"[{gpk}] upserted {inserted} skater rows; skipped_no_map_so_far={skipped_no_map}")

    print(f"Done. Upserted total {inserted_total} skater rows for {SLATE_DATE}; skipped_no_map={skipped_no_map}")

if __name__ == "__main__":
    main()
