#!/usr/bin/env python3
# Ingest skater boxscore stats for SLATE_DATE into nhl.import_skater_logs_stage
# Robust mapping: prefer roster->players(full_name) for the same game; fall back to player_external_ids(provider='nhl').
# Uses new NHL endpoints:
#   - Schedule:  https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
#   - Boxscore:  https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore

import os, sys, datetime as dt
from zoneinfo import ZoneInfo
import re, unicodedata

# ---------------- No-prepares guard (prevents DuplicatePreparedStatement) ----------------
os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

import psycopg
from psycopg.rows import dict_row
import requests


# Force every cursor.execute(...) to use simple execution (no PREPARE)
_ORIG_EXECUTE = psycopg.Cursor.execute
def _no_prep_execute(self, query, params=None, **kw):
    kw["prepare"] = False
    return _ORIG_EXECUTE(self, query, params, **kw)
psycopg.Cursor.execute = _no_prep_execute

# optional: load .env locally
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# Reliable SSL / GSS settings for Supabase/PG
if "?sslmode=" not in DB_URL and "&sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
if "?gssencmode=" not in DB_URL and "&gssencmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "gssencmode=disable"

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
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

S = _session()

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

# ---------------- Data fetch (new endpoints) ----------------
def get_schedule(date_str: str):
    """
    https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
    Response may be {"games":[...]} OR {"gameWeek":[{"games":[...]}...]}.
    We also ensure the start time falls on date_str in ET.
    """
    url = f"{BASE_SCHEDULE}/{date_str}"
    r = S.get(url, timeout=15); r.raise_for_status()
    data = r.json()

    games_in = []
    if isinstance(data, dict) and "gameWeek" in data:
        for day in data.get("gameWeek", []):
            games_in.extend(day.get("games", []))
    else:
        games_in = list((data or {}).get("games") or [])

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
    url = f"{BASE_BOXSCORE}/{game_pk}/boxscore"
    r = S.get(url, timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

# ---------------- DB helpers ----------------

NAME_INITIAL_RE = re.compile(r"^([A-Za-z])[.\s-]*([A-Za-z][A-Za-z\-\s'’]+)$")

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm_name(s: str) -> str:
    return " ".join(_strip_accents((s or "").lower().replace("’", "'")).split())

def _extract_box_name(p: dict) -> str:
    """
    Try first/last from boxscore player object; fall back to name.full/default; else empty.
    """
    first = (p.get("firstName") or "").strip()
    last  = (p.get("lastName")  or "").strip()
    if first or last:
        return f"{first} {last}".strip()

    nm = p.get("name")
    if isinstance(nm, dict):
        return (nm.get("full") or nm.get("default") or "").strip()
    return (nm or "").strip()

def _expand_initial_last(nm_norm: str, roster_map_keys: list[str]) -> str | None:
    """
    If nm_norm is like 'a. killorn', find the single roster full name whose
    first initial matches and whose last word matches the last name.
    """
    m = NAME_INITIAL_RE.match(nm_norm)
    if not m:
        return None
    first_init = m.group(1).lower()
    last_norm  = _norm_name(m.group(2))
    cands = [k for k in roster_map_keys
             if k.endswith(" " + last_norm) and k[0] == first_init]
    return cands[0] if len(cands) == 1 else None


def roster_name_map(conn, game_id: int):
    """
    returns {norm_full_name -> (player_id, team_id)}
    """
    sql = """
      SELECT
        r.player_id,
        r.team_id,
        LOWER(REGEXP_REPLACE(p.full_name, '\s+', ' ', 'g')) AS nm
      FROM nhl.roster_status r
      JOIN nhl.players p ON p.player_id = r.player_id
      WHERE r.game_id = %s
    """
    mp = {}
    # force dict rows here so we read by column name
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (game_id,))
        for r in cur.fetchall():
            pid = int(r["player_id"])
            tid = int(r["team_id"])
            nm  = r["nm"]
            mp[nm] = (pid, tid)
    return mp

def external_map(conn, nhl_ids):
    """Map NHL numeric IDs -> internal player_id using nhl.player_external_ids(provider='nhl')."""
    try:
        ids = sorted({int(x) for x in nhl_ids if x is not None})
    except Exception:
        ids = [int(x) for x in nhl_ids if isinstance(x, (int, str)) and str(x).isdigit()]

    if not ids:
        return {}

    sql = """
      SELECT provider_player_id::bigint AS nhl_id, player_id
      FROM nhl.player_external_ids
      WHERE provider = 'nhl' AND provider_player_id ~ '^[0-9]+$'
        AND provider_player_id::bigint = ANY(%s)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (ids,))
        mp = {}
        for r in cur.fetchall():
            try:
                mp[int(r["nhl_id"])] = int(r["player_id"])
            except Exception:
                pass
        return mp
    
# --- add/replace these helpers near your other utilities ---

def _safe_str(v):
    return v.strip() if isinstance(v, str) and v.strip() else ""

def full_name_from_box_player(p: dict) -> str:
    """
    Prefer full names: firstName + lastName.
    Fall back to name.default (abbreviated) only if first/last are missing.
    """
    first = _safe_str(p.get("firstName"))
    last  = _safe_str(p.get("lastName"))
    if first or last:
        return (first + " " + last).strip()

    nm = p.get("name")
    if isinstance(nm, dict):
        # 'default' is like "a. debrincat" (won't match DB full_name, but better than None)
        return _safe_str(nm.get("full") or nm.get("default") or "")
    return _safe_str(nm)

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

def refresh_roster_status_from_box(conn, gpk: int):
    """
    Ensure nhl.roster_status has SKATERS (F/D) for this game, by team, derived from
    api-web.nhle.com gamecenter/{gpk}/boxscore. Uses player_external_ids for ID mapping.
    Safe guards: abort if mapping fails; inserts missing rows only (no deletes).
    """

    def _box(g):
        r = requests.get(f"{BASE_BOXSCORE}/{g}/boxscore", timeout=20)
        r.raise_for_status()
        return r.json()

    def _collect_ids_by_team(box):
        out = {}
        p = box.get("playerByGameStats") or {}
        side_to_tid = {
            "homeTeam": (box.get("homeTeam") or {}).get("id"),
            "awayTeam": (box.get("awayTeam") or {}).get("id"),
        }
        for side_key, tid in side_to_tid.items():
            if tid is None:
                continue
            s = set()
            team = p.get(side_key) or {}
            for bucket in ("forwards", "defense"):
                for x in (team.get(bucket) or []):
                    nhl_id = x.get("playerId") or x.get("id")
                    try:
                        s.add(int(nhl_id))
                    except Exception:
                        pass
            out[int(tid)] = s
        return out

    def _ext_map(c, ids):
        if not ids:
            return {}
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT provider_player_id::bigint AS nhl_id, player_id
                FROM nhl.player_external_ids
                WHERE provider='nhl'
                  AND provider_player_id ~ '^[0-9]+$'
                  AND provider_player_id::bigint = ANY(%s)
            """, (list(ids),))
            return {int(r["nhl_id"]): int(r["player_id"]) for r in cur.fetchall()}

    def _roster_cols(c):
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT lower(column_name) AS col
                FROM information_schema.columns
                WHERE table_schema='nhl' AND table_name='roster_status'
            """)
            return {r["col"] for r in cur.fetchall()}

    box = _box(gpk)
    by_team = _collect_ids_by_team(box)
    all_ids = set().union(*by_team.values()) if by_team else set()
    xmap = _ext_map(conn, all_ids)
    desired = {(t, xmap[i]) for t, ids in by_team.items() for i in ids if i in xmap}

    if not desired:
        print(f"[{gpk}] roster refresh ABORTED: mapped 0 of {len(all_ids)} NHL IDs")
        return

    # existing skater rows (F/D) for this game
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.team_id, r.player_id
            FROM nhl.roster_status r
            JOIN nhl.players p ON p.player_id = r.player_id
            WHERE r.game_id=%s AND p.position IN ('F','D')
        """, (gpk,))
        existing = {(int(t), int(p)) for t, p in cur.fetchall()}

    to_insert = desired - existing
    if not to_insert:
        print(f"[{gpk}] roster refresh: already in sync ({len(desired)} skaters).")
        return

    cols = _roster_cols(conn)
    insert_cols = ["game_id", "team_id", "player_id"]
    row_defaults = []
    if "active_flag" in cols:
        insert_cols.append("active_flag"); row_defaults.append(True)
    if "line_role" in cols:
        insert_cols.append("line_role");  row_defaults.append(None)
    if "pp_unit" in cols:
        insert_cols.append("pp_unit");    row_defaults.append(None)
    if "asof_ts" in cols:
        insert_cols.append("asof_ts");    row_defaults.append(dt.datetime.now(dt.timezone.utc))

    placeholders = "(" + ",".join(["%s"] * len(insert_cols)) + ")"
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO nhl.roster_status ({', '.join(insert_cols)}) VALUES {placeholders} ON CONFLICT DO NOTHING",
            [tuple([gpk, t, p] + row_defaults) for (t, p) in to_insert]
        )
    print(f"[{gpk}] roster refresh: inserted {len(to_insert)} / desired {len(desired)} skaters.")


# ---------------- Parse skaters from new boxscore ----------------
def _iter_skaters_from_box(box: dict):
    """
    Yields dicts with normalized name + stats from api-web.nhle.com gamecenter payload:
      {"nhl_id": int|None, "nm": <normalized name>, "sog": int|None, "attempts": int|None,
       "toi_min": float, "pp_min": float}
    """
    pstats = (box.get("playerByGameStats") or {})
    for side_key in ("homeTeam", "awayTeam"):
        team = pstats.get(side_key) or {}
        for k in ("forwards", "defense"):  # goalies excluded here
            arr = team.get(k) or []
            if not isinstance(arr, list):
                continue
            for p in arr:
                nhl_id = p.get("playerId") or p.get("id")
                try:
                    nhl_id = int(nhl_id) if nhl_id is not None else None
                except Exception:
                    nhl_id = None

                name_raw = _extract_box_name(p)
                nm = _norm_name(name_raw)

                stats = p.get("stats") or {}
                sog = stats.get("shotsOnGoal")
                if sog is None:
                    sog = stats.get("shots")

                attempts = (
                    stats.get("shotsAttempted")
                    if "shotsAttempted" in stats
                    else ((stats.get("shotsOnGoal") or stats.get("shots") or 0)
                          + (stats.get("missedShots") or 0)
                          + (stats.get("blockedShots") or 0))
                )

                toi_s    = stats.get("toi") or stats.get("timeOnIce")
                pp_toi_s = stats.get("powerPlayToi") or stats.get("powerPlayTimeOnIce")

                yield {
                    "nhl_id": nhl_id,
                    "nm": nm,
                    "sog": int(sog) if sog is not None else None,
                    "attempts": int(attempts) if attempts is not None else None,
                    "toi_min": toi_to_minutes(toi_s),
                    "pp_min":  toi_to_minutes(pp_toi_s),
                }

# ---------------- Main ----------------
def main():
    games = get_schedule(SLATE_DATE)
    if not games:
        print(f"No games on {SLATE_DATE}")
        return

    inserted_total = 0
    skipped_no_map_total = 0

    with psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row, prepare_threshold=0) as conn:
        try:
            conn.prepare_threshold = 0  # some drivers expose this
        except Exception:
            pass

        for gpk in games:
            skipped_no_map = 0  # reset per game
            try:
                # keep roster_status aligned to the actual skaters in this game
                refresh_roster_status_from_box(conn, gpk)

                box = get_boxscore(gpk)
                if not box:
                    print(f"[{gpk}] boxscore 404/empty; skipping")
                    conn.rollback()
                    continue

                roster_map = roster_name_map(conn, gpk)  # {norm_full_name -> (player_id, team_id)}
                roster_keys = list(roster_map.keys())

                skaters = list(_iter_skaters_from_box(box))
                nhl_ids = [s["nhl_id"] for s in skaters if s["nhl_id"] is not None]
                ext_map = external_map(conn, nhl_ids)

                rows = []
                for s in skaters:
                    pid = None
                    learned_from_roster = False

                    # a) exact roster full-name match
                    if s["nm"] in roster_map:
                        pid = roster_map[s["nm"]][0]
                        learned_from_roster = True
                    else:
                        # b) expand 'a. last' against roster names for this game
                        alt = _expand_initial_last(s["nm"], roster_keys)
                        if alt is not None:
                            pid = roster_map[alt][0]
                            learned_from_roster = True
                        # c) fallback: learned external id
                        elif s["nhl_id"] is not None:
                            pid = ext_map.get(int(s["nhl_id"]))

                    if pid is None:
                        skipped_no_map += 1
                        continue

                    # teach external id only when we matched via roster path
                    if learned_from_roster and s["nhl_id"] is not None:
                        upsert_external_id(conn, pid, s["nhl_id"])

                    rows.append((
                        int(pid), int(gpk), SLATE_DATE,
                        s["sog"], s["attempts"],
                        float(s["toi_min"]) if s["toi_min"] is not None else None,
                        float(s["pp_min"])  if s["pp_min"]  is not None else None,
                    ))

                inserted = upsert_rows(conn, rows)
                conn.commit()
                inserted_total += inserted
                skipped_no_map_total += skipped_no_map
                print(f"[{gpk}] upserted {inserted} skater rows; skipped_no_map={skipped_no_map}")

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"[{gpk}] ERROR: {e}", file=sys.stderr)

    print(f"Done. Upserted total {inserted_total} skater rows for {SLATE_DATE}; skipped_no_map={skipped_no_map_total}")

if __name__ == "__main__":
    main()
