#!/usr/bin/env python3
# Ingest skater boxscore stats for SLATE_DATE into nhl.import_skater_logs_stage
# Robust mapping: prefer roster->players(full_name) for the same game; fall back to player_external_ids(provider='nhl').
# Uses new NHL endpoints:
#   - Schedule:  https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
#   - Boxscore:  https://api-web.nhle.com/v1/gamecenter/{gamePk}/boxscore

#  python backend/nhl/scripts/seed_skater_logs_for_date.py

import os, sys, datetime as dt
from zoneinfo import ZoneInfo
import re, unicodedata
from typing import Optional, Any

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

# Force executemany(...) to avoid PREPARE by delegating to execute() per item
_ORIG_EXECUTEMANY = psycopg.Cursor.executemany
def _no_prep_executemany(self, query, params_seq=None, **kw):
    for params in (params_seq or []):
        _no_prep_execute(self, query, params, **kw)
    return None
psycopg.Cursor.executemany = _no_prep_executemany

# optional: load .env locally
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.nhl.official_request_journal import ENV_REQUIRED, RequestContext, official_get
from backend.nhl.player_external_identity import (
    ABBREVIATED_PLAYER_NAME_RE,
    is_abbreviated_player_name,
    localized_text,
    resolve_player_external_identity,
)

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
def toi_to_minutes(x: Any) -> Optional[float]:
    """
    Convert TOI formats to minutes.
    - None / "" -> None (IMPORTANT: do NOT coerce missing to 0.0)
    - "MM:SS" or "M:SS" -> minutes as float
    - numeric seconds -> minutes as float
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        # assume seconds
        return float(x) / 60.0
    s = str(x).strip()
    if not s:
        return None
    # common formats: "0:00", "03:21", "12:34"
    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            mm, ss = parts
            try:
                return float(int(mm)) + float(int(ss)) / 60.0
            except Exception:
                return None
        # if weird, bail safely
        return None
    # last resort numeric string (seconds)
    try:
        return float(s) / 60.0
    except Exception:
        return None

def extract_pp_toi_minutes(p: dict) -> Optional[float]:
    """
    Extract PP TOI from whatever we have.
    If the key doesn't exist, return None (not 0.0).
    """
    # Most likely keys you intended:
    v = (
        p.get("ppToi")
        or p.get("powerPlayToi")
        or p.get("powerPlayTimeOnIce")
        or p.get("powerPlayTime")
    )

    # Sometimes it's nested under stats/timeOnIce blobs
    if v is None:
        for k in ("timeOnIce", "toi", "stats", "playerStats"):
            blob = p.get(k)
            if isinstance(blob, dict):
                v = (
                    blob.get("ppToi")
                    or blob.get("powerPlayToi")
                    or blob.get("powerPlayTimeOnIce")
                    or blob.get("powerPlayTime")
                )
                if v is not None:
                    break

    return toi_to_minutes(v)
# ---------------- Data fetch (new endpoints) ----------------
def get_schedule(date_str: str):
    """
    https://api-web.nhle.com/v1/schedule/YYYY-MM-DD
    Response may be {"games":[...]} OR {"gameWeek":[{"games":[...]}...]}.
    We also ensure the start time falls on date_str in ET.
    """
    url = f"{BASE_SCHEDULE}/{date_str}"
    context = RequestContext.from_env()
    r = official_get(
        url, timeout=15, session=S, stage="SKATER_COLLECTION",
        endpoint_family="SCHEDULE", identity={"slate_date": date_str},
        max_attempts=6, retry_statuses={429, 500, 502, 503, 504},
        backoff_seconds=0.5, reuse_preserved=context is not None,
    ); r.raise_for_status()
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
    context = RequestContext.from_env()
    r = official_get(
        url, timeout=20, session=S, stage="SKATER_COLLECTION",
        endpoint_family="BOXSCORE",
        identity={"slate_date": SLATE_DATE, "game_id": int(game_pk)},
        max_attempts=6, retry_statuses={429, 500, 502, 503, 504},
        backoff_seconds=0.5, reuse_preserved=context is not None,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

# ---------------- DB helpers ----------------

NAME_INITIAL_RE = ABBREVIATED_PLAYER_NAME_RE

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm_name(s: str) -> str:
    return " ".join(_strip_accents((s or "").lower().replace("’", "'")).split())

def _extract_box_name(p: dict) -> str:
    """
    Try first/last from boxscore player object; fall back to name.full/default; else empty.
    """
    first = localized_text(p.get("firstName")) or ""
    last  = localized_text(p.get("lastName")) or ""
    if first or last:
        return f"{first} {last}".strip()

    nm = p.get("name")
    if isinstance(nm, dict):
        return localized_text(nm.get("full") or nm) or ""
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


def resolve_skater_player_id(*, nhl_id: int | None, normalized_name: str,
                             external_ids: dict[int, int],
                             roster_names: dict[str, tuple[int, int]]) -> tuple[int | None, bool]:
    """Resolve only exact provider identities; names may confirm, never merge."""
    if nhl_id is None:
        return None, False
    provider_id = int(nhl_id)
    if provider_id in external_ids:
        return int(external_ids[provider_id]), False
    candidate = None
    if normalized_name in roster_names:
        candidate = roster_names[normalized_name][0]
    else:
        expanded = _expand_initial_last(normalized_name, list(roster_names))
        if expanded is not None:
            candidate = roster_names[expanded][0]
    if candidate is not None and int(candidate) == provider_id:
        return provider_id, True
    return None, False

def ensure_player_exists(conn, nhl_id: int, full_name: str | None, team_id: int | None):
    """
    Ensure nhl.players has a row for this NHL player_id.
    Rules:
      - Never insert from empty/placeholder names.
      - If name is missing, try to resolve from NHL API by player id.
    """
    import json

    def _fetch_player_full_name_by_id(pid: int) -> str | None:
        # NHL "player landing" endpoint (authoritative for name)
        # If this ever changes, the curl test below will tell you immediately.
        url = f"https://api-web.nhle.com/v1/player/{pid}/landing"
        try:
            response = official_get(
                url, timeout=10, session=S, stage="SKATER_COLLECTION",
                endpoint_family="PLAYER_LANDING",
                identity={"slate_date": SLATE_DATE, "player_id": int(pid)},
                max_attempts=6, retry_statuses={429, 500, 502, 503, 504},
                backoff_seconds=0.5, preserve_response=True,
            )
            data = response.json()
            returned_id = data.get("playerId") or data.get("id")
            if returned_id is not None and int(returned_id) != int(pid):
                raise RuntimeError("PLAYER_LANDING_IDENTITY_MISMATCH")
            first = localized_text(data.get("firstName")) or ""
            last = localized_text(data.get("lastName")) or ""
            nm = f"{first} {last}".strip()
            return nm or None
        except Exception:
            if os.environ.get(ENV_REQUIRED) == "1":
                raise
            return None

    raw_name = (full_name or "").strip()

    with conn.cursor(row_factory=dict_row) as cur:
        # 1) Already exists?
        cur.execute(
            """
            SELECT player_id, full_name, position, team_id
            FROM nhl.players
            WHERE player_id = %s
            """,
            (nhl_id,),
        )
        row = cur.fetchone()

        if row:
            existing_team_id = row.get("team_id")
            if team_id is not None and existing_team_id is None:
                cur.execute(
                    """
                    UPDATE nhl.players
                    SET team_id = %s
                    WHERE player_id = %s
                    """,
                    (team_id, nhl_id),
                )
        else:
            # Missing or abbreviated names require exact-ID landing evidence.
            if ((not raw_name) or raw_name.startswith("Player ")
                    or is_abbreviated_player_name(raw_name)):
                resolved = _fetch_player_full_name_by_id(nhl_id)
                if resolved:
                    raw_name = resolved.strip()
            if (not raw_name or raw_name.startswith("Player ")
                    or is_abbreviated_player_name(raw_name)):
                raise ValueError(
                    f"Refusing to insert unresolved name for nhl_id={nhl_id}: {raw_name!r}"
                )
            cur.execute(
                """
                INSERT INTO nhl.players (player_id, full_name, team_id, position)
                VALUES (%s, %s, %s, 'F')
                """,
                (nhl_id, raw_name, team_id),
            )
    resolve_player_external_identity(
        conn, player_id=nhl_id, provider="nhl", provider_player_id=nhl_id)

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
    return localized_text(v) or ""

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
    Learn NHL external id when we matched via roster mapping.
    Conservative behavior: if the (provider, provider_player_id) already exists for a *different*
    player, we do not override it — we just log once.
    """
    if nhl_id is None:
        return
    return resolve_player_external_identity(
        conn, player_id=int(player_id), provider="nhl",
        provider_player_id=str(int(nhl_id)))


def _stage_cols(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'nhl'
              AND table_name = 'import_skater_logs_stage'
            """
        )
        rows = cur.fetchall() or []
        cols: set[str] = set()
        for r in rows:
            if isinstance(r, dict):
                cols.add(str(r["column_name"]))
            else:
                cols.add(str(r[0]))
        return cols

def upsert_rows(conn, rows):
    """
    rows items must be:
      (player_id, game_id, game_date, team_id, shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes, blocks)
    """
    if not rows:
        return 0

    cols = _stage_cols(conn)
    has_blocks = "blocks" in cols
    insert_cols = [
        "player_id",
        "game_id",
        "game_date",
        "team_id",
        "shots_on_goal",
        "shot_attempts",
        "toi_minutes",
        "pp_toi_minutes",
    ]
    if has_blocks:
        insert_cols.append("blocks")

    placeholders = ",".join(["%s"] * len(insert_cols))
    sql = f"""
    INSERT INTO nhl.import_skater_logs_stage
      ({", ".join(insert_cols)})
    VALUES ({placeholders})
    ON CONFLICT (player_id, game_id) DO UPDATE SET
      -- keep date/team_id fresh (this was the bug causing old dates + NULL team_id to persist)
      game_date       = EXCLUDED.game_date,
      team_id         = COALESCE(EXCLUDED.team_id, nhl.import_skater_logs_stage.team_id),

      shots_on_goal = CASE
        WHEN EXCLUDED.toi_minutes > 0 THEN COALESCE(EXCLUDED.shots_on_goal, 0)
        ELSE EXCLUDED.shots_on_goal
        END,

      shot_attempts   = COALESCE(EXCLUDED.shot_attempts, nhl.import_skater_logs_stage.shot_attempts),
      toi_minutes     = EXCLUDED.toi_minutes,

      -- IMPORTANT: do not clobber with NULL/0; only take incoming when >0
      pp_toi_minutes  = COALESCE(
                          NULLIF(EXCLUDED.pp_toi_minutes, 0),
                          nhl.import_skater_logs_stage.pp_toi_minutes
                        )
      {"," if has_blocks else ""}{f" blocks = COALESCE(EXCLUDED.blocks, nhl.import_skater_logs_stage.blocks)" if has_blocks else ""};
    """
    use_rows = [tuple(r[: len(insert_cols)]) for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, use_rows)
    return len(rows)

def refresh_roster_status_from_box(conn, gpk: int):
    """
    Ensure nhl.roster_status has SKATERS (F/D) for this game, by team, derived from
    api-web.nhle.com gamecenter/{gpk}/boxscore. Uses player_external_ids for ID mapping.
    Safe guards: abort if mapping fails; inserts missing rows only (no deletes).
    """

    def _box(g):
        context = RequestContext.from_env()
        r = official_get(
            f"{BASE_BOXSCORE}/{g}/boxscore", timeout=20, session=S,
            stage="SKATER_ROSTER_ALIGNMENT", endpoint_family="BOXSCORE",
            identity={"slate_date": SLATE_DATE, "game_id": int(g)},
            max_attempts=6, retry_statuses={429, 500, 502, 503, 504},
            backoff_seconds=0.5, reuse_preserved=context is not None,
        )
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
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT r.team_id, r.player_id
            FROM nhl.roster_status r
            JOIN nhl.players p ON p.player_id = r.player_id
            WHERE r.game_id=%s AND p.position IN ('F','D')
        """, (gpk,))
        existing = {(int(r["team_id"]), int(r["player_id"])) for r in cur.fetchall()}

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
# =========================
# 1) REPLACE _iter_skaters_from_box WITH THIS
# =========================
def _iter_skaters_from_box(box: dict):
    """
    Parse skaters from api-web.nhle.com gamecenter/{gamePk}/boxscore.
    Yields dicts including team_id + full_name so downstream mapping/ensure_player_exists can work.
    """
    pstats = (box.get("playerByGameStats") or {})

    home_tid = (box.get("homeTeam") or {}).get("id")
    away_tid = (box.get("awayTeam") or {}).get("id")

    side_to_tid = {
        "homeTeam": int(home_tid) if home_tid is not None else None,
        "awayTeam": int(away_tid) if away_tid is not None else None,
    }

    for side_key in ("homeTeam", "awayTeam"):
        team_id = side_to_tid.get(side_key)
        team = pstats.get(side_key) or {}

        for k in ("forwards", "defense"):  # goalies excluded
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

                toi_s = p.get("toi")  # "MM:SS"
                sog = p.get("sog")

                # NHL boxscore `blockedShots` is defensive blocks by this skater, not the
                # offensive player's own attempts that were blocked. Only use explicit
                # shot-attempt fields for attempts; otherwise leave attempts unknown here.
                attempts = p.get("shotAttempts")
                if attempts is None and p.get("missedShots") is not None:
                    try:
                        attempts = int(p.get("sog") or 0) + int(p.get("missedShots") or 0)
                    except Exception:
                        attempts = None
                blocks = p.get("blockedShots")

                toi_min = toi_to_minutes(toi_s)

                # PP TOI: your canonical extractor first, then fallback guesses
                pp_min = extract_pp_toi_minutes(p)
                if pp_min is None:
                    pp_toi_s = (
                        p.get("ppToi")
                        or p.get("powerPlayToi")
                        or p.get("powerPlayTimeOnIce")
                        or p.get("ppTimeOnIce")
                        or p.get("pp_time_on_ice")
                        or p.get("pp_toi")
                    )
                    pp_min = toi_to_minutes(pp_toi_s)

                yield {
                    "nhl_id": nhl_id,
                    "nm": nm,
                    "full_name": name_raw.strip() if isinstance(name_raw, str) else None,
                    "team_id": team_id,
                    "sog": int(sog) if sog is not None else None,
                    "attempts": int(attempts) if attempts is not None else None,
                    "blocks": int(blocks) if blocks is not None else None,
                    "toi_min": toi_min,
                    "pp_min": pp_min,
                }                
# ---------------- Main ----------------
def main():
    games = get_schedule(SLATE_DATE)
    if not games:
        print(f"No games on {SLATE_DATE}")
        return

    inserted_total = 0
    skipped_no_map_total = 0

    with psycopg.connect(DB_URL, autocommit=False, row_factory=dict_row, prepare_threshold=None) as conn:
        try:
            conn.prepare_threshold = None  # some drivers expose this
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

                skaters = list(_iter_skaters_from_box(box))
                nhl_ids = [s["nhl_id"] for s in skaters if s["nhl_id"] is not None]
                ext_map = external_map(conn, nhl_ids)

                rows = []
                for s in skaters:
                    pid = None
                    learned_from_roster = False

                    # Always initialize these so we never reference an unassigned local.
                    nhl_id_val = None
                    team_id_val = None
                    full_name_val = None

                    # Exact provider identity always wins. Name matching may
                    # confirm the same numeric identity, never merge two IDs.
                    pid, learned_from_roster = resolve_skater_player_id(
                        nhl_id=s["nhl_id"], normalized_name=s["nm"],
                        external_ids=ext_map, roster_names=roster_map)

                    if pid is None:
                        if os.environ.get(ENV_REQUIRED) == "1":
                            raise RuntimeError(
                                f"UNPREPARED_SKATER_EXTERNAL_ID:{gpk}:{s.get('nhl_id')}")
                        # Auto-heal nhl.players so future runs can map this skater.
                        # ✅ Use the fields we *actually have* from the boxscore now.
                        nhl_id_val = s.get("nhl_id")
                        team_id_val = s.get("team_id")
                        full_name_val = s.get("full_name")

                        # If boxscore didn't include team_id, fall back to roster_status
                        # (latest snapshot for this game+player). Note: roster_status.player_id
                        # is expected to be the NHL player id used by your boxscore iterator.
                        if team_id_val is None and nhl_id_val is not None:
                            with conn.cursor(row_factory=dict_row) as cur2:
                                cur2.execute(
                                    """
                                    SELECT team_id
                                    FROM nhl.roster_status
                                    WHERE game_id = %s
                                      AND player_id = %s
                                      AND team_id IS NOT NULL
                                    ORDER BY asof_ts DESC
                                    LIMIT 1
                                    """,
                                    (int(gpk), int(nhl_id_val)),
                                )
                                rr = cur2.fetchone()
                                if rr and rr.get("team_id") is not None:
                                    team_id_val = int(rr["team_id"])

                        if nhl_id_val is not None:
                            try:
                                ensure_player_exists(
                                    conn,
                                    nhl_id=int(nhl_id_val),
                                    full_name=full_name_val,
                                    team_id=int(team_id_val) if team_id_val is not None else None,
                                )
                            except Exception as e:
                                if os.environ.get(ENV_REQUIRED) == "1":
                                    raise
                                print(
                                    f"[seed_skater_logs] warn: ensure_player_exists failed for nhl_id={nhl_id_val}: {e}"
                                )

                        if nhl_id_val is None:
                            skipped_no_map += 1
                            continue
                        pid = int(nhl_id_val)

                    # teach external id only when we matched via roster path
                    if learned_from_roster and s["nhl_id"] is not None:
                        upsert_external_id(conn, pid, s["nhl_id"])

                    rows.append(
                        (
                            int(pid),
                            int(gpk),
                            SLATE_DATE,
                            int(s["team_id"]) if s.get("team_id") is not None else None,
                            s["sog"],
                            s["attempts"],
                            float(s["toi_min"]) if s["toi_min"] is not None else None,
                            float(s["pp_min"]) if s["pp_min"] is not None else None,
                            s["blocks"],
                        )
                    )

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
                if os.environ.get(ENV_REQUIRED) == "1":
                    raise

    print(f"Done. Upserted total {inserted_total} skater rows for {SLATE_DATE}; skipped_no_map={skipped_no_map_total}")

if __name__ == "__main__":
    main()
