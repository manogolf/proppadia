#!/usr/bin/env python3
"""
refresh_players_and_roster_today.py

Resilient daily "ensure" step:
- ONLINE: fetch roster per team from api-web.nhle.com, stage players → run
          upsert_players_from_stage.sql; stage roster rows → run
          upsert_roster_status_from_stage.sql (uses DISTINCT + ON CONFLICT).
- OFFLINE (API down or disabled): derive (game_id, team_id, player_id) from
          v_slate_* feature views and UPSERT nhl.roster_status directly
          (no temp tables). Also ensure placeholder players exist so joins
          never break.

Env:
  SLATE_DATE=YYYY-MM-DD (defaults to ET today)
  SUPABASE_DB_URL / DATABASE_URL
  NHL_FETCH_DISABLE=1  # force offline path
"""

import os, sys, datetime as dt
from zoneinfo import ZoneInfo

# ---- absolutely disable server-side prepares (must run before importing psycopg) ----
os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

import psycopg
from psycopg.rows import dict_row
from psycopg import errors as pg_errors

# Force every cursor.execute(...) to use simple execution (no PREPARE)
_ORIG_EXECUTE = psycopg.Cursor.execute
def _no_prep_execute(self, query, params=None, **kw):
    kw["prepare"] = False
    return _ORIG_EXECUTE(self, query, params, **kw)
psycopg.Cursor.execute = _no_prep_execute

# Optional: load .env locally
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---------------- Config ----------------
ET = ZoneInfo("America/New_York")
SLATE_DATE = os.environ.get("SLATE_DATE") or dt.datetime.now(ET).date().isoformat()

DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
if not DB_URL:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB_URL and "&sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
if "?gssencmode=" not in DB_URL and "&gssencmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "gssencmode=disable"

# Prefer api-web (more stable, tri-code based)
BASE = "https://api-web.nhle.com/v1"
FETCH_DISABLED = os.environ.get("NHL_FETCH_DISABLE", "0") == "1"

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SQL_DIR = os.path.join(ROOT, "backend", "nhl", "sql")
UPsertPlayersSQL = os.path.join(SQL_DIR, "upsert_players_from_stage.sql")
MergeRosterSQL   = os.path.join(SQL_DIR, "upsert_roster_status_from_stage.sql")

def _session() -> requests.Session:
    r = Retry(
        total=6, connect=6, read=4,
        backoff_factor=0.75,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia/refresh-players-roster (requests)"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    return s

S = _session()

# ---------------- Helpers ----------------
def season_code_from_date(iso_date: str) -> str:
    """Return NHL season code like '20252026' from 'YYYY-MM-DD'."""
    y, m, _ = map(int, iso_date.split("-"))
    start = y if m >= 7 else y - 1
    return f"{start}{start+1}"

def _normalize_pos(code: str | None) -> str | None:
    if not code:
        return None
    c = str(code).upper().strip()
    if c in {"G", "GOALIE"}: return "G"
    if c in {"D", "LD", "RD", "DEF", "DEFENSE", "DEFENCE"}: return "D"
    if c in {"C", "L", "R", "LW", "RW", "F", "W", "CENTER", "LEFT WING", "RIGHT WING", "FORWARD"}:
        return "F"
    return None

def _extract_names_from_item(p: dict) -> tuple[str|None, str|None]:
    """
    Try multiple shapes from api-web roster payloads.
    Returns (first_name, last_name) or (None, None).
    """
    # flat shapes seen on api-web:
    full = (p.get("fullName") or p.get("playerName") or p.get("name") or "").strip() or None
    first = (p.get("firstName") or "").strip() or None
    last  = (p.get("lastName")  or "").strip() or None

    if (first or last) and not full:
        return first, last
    if full and not first and not last:
        parts = full.split(" ", 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""
        return first or None, (last or None)
    return (first or None), (last or None)

def _backfill_missing_names(players_stage: list[dict]) -> None:
    """
    For staged players missing both first/last names, hit /v1/player/{id}/landing
    to fetch a proper full name (best-effort, bounded).
    Mutates players_stage in place.
    """
    need = [row for row in players_stage if not row.get("first_name") and not row.get("last_name")]
    if not need:
        return

    # Be polite: cap backfill calls per run
    MAX_LOOKUPS = 250
    for row in need[:MAX_LOOKUPS]:
        pid = row["player_id"]
        try:
            resp = S.get(f"{BASE}/player/{pid}/landing", timeout=8)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            j = resp.json() or {}
            nm = (j.get("fullName") or j.get("playerName") or j.get("name") or "").strip()
            if nm:
                parts = nm.split(" ", 1)
                row["first_name"] = parts[0]
                row["last_name"]  = parts[1] if len(parts) > 1 else ""
        except Exception:
            # ignore individual failures; SQL still has a safe fallback
            pass

def upsert_players_from_stage(cur) -> None:
    with open(UPsertPlayersSQL, "r") as f:
        cur.execute(f.read())

def merge_roster_status_from_temp(cur) -> None:
    with open(MergeRosterSQL, "r") as f:
        cur.execute(f.read())

def ensure_players_exist(cur, player_ids: list[int]) -> None:
    """Insert minimal player rows if missing, so joins never break."""
    if not player_ids:
        return
    cur.execute("SELECT player_id FROM nhl.players WHERE player_id = ANY(%s);", (player_ids,))
    have = {row[0] for row in cur.fetchall()}
    missing = [int(pid) for pid in set(player_ids) if pid not in have]
    if not missing:
        return
    cur.executemany("""
        INSERT INTO nhl.players (player_id, full_name, active, updated_at)
        VALUES (%(pid)s, NULL, TRUE, now())
        ON CONFLICT (player_id) DO UPDATE SET active = TRUE, updated_at = now();
    """, [{"pid": pid} for pid in missing])
    print(f"[info] players: inserted/activated {len(missing)} placeholder rows")

def _dedupe_roster_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for r in rows:
        k = (int(r["team_id"]), int(r["player_id"]))
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out

def upsert_roster_status_from_features(cur, slate_date: str) -> None:
    """Offline UPSERT directly from feature views (no temp table)."""
    cur.execute("""
    WITH f AS (
      SELECT game_id, team_id, player_id
        FROM nhl.v_slate_sog_features   WHERE game_date = %s
      UNION
      SELECT game_id, team_id, player_id
        FROM nhl.v_slate_saves_features WHERE game_date = %s
    ),
    new_rows AS (
      SELECT DISTINCT game_id, team_id, player_id FROM f
    )
    INSERT INTO nhl.roster_status (
      game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
    )
    SELECT
      nr.game_id, nr.team_id, nr.player_id, TRUE, NULL::text, 'None', now()
    FROM new_rows nr
    ON CONFLICT (game_id, team_id, player_id)
    DO UPDATE SET
      active_flag = TRUE,
      asof_ts     = now();
    """, (slate_date, slate_date))

def _extract_names_from_item(p: dict) -> tuple[str|None, str|None]:
    """
    Try multiple shapes from api-web roster payloads.
    Returns (first_name, last_name) or (None, None).
    """
    # flat shapes seen on api-web:
    full = (p.get("fullName") or p.get("playerName") or p.get("name") or "").strip() or None
    first = (p.get("firstName") or "").strip() or None
    last  = (p.get("lastName")  or "").strip() or None

    if (first or last) and not full:
        return first, last
    if full and not first and not last:
        parts = full.split(" ", 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""
        return first or None, (last or None)
    return (first or None), (last or None)

def _backfill_missing_names(players_stage: list[dict]) -> None:
    """
    For staged players missing both first/last names, hit /v1/player/{id}/landing
    to fetch a proper full name (best-effort, bounded).
    Mutates players_stage in place.
    """
    need = [row for row in players_stage if not row.get("first_name") and not row.get("last_name")]
    if not need:
        return

    # Be polite: cap backfill calls per run
    MAX_LOOKUPS = 250
    for row in need[:MAX_LOOKUPS]:
        pid = row["player_id"]
        try:
            resp = S.get(f"{BASE}/player/{pid}/landing", timeout=8)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            j = resp.json() or {}
            nm = (j.get("fullName") or j.get("playerName") or j.get("name") or "").strip()
            if nm:
                parts = nm.split(" ", 1)
                row["first_name"] = parts[0]
                row["last_name"]  = parts[1] if len(parts) > 1 else ""
        except Exception:
            # ignore individual failures; SQL still has a safe fallback
            pass

def fetch_roster(team_tri: str, when_iso: str) -> list[dict]:
    """
    Fetch roster using team tri-code (e.g., 'LAK'). Try /current then explicit season.
    Returns items with:
      {
        "person":   {"id": <int>},
        "position": {"code": "F"|"D"|"G"},
        "firstName": <str|None>,
        "lastName":  <str|None>,
      }
    """
    tri = str(team_tri).upper()
    season = season_code_from_date(when_iso)
    urls = [
        f"{BASE}/roster/{tri}/current",
        f"{BASE}/roster/{tri}/{season}",
    ]

    def _append_from_section(out: list, section: list | None, default_pos: str):
        for p in (section or []):
            pid = p.get("id") or p.get("playerId") or (p.get("player") or {}).get("id")
            if not pid:
                continue
            pos = _normalize_pos(p.get("positionCode") or p.get("position") or default_pos) or "F"
            # Use safe guards: api-web sometimes returns dicts for name fields
            first = _safe_str(p.get("firstName"))
            last  = _safe_str(p.get("lastName"))
            out.append({
                "person":   {"id": int(pid)},
                "position": {"code": pos},
                "firstName": first,
                "lastName":  last,
            })

    for url in urls:
        resp = S.get(url, timeout=20)
        if resp.status_code == 404:
            continue
        resp.raise_for_status()
        j = resp.json() or {}

        out: list[dict] = []
        # common keys
        _append_from_section(out, j.get("forwards"), "F")
        _append_from_section(out, j.get("defense"),  "D")
        _append_from_section(out, j.get("goalies"),  "G")

        # wrapped shape: { "roster": { ... } }
        if not out and isinstance(j.get("roster"), dict):
            r = j["roster"]
            _append_from_section(out, r.get("forwards"), "F")
            _append_from_section(out, r.get("defense"),  "D")
            _append_from_section(out, r.get("goalies"),  "G")

        if out:
            return out

    # No roster found for either URL
    return []

def _safe_str(v):
    """Return a trimmed string or None; ignores non-strings (e.g., dicts from api-web)."""
    return v.strip() if isinstance(v, str) and v.strip() else None



# ---------------- Main ----------------
def main():
    source = "features-fallback"
    with psycopg.connect(DB_URL, prepare_threshold=0, row_factory=dict_row) as conn:
        try:
            conn.prepare_threshold = 0  # type: ignore[attr-defined]
        except Exception:
            pass

        with conn.transaction():
            with conn.cursor() as cur:
                # Get slate games & tri-codes (we'll use tri to call the API)
                cur.execute("""
                    SELECT
                      g.game_id,
                      g.home_team_id,
                      g.away_team_id,
                      ht.abbr AS home_tri,
                      at.abbr AS away_tri
                    FROM nhl.games g
                    JOIN nhl.teams ht ON ht.team_id = g.home_team_id
                    JOIN nhl.teams at ON at.team_id = g.away_team_id
                   WHERE g.game_date = %s::date
                   ORDER BY g.game_id
                """, (SLATE_DATE,))
                games = cur.fetchall()

                if not games:
                    print(f"ℹ️ No games for {SLATE_DATE} in nhl.games; run import_schedule_today.py first.")
                    return

                players_stage = []  # rows for nhl.import_players_stage
                roster_rows   = []  # rows for tmp_roster_stage

        # -------- ONLINE PATH: fetch team rosters (api-web) and stage players/roster --------
        if not FETCH_DISABLED:
            try:
                for g in games:
                    # iterate both teams for this game
                    for tri, team_id in ((g["home_tri"], g["home_team_id"]),
                                         (g["away_tri"], g["away_team_id"])):
                        try:
                            roster = fetch_roster(tri, SLATE_DATE) or []
                        except Exception as e:
                            print(f"[warn] roster fetch failed for {tri}: {e}")
                            roster = []

                        for item in roster:
                            person = item.get("person") or {}
                            pid = person.get("id")
                            if pid is None:
                                continue
                            pid = int(pid)

                            # normalize position & try to carry first/last name if present
                            pos   = _normalize_pos(((item.get("position") or {}) or {}).get("code")) or "F"
                            first = item.get("firstName") or None
                            last  = item.get("lastName")  or None

                            # stage player row (names may be None; we can backfill later)
                            players_stage.append({
                                "player_id": pid,
                                "team_id": int(team_id),
                                "first_name": first,
                                "last_name": last,
                                "position": pos,
                                "shoots_catches": None,
                                "active": True,
                            })

                            # stage roster_status row
                            roster_rows.append({
                                "game_date": SLATE_DATE,
                                "team_id": int(team_id),
                                "player_id": pid,
                                "active_flag": True,
                                "pp_unit": "None",
                            })

                if players_stage or roster_rows:
                    # Optional: backfill missing names using /v1/player/{id}/landing if helper exists
                    try:
                        _backfill_missing_names(players_stage)  # no-op if you didn't add the helper
                    except NameError:
                        pass
                    source = "API"
            except Exception as e:
                print(f"[warn] NHL API fetch failed: {e}")

                if source == "API" and players_stage and roster_rows:
                    # ONLINE PATH: stage → upsert players; stage → merge roster
                    # 1) Stage players
                    if not FETCH_DISABLED:
                        _backfill_missing_names(players_stage)

                    cur.execute("TRUNCATE nhl.import_players_stage;")
                    cur.executemany("""
                        INSERT INTO nhl.import_players_stage
                          (player_id, team_id, first_name, last_name, "position", shoots_catches, active)
                        VALUES (%(player_id)s, %(team_id)s, %(first_name)s, %(last_name)s,
                                %(position)s, %(shoots_catches)s, %(active)s)
                    """, players_stage)
                    upsert_players_from_stage(cur)

                    # 2) Stage roster & merge (de-dupe first)
                    roster_rows = _dedupe_roster_rows(roster_rows)
                    cur.execute("""
                        CREATE TEMP TABLE tmp_roster_stage (
                          game_date date, team_id bigint, player_id bigint, active_flag boolean, pp_unit text
                        ) ON COMMIT DROP;
                    """)
                    cur.executemany("""
                        INSERT INTO tmp_roster_stage (game_date, team_id, player_id, active_flag, pp_unit)
                        VALUES (%(game_date)s, %(team_id)s, %(player_id)s, %(active_flag)s, %(pp_unit)s)
                    """, roster_rows)
                    merge_roster_status_from_temp(cur)
                else:
                    # OFFLINE PATH: ensure players exist for slate → upsert roster from features
                    # Get all (game_id, team_id, player_id) for slate and ensure players exist
                    cur.execute("""
                        WITH f AS (
                          SELECT game_id, team_id, player_id
                            FROM nhl.v_slate_sog_features   WHERE game_date = %s
                          UNION
                          SELECT game_id, team_id, player_id
                            FROM nhl.v_slate_saves_features WHERE game_date = %s
                        )
                        SELECT DISTINCT player_id FROM f
                    """, (SLATE_DATE, SLATE_DATE))
                    pid_rows = cur.fetchall()
                    ensure_players_exist(cur, [int(r[0]) for r in pid_rows])

                    # Upsert roster_status directly
                    upsert_roster_status_from_features(cur, SLATE_DATE)

                # For logging: how many roster_status rows exist for this slate?
                cur.execute("""
                    SELECT COUNT(*)
                      FROM nhl.roster_status rs
                      JOIN nhl.games g USING (game_id)
                     WHERE g.game_date = %s::date
                """, (SLATE_DATE,))
                total_rs = cur.fetchone()[0]
                print(f"Refreshed players & roster_status for {SLATE_DATE} (source={source})")
                print(f"✅ roster_status rows present for {SLATE_DATE}: {total_rs}")

if __name__ == "__main__":
    main()
