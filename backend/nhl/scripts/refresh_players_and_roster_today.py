#!/usr/bin/env python3
"""
refresh_players_and_roster_today.py

Resilient daily "ensure" step:
- ONLINE: fetch roster per team from api-web.nhle.com, stage players → run
          upsert_players_from_stage.sql; stage roster rows → merge into
          nhl.roster_status (temp table).
- OFFLINE (API down or disabled): derive (game_id, team_id, player_id) from
          v_slate_* feature views and UPSERT nhl.roster_status directly
          (no temp tables). We DO NOT fabricate placeholder player names
          (to avoid CHECK constraints); we only write roster rows whose
          players already exist (or that we can name via API lookup).

Env:
  SLATE_DATE=YYYY-MM-DD (defaults to ET today)
  SUPABASE_DB_URL / DATABASE_URL
  NHL_FETCH_DISABLE=1  # force offline path
"""

import os, sys, datetime as dt, re
from zoneinfo import ZoneInfo

# ---- absolutely disable server-side prepares (must run before importing psycopg) ----
import os
os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

import psycopg
from psycopg.rows import dict_row

# Force every cursor.execute(...) to use simple execution (no PREPARE)
_ORIG_EXECUTE = psycopg.Cursor.execute
def _no_prep_execute(self, query, params=None, **kw):
    kw["prepare"] = False
    return _ORIG_EXECUTE(self, query, params, **kw)
psycopg.Cursor.execute = _no_prep_execute

# Force executemany(...) to avoid PREPARE as well (this is the missing piece)
_ORIG_EXECUTEMANY = psycopg.Cursor.executemany
def _no_prep_executemany(self, query, params_seq=None, **kw):
    kw["prepare"] = False
    return _ORIG_EXECUTEMANY(self, query, params_seq, **kw)
psycopg.Cursor.executemany = _no_prep_executemany

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

BASE = "https://api-web.nhle.com/v1"
FETCH_DISABLED = os.environ.get("NHL_FETCH_DISABLE", "0") == "1"

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SQL_DIR = os.path.join(ROOT, "backend", "nhl", "sql")
UPsertPlayersSQL = os.path.join(SQL_DIR, "upsert_players_from_stage.sql")

# ---------------- HTTP session ----------------
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
PLACEHOLDER_RE = re.compile(r"^\s*(?:player|unknown)\s+\d+\s*$", re.IGNORECASE)

def is_placeholder(name: str | None) -> bool:
    if not name or not str(name).strip():
        return True
    return PLACEHOLDER_RE.match(str(name)) is not None

def season_code_from_date(iso_date: str) -> str:
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

def _safe_str(v):
    return v.strip() if isinstance(v, str) and v.strip() else None

def fetch_player_name_strict(nhl_pid: int | str) -> str | None:
    try:
        resp = S.get(f"{BASE}/player/{nhl_pid}/landing", timeout=8)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        j = resp.json() or {}
        for k in ("fullName", "playerName", "name"):
            v = _safe_str(j.get(k))
            if v and not is_placeholder(v):
                return v
        first, last = _safe_str(j.get("firstName")), _safe_str(j.get("lastName"))
        if first or last:
            nm = f"{first or ''} {last or ''}".strip()
            if nm and not is_placeholder(nm):
                return nm
    except Exception:
        pass
    return None

def _backfill_missing_names(players_stage: list[dict]) -> None:
    need = [row for row in players_stage if not row.get("first_name") and not row.get("last_name")]
    if not need:
        return
    MAX_LOOKUPS = 250
    for row in need[:MAX_LOOKUPS]:
        pid = row["player_id"]
        nm = fetch_player_name_strict(pid)
        if nm:
            parts = nm.split(" ", 1)
            row["first_name"] = parts[0]
            row["last_name"]  = parts[1] if len(parts) > 1 else ""

def upsert_players_from_stage(cur) -> None:
    with open(UPsertPlayersSQL, "r") as f:
        cur.execute(f.read())

def merge_roster_status_from_temp(cur, slate_date: str):
    """
    Merge roster rows either from tmp_import_roster (if present) or,
    as a fallback, from slate feature views for the given slate_date.
    Enforces FK to nhl.players to avoid violations.
    """
    cur.execute("""
        SELECT
          (to_regclass('pg_temp.tmp_import_roster') IS NOT NULL)
          OR (to_regclass('tmp_import_roster') IS NOT NULL) AS has_tmp
    """)
    row = cur.fetchone()
    has_tmp = bool(row["has_tmp"] if isinstance(row, dict) else row[0])

    if has_tmp:
        # NOTE: tmp_import_roster has (game_date, team_id, player_id, active_flag, pp_unit).
        # Resolve the target game_id from date+team, then enforce player FK.
        cur.execute("""
        WITH src AS (
          SELECT DISTINCT
            g.game_id,
            r.team_id,
            r.player_id,
            COALESCE(r.active_flag, TRUE) AS active_flag,
            NULL::text                    AS line_role,
            COALESCE(r.pp_unit, 'None')   AS pp_unit
          FROM tmp_import_roster r
          JOIN nhl.games g
            ON g.game_date = r.game_date::date
           AND (g.home_team_id = r.team_id OR g.away_team_id = r.team_id)
          WHERE g.game_date = %s::date
        ),
        src_checked AS (
          SELECT s.*
          FROM src s
          JOIN nhl.players p ON p.player_id = s.player_id  -- FK guard
        )
        INSERT INTO nhl.roster_status (
          game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
        )
        SELECT
          s.game_id, s.team_id, s.player_id, s.active_flag, s.line_role, s.pp_unit, now()
        FROM src_checked s
        ON CONFLICT (game_id, team_id, player_id)
        DO UPDATE
          SET active_flag = EXCLUDED.active_flag,
              line_role   = COALESCE(EXCLUDED.line_role, nhl.roster_status.line_role),
              pp_unit     = EXCLUDED.pp_unit,
              asof_ts     = now();
        """, (slate_date,))
    else:
        cur.execute("""
        WITH f AS (
          SELECT game_id, team_id, player_id
            FROM nhl.v_slate_sog_features   WHERE game_date = %s::date
          UNION
          SELECT game_id, team_id, player_id
            FROM nhl.v_slate_saves_features WHERE game_date = %s::date
        ),
        src_checked AS (
          SELECT f.*
          FROM f
          JOIN nhl.players p ON p.player_id = f.player_id  -- FK guard
        )
        INSERT INTO nhl.roster_status (
          game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
        )
        SELECT
          sc.game_id, sc.team_id, sc.player_id, TRUE, NULL::text, 'None', now()
        FROM src_checked sc
        ON CONFLICT (game_id, team_id, player_id)
        DO UPDATE
          SET active_flag = TRUE,
              asof_ts     = now();
        """, (slate_date, slate_date))

def ensure_players_exist(cur, player_ids: list[int]) -> None:
    """
    Ensure players exist WITHOUT violating any "no placeholder" CHECKs.
    Only insert when a real, non-placeholder name can be fetched.
    """
    if not player_ids:
        return
    cur.execute("SELECT player_id FROM nhl.players WHERE player_id = ANY(%s);", (player_ids,))
    have_rows = cur.fetchall()
    have = { (r["player_id"] if isinstance(r, dict) else r[0]) for r in have_rows }
    missing = [int(pid) for pid in set(player_ids) if pid not in have]
    if not missing:
        return

    to_insert = []
    for pid in missing:
        nm = fetch_player_name_strict(pid)
        if nm:
            to_insert.append({"pid": pid, "full_name": nm})

    if to_insert:
        cur.executemany(
            """
            INSERT INTO nhl.players (player_id, full_name, position, status, active, updated_at)
            VALUES (%(pid)s, %(full_name)s, 'F', 'active', TRUE, now())
            ON CONFLICT (player_id) DO UPDATE
              SET active = TRUE,
                  updated_at = now();
            """,
            to_insert,
        )
        print(f"[info] players: inserted/activated {len(to_insert)} with real names")
    unresolved = len(missing) - len(to_insert)
    if unresolved > 0:
        print(f"[warn] players: {unresolved} missing player_ids had no safe name; they will be skipped by FK guard")

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
    """Offline UPSERT directly from feature views (no temp table), FK-safe."""
    cur.execute("""
    WITH f AS (
      SELECT game_id, team_id, player_id
        FROM nhl.v_slate_sog_features   WHERE game_date = %s
      UNION
      SELECT game_id, team_id, player_id
        FROM nhl.v_slate_saves_features WHERE game_date = %s
    ),
    src_checked AS (
      SELECT f.*
      FROM f
      JOIN nhl.players p ON p.player_id = f.player_id  -- FK guard
    )
    INSERT INTO nhl.roster_status (
      game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
    )
    SELECT
      sc.game_id, sc.team_id, sc.player_id, TRUE, NULL::text, 'None', now()
    FROM src_checked sc
    ON CONFLICT (game_id, team_id, player_id)
    DO UPDATE SET
      active_flag = TRUE,
      asof_ts     = now();
    """, (slate_date, slate_date))

def fetch_roster(team_tri: str, when_iso: str) -> list[dict]:
    """
    Fetch roster using team tri-code (e.g., 'LAK'). Try /current then explicit season.
    Returns items with person.id, position.code, firstName/lastName when present.
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
        _append_from_section(out, j.get("forwards"), "F")
        _append_from_section(out, j.get("defense"),  "D")
        _append_from_section(out, j.get("goalies"),  "G")

        if not out and isinstance(j.get("roster"), dict):
            r = j["roster"]
            _append_from_section(out, r.get("forwards"), "F")
            _append_from_section(out, r.get("defense"),  "D")
            _append_from_section(out, r.get("goalies"),  "G")

        if out:
            return out

    return []

# ---------------- Main ----------------
def main():
    source = "features-fallback"
    with psycopg.connect(DB_URL, prepare_threshold=0, row_factory=dict_row) as conn:
        try:
            conn.prepare_threshold = 0  # type: ignore[attr-defined]
        except Exception:
            pass

        # 1) Fetch slate games
        with conn.cursor() as cur:
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

        players_stage = []
        roster_rows   = []

        # 2) ONLINE PATH
        if not FETCH_DISABLED:
            try:
                for g in games:
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
                            pos   = _normalize_pos(((item.get("position") or {}) or {}).get("code")) or "F"
                            first = item.get("firstName") or None
                            last  = item.get("lastName")  or None

                            players_stage.append({
                                "player_id": pid,
                                "team_id": int(team_id),
                                "first_name": first,
                                "last_name": last,
                                "position": pos,
                                "shoots_catches": None,
                                "active": True,
                            })
                            roster_rows.append({
                                "game_date": SLATE_DATE,
                                "team_id": int(team_id),
                                "player_id": pid,
                                "active_flag": True,
                                "pp_unit": "None",
                            })
                if players_stage or roster_rows:
                    source = "API"
            except Exception as e:
                print(f"[warn] NHL API fetch failed: {e}")

        # 3) Decide path & write
        with conn.transaction():
            with conn.cursor() as cur:
                if source == "API" and players_stage and roster_rows:
                    try:
                        _backfill_missing_names(players_stage)
                    except NameError:
                        pass

                    cur.execute("TRUNCATE nhl.import_players_stage;")
                    cur.executemany("""
                        INSERT INTO nhl.import_players_stage
                            (player_id, team_id, first_name, last_name, "position", shoots_catches, active)
                        VALUES (%(player_id)s, %(team_id)s, %(first_name)s, %(last_name)s,
                                %(position)s, %(shoots_catches)s, %(active)s)
                    """, players_stage)
                    upsert_players_from_stage(cur)

                    roster_rows = _dedupe_roster_rows(roster_rows)
                    cur.execute("""
                        CREATE TEMP TABLE tmp_import_roster (
                          game_date date,
                          team_id bigint,
                          player_id bigint,
                          active_flag boolean,
                          pp_unit text
                        ) ON COMMIT DROP;
                    """)
                    cur.executemany("""
                        INSERT INTO tmp_import_roster (game_date, team_id, player_id, active_flag, pp_unit)
                        VALUES (%(game_date)s, %(team_id)s, %(player_id)s, %(active_flag)s, %(pp_unit)s)
                    """, roster_rows)

                    merge_roster_status_from_temp(cur, SLATE_DATE)

                else:
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
                    rows = cur.fetchall()
                    pids = [int(r["player_id"] if isinstance(r, dict) else r[0]) for r in rows]

                    ensure_players_exist(cur, pids)
                    upsert_roster_status_from_features(cur, SLATE_DATE)

                cur.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM nhl.roster_status rs
                    JOIN nhl.games g USING (game_id)
                    WHERE g.game_date = %s::date
                """, (SLATE_DATE,))
                row = cur.fetchone()
                total_rs = (row["cnt"] if isinstance(row, dict) else row[0])
                print(f"Refreshed players & roster_status for {SLATE_DATE} (source={source})")
                print(f"✅ roster_status rows present for {SLATE_DATE}: {total_rs}")

if __name__ == "__main__":
    main()
