#!/usr/bin/env python3
import os, sys, json, datetime as dt
from zoneinfo import ZoneInfo

# ---- absolutely disable server-side prepares (must run before importing psycopg) ----
os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

import psycopg
from psycopg.rows import dict_row  # (not strictly needed here, but fine to keep)
from psycopg import errors as pg_errors  # noqa: F401  (may be unused depending on path)

# Force every cursor.execute(...) to use simple execution (no PREPARE)
_ORIG_EXECUTE = psycopg.Cursor.execute
def _no_prep_execute(self, query, params=None, **kw):
    kw["prepare"] = False
    return _ORIG_EXECUTE(self, query, params, **kw)
psycopg.Cursor.execute = _no_prep_execute

# optional: load .env locally when running on your laptop
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

ET = ZoneInfo("America/New_York")
DATE = os.getenv("SLATE_DATE") or dt.datetime.now(ET).date().isoformat()

DB = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB and "&sslmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "sslmode=require"
if "?gssencmode=" not in DB and "&gssencmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "gssencmode=disable"

BASE = "https://api-web.nhle.com/v1"

def _session() -> requests.Session:
    r = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-roster/1.0"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s

S = _session()

def season_code_from_date(iso_date: str) -> str:
    """Return NHL season code like '20252026' from 'YYYY-MM-DD'."""
    y, m, _ = map(int, iso_date.split("-"))
    start = y if m >= 7 else y - 1
    return f"{start}{start+1}"

def _normalize_apiweb_roster(j: dict) -> list[dict]:
    """
    Normalize api-web roster shapes to a StatsAPI-like list:
      [{'person': {'id': <int>}, 'position': {'code': 'G'|'D'|'C'|'L'|'R'}}]
    """
    out = []
    for key, default_pos in (("forwards", "F"), ("defense", "D"), ("goalies", "G")):
        for p in (j.get(key) or []):
            pid = p.get("id") or p.get("playerId") or (p.get("player") or {}).get("id")
            if not pid:
                continue
            pos = (p.get("positionCode") or p.get("position") or default_pos or "").upper()
            out.append({"person": {"id": int(pid)}, "position": {"code": pos}})
    if not out and isinstance(j.get("roster"), dict):
        return _normalize_apiweb_roster(j["roster"])
    return out

def fetch_roster(team_tri: str, when_iso: str = DATE) -> list[dict]:
    """
    Fetch roster using team tri-code (e.g., 'LAK'). Try /current then explicit season.
    Returns normalized list compatible with downstream code.
    """
    tri = str(team_tri).upper()
    season = season_code_from_date(when_iso)
    urls = [
        f"{BASE}/roster/{tri}/current",
        f"{BASE}/roster/{tri}/{season}",
    ]
    for url in urls:
        resp = S.get(url, timeout=20)
        if resp.status_code == 404:
            continue
        resp.raise_for_status()
        return _normalize_apiweb_roster(resp.json() or {})
    print(f"⚠️ roster 404 for {tri} on {when_iso} (tried current & {season})")
    return []

def _normalize_pos(code: str | None) -> str | None:
    if not code:
        return None
    c = str(code).upper().strip()
    if c in {"G", "GOALIE"}:
        return "G"
    if c in {"D", "LD", "RD", "DEF", "DEFENSE", "DEFENCE"}:
        return "D"
    if c in {"C", "L", "R", "LW", "RW", "F", "W", "CENTER", "LEFT WING", "RIGHT WING", "FORWARD"}:
        return "F"
    return None

def fetch_player_name(nhl_pid: str) -> str | None:
    """Best-effort: fetch a single player's profile to get a reliable full name."""
    try:
        resp = S.get(f"{BASE}/player/{nhl_pid}/landing", timeout=8)
        resp.raise_for_status()
        j = resp.json() or {}
        for k in ("fullName", "playerName", "name"):
            v = j.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        first = j.get("firstName"); last = j.get("lastName")
        if isinstance(first, str) and isinstance(last, str):
            nm = f"{first.strip()} {last.strip()}".strip()
            return nm or None
    except Exception:
        pass
    return None

def main():
    # PgBouncer-safe: no server-side prepares, all executes inside one cursor
    with psycopg.connect(DB, prepare_threshold=0) as conn:
        try:
            conn.prepare_threshold = 0  # type: ignore[attr-defined]
        except Exception:
            pass

        inserted_rs = 0
        upserted_players = 0
        seeded_maps = 0

        def norm_pos(code: str | None) -> str:
            c = (code or "").upper()
            if c in ("G", "D"):
                return c
            if c in ("LW", "RW", "C", "F"):
                return "F"
            return "F"

        try:
            # ---------- PLAYER UPSERTS (separate cursor context) ----------
            with conn.cursor() as cur:
                # 1) Get today’s games (ET day) with team tri-codes (use tris for roster fetch)
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
                """, (DATE,))
                games = cur.fetchall()
                if not games:
                    print(f"ℹ️ No games for {DATE} in nhl.games; run import_schedule_today.py first.")
                    return

                # 2) Existing player map: provider_player_id (nhl) -> internal player_id
                cur.execute("""
                    SELECT provider_player_id::text, player_id
                    FROM nhl.player_external_ids
                    WHERE provider = 'nhl'
                """)
                player_map_by_provider = {pid: pl for (pid, pl) in cur.fetchall()}

                # 3) For each team in each game: fetch roster, upsert players + mappings
                for game_id, home_tid, away_tid, home_tri, away_tri in games:
                    for tri, tid in ((home_tri, home_tid), (away_tri, away_tid)):
                        try:
                            roster = fetch_roster(tri, DATE) or []
                        except Exception as e:
                            print(f"[warn] roster fetch failed for {tri}: {e}")
                            roster = []

                        for item in roster:
                            person = item.get("person") or {}
                            nhl_pid_raw = person.get("id")
                            if nhl_pid_raw is None:
                                continue
                            nhl_pid = str(nhl_pid_raw)

                            # best-effort name (keeps NOT NULL happy even if offline)
                            full_name = fetch_player_name(nhl_pid) or f"Player {nhl_pid}"
                            pos = norm_pos((item.get("position") or {}).get("code"))

                            internal_pid = player_map_by_provider.get(nhl_pid)
                            if internal_pid is None:
                                internal_pid = int(nhl_pid)
                                cur.execute("""
                                    INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                                    VALUES (%s, %s, %s, %s, 'active')
                                    ON CONFLICT (player_id) DO UPDATE
                                      SET full_name       = EXCLUDED.full_name,
                                          current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                                          position        = EXCLUDED.position,
                                          status          = 'active'
                                """, (internal_pid, full_name, tid, pos))
                                upserted_players += 1

                                cur.execute("""
                                    INSERT INTO nhl.player_external_ids (player_id, provider, provider_player_id)
                                    VALUES (%s, 'nhl', %s)
                                    ON CONFLICT (player_id, provider) DO UPDATE
                                      SET provider_player_id = EXCLUDED.provider_player_id
                                """, (internal_pid, nhl_pid))
                                player_map_by_provider[nhl_pid] = internal_pid
                                seeded_maps += 1
                            else:
                                cur.execute("""
                                    UPDATE nhl.players
                                       SET full_name       = COALESCE(NULLIF(%s,''), full_name),
                                           current_team_id = COALESCE(%s, current_team_id),
                                           position        = %s,
                                           status          = 'active'
                                     WHERE player_id = %s
                                """, (full_name, tid, pos, internal_pid))

            # ---------- ROSTER_STATUS UPSERT (separate cursor context) ----------
            if os.environ.get("SKIP_ROSTER_STATUS", "1") == "1":
                print("↪︎ SKIP_ROSTER_STATUS=1: importer will not write nhl.roster_status")
            else:
                slate_date = os.environ.get("SLATE_DATE") or DATE
                with conn.cursor() as cur:
                    # Check if the session-local temp table exists (avoid exceptions entirely)
                    cur.execute("""
                      SELECT (to_regclass('pg_temp.tmp_import_roster') IS NOT NULL)
                          OR (to_regclass('tmp_import_roster') IS NOT NULL) AS has_tmp
                    """)
                    has_tmp = bool(cur.fetchone()[0])

                    if has_tmp:
                        # Use rows staged by the importer
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
                            ON g.game_date = r.game_date
                           AND (g.home_team_id = r.team_id OR g.away_team_id = r.team_id)
                        )
                        INSERT INTO nhl.roster_status (
                          game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts
                        )
                        SELECT
                          s.game_id, s.team_id, s.player_id, s.active_flag, s.line_role, s.pp_unit, now()
                        FROM src s
                        ON CONFLICT (game_id, team_id, player_id)
                        DO UPDATE
                        SET active_flag = EXCLUDED.active_flag,
                            line_role   = COALESCE(EXCLUDED.line_role, nhl.roster_status.line_role),
                            pp_unit     = EXCLUDED.pp_unit,
                            asof_ts     = now();
                        """)
                    else:
                        # Offline/fallback: derive from feature views for this slate_date
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
                        DO UPDATE
                        SET active_flag = TRUE,
                            asof_ts     = now();
                        """, (slate_date, slate_date))

            # ---------- LOGGING COUNT (separate cursor context) ----------
            with conn.cursor() as cur:
                cur.execute("""
                  SELECT COUNT(*)
                    FROM nhl.roster_status rs
                    JOIN nhl.games g USING (game_id)
                   WHERE g.game_date = %s::date
                """, (DATE,))
                inserted_rs = cur.fetchone()[0]

            # ---------- COMMIT ONCE ----------
            conn.commit()

        except Exception:
            # make sure an earlier SQL error doesn't poison the rest of the run
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        print(f"🔧 Upserted/confirmed {upserted_players} players in nhl.players")
        print(f"🔄 Seeded {seeded_maps} mappings in nhl.player_external_ids (provider='nhl')")
        print(f"✅ roster_status rows present for {DATE}: {inserted_rs}")

if __name__ == "__main__":
    main()
