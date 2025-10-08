#!/usr/bin/env python3
import os, sys, json, datetime as dt
from zoneinfo import ZoneInfo
import os
os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

# optional: load .env locally
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

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
    # common api-web keys: 'forwards', 'defense', 'goalies'
    for key, default_pos in (("forwards", "F"), ("defense", "D"), ("goalies", "G")):
        for p in (j.get(key) or []):
            pid = p.get("id") or p.get("playerId") or (p.get("player") or {}).get("id")
            if not pid:
                continue
            pos = (p.get("positionCode") or p.get("position") or default_pos or "").upper()
            out.append({"person": {"id": int(pid)}, "position": {"code": pos}})

    # some payloads are wrapped: {"roster": {...}}
    if not out and isinstance(j.get("roster"), dict):
        return _normalize_apiweb_roster(j["roster"])
    return out

def fetch_roster(team_tri: str, when_iso: str = DATE) -> list[dict]:
    """
    Fetch roster using team tri-code (e.g., 'LAK'). Try /current then explicit season.
    Returns normalized list compatible with your downstream code.
    """
    tri = str(team_tri).upper()
    season = season_code_from_date(when_iso)
    urls = [
        f"https://api-web.nhle.com/v1/roster/{tri}/current",
        f"https://api-web.nhle.com/v1/roster/{tri}/{season}",
    ]
    for url in urls:
        resp = requests.get(url, timeout=20)
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
    # Goalie
    if c in {"G", "GOALIE"}:
        return "G"
    # Defense
    if c in {"D", "LD", "RD", "DEF", "DEFENSE", "DEFENCE"}:
        return "D"
    # Forwards (collapse everything to F)
    if c in {"C", "L", "R", "LW", "RW", "F", "W", "CENTER", "LEFT WING", "RIGHT WING", "FORWARD"}:
        return "F"
    return None

def _extract_position_code(item: dict) -> str | None:
    pos = item.get("position")
    if isinstance(pos, dict) and isinstance(pos.get("code"), str):
        return _normalize_pos(pos["code"])
    if isinstance(item.get("positionCode"), str):
        return _normalize_pos(item["positionCode"])
    if isinstance(item.get("position"), str):
        return _normalize_pos(item["position"])
    return None

def _extract_player_id(item: dict) -> str | None:
    if isinstance(item.get("person"), dict) and "id" in item["person"]:
        return str(item["person"]["id"])
    if "id" in item and isinstance(item["id"], (int, str)):
        return str(item["id"])
    if "playerId" in item and isinstance(item["playerId"], (int, str)):
        return str(item["playerId"])
    return None

def fetch_player_name(nhl_pid: str) -> str | None:
    """
    Try to fetch a single player's profile to get a reliable full name.
    Endpoint observed: /v1/player/{id}/landing
    """
    try:
        resp = S.get(f"{BASE}/player/{nhl_pid}/landing", timeout=8)
        resp.raise_for_status()
        j = resp.json()
        # common fields: fullName, firstName, lastName, playerName
        for k in ("fullName", "playerName", "name"):
            if isinstance(j.get(k), str) and j[k].strip():
                return j[k].strip()
        first = j.get("firstName"); last = j.get("lastName")
        if isinstance(first, str) and isinstance(last, str):
            nm = f"{first.strip()} {last.strip()}".strip()
            return nm or None
    except Exception:
        pass
    return None

def _extract_player_name(item: dict) -> str | None:
    p = item.get("person")
    if isinstance(p, dict):
        for k in ("fullName", "name"):
            v = p.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        first = p.get("firstName"); last = p.get("lastName")
        if isinstance(first, str) and isinstance(last, str):
            nm = f"{first.strip()} {last.strip()}".strip()
            if nm:
                return nm
    # flat shapes
    for k in ("fullName", "playerName", "name"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    first = item.get("firstName"); last = item.get("lastName")
    if isinstance(first, str) and isinstance(last, str):
        nm = f"{first.strip()} {last.strip()}".strip()
        if nm:
            return nm
    return None

def fetch_player_info(nhl_pid: str) -> tuple[str | None, str | None]:
    """
    Fetches player profile to get reliable name and position.
    Observed endpoint: /v1/player/{id}/landing
    Returns (full_name, position_code) where position_code is normalized.
    """
    try:
        resp = S.get(f"{BASE}/player/{nhl_pid}/landing", timeout=8)
        resp.raise_for_status()
        j = resp.json()

        # name
        nm = None
        for k in ("fullName", "playerName", "name"):
            v = j.get(k)
            if isinstance(v, str) and v.strip():
                nm = v.strip(); break
        if not nm:
            first = j.get("firstName"); last = j.get("lastName")
            if isinstance(first, str) and isinstance(last, str):
                nm = f"{first.strip()} {last.strip()}".strip() or None

        # position
        pos = j.get("position") or j.get("positionCode")
        pos = _normalize_pos(pos)

        return nm, pos
    except Exception:
        return None, None
    
def _get_table_columns(cur, schema: str, table: str) -> list[str]:
    cur.execute("""
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
    """, (schema, table))
    return [r[0] for r in cur.fetchall()]

def _ensure_players_and_mappings(cur, rosters: dict[str, list[dict]]) -> tuple[int, int]:
    """
    Ensure nhl.players(player_id, full_name, position?) and nhl.player_external_ids mappings exist.
    Guarantees non-null full_name and position if those columns are NOT NULL.
    """
    # discover columns + nullability
    cur.execute("""
        select column_name, is_nullable
        from information_schema.columns
        where table_schema='nhl' and table_name='players'
    """)
    cols = {name: (nullable == "YES") for (name, nullable) in cur.fetchall()}
    has_full_name = "full_name" in cols
    full_name_nullable = cols.get("full_name", True)
    has_position = "position" in cols
    position_nullable = cols.get("position", True)

    # existing external mappings
    cur.execute("""
        select provider_player_id::text, player_id
        from nhl.player_external_ids
        where provider = 'nhl'
    """)
    existing_map = {pid: plid for (pid, plid) in cur.fetchall()}

    players_upserted = 0
    mappings_upserted = 0

    seen = set()
    for tri, items in rosters.items():
        for it in items:
            nhl_pid = _extract_player_id(it)
            if not nhl_pid or nhl_pid in seen:
                continue
            seen.add(nhl_pid)

            # gather name/pos from roster
            name = _extract_player_name(it)
            pos = _extract_position_code(it)

            # backfill from profile if required fields missing
            need_name = has_full_name and not full_name_nullable and not name
            need_pos  = has_position and not position_nullable and not pos
            if need_name or need_pos:
                prof_name, prof_pos = fetch_player_info(nhl_pid)
                if need_name and prof_name:
                    name = prof_name
                if need_pos and prof_pos:
                    pos = prof_pos

            # enforce non-null if required (final fallback)
            if has_full_name and not full_name_nullable and not name:
                name = f"Player {nhl_pid}"
            if has_position and not position_nullable and not pos:
                pos = "F"  # <- was "C"; use "F" to satisfy check constraint
            # Build a dynamic insert/upsert based on columns present
            insert_cols = ["player_id"]
            insert_vals = [int(nhl_pid)]
            update_sets = []  # coalesce updates to avoid overwriting with NULL

            if has_full_name:
                insert_cols.append("full_name")
                insert_vals.append(name)
                update_sets.append("full_name = coalesce(excluded.full_name, nhl.players.full_name)")

            if has_position:
                insert_cols.append("position")
                insert_vals.append(pos)
                update_sets.append("position = coalesce(excluded.position, nhl.players.position)")

            cols_sql = ", ".join(insert_cols)
            placeholders = ", ".join(["%s"] * len(insert_vals))
            update_sql = ", ".join(update_sets) if update_sets else "player_id = nhl.players.player_id"

            cur.execute(
                f"""
                insert into nhl.players ({cols_sql})
                values ({placeholders})
                on conflict (player_id)
                do update set {update_sql}
                """,
                insert_vals,
            )
            players_upserted += 1

            # external id mapping
            if nhl_pid not in existing_map:
                cur.execute("""
                    insert into nhl.player_external_ids (player_id, provider, provider_player_id)
                    values (%s, 'nhl', %s)
                    on conflict (provider, provider_player_id)
                    do update set player_id = excluded.player_id
                """, (int(nhl_pid), nhl_pid))
                mappings_upserted += 1

    if players_upserted:
        print(f"🔧 Upserted/confirmed {players_upserted} players in nhl.players")
    if mappings_upserted:
        print(f"🔄 Seeded {mappings_upserted} mappings in nhl.player_external_ids (provider='nhl')")
    return players_upserted, mappings_upserted

def main():
    # PgBouncer-safe: no server-side prepares, all executes inside one cursor
    with psycopg.connect(DB, prepare_threshold=0) as conn:
        # extra safety in case the kwarg gets lost in a refactor
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

                # 3) For each team in each game: fetch roster, upsert players + mappings, insert roster_status
                for game_id, home_tid, away_tid, home_tri, away_tri in games:
                    for tri, tid in ((home_tri, home_tid), (away_tri, away_tid)):
                        roster = fetch_roster(tri, DATE) or []
                        for item in roster:
                            person = item.get("person") or {}
                            nhl_pid = person.get("id")
                            if nhl_pid is None:
                                continue
                            nhl_pid = str(nhl_pid)
                            full_name = (person.get("fullName") or f"Player {nhl_pid}").strip()
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

                            cur.execute("""
                                INSERT INTO nhl.roster_status
                                  (game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts)
                                VALUES (%s, %s, %s, true, NULL, 'None', now())
                                ON CONFLICT (team_id, asof_ts, game_id, player_id) DO NOTHING
                            """, (game_id, tid, internal_pid))
                            inserted_rs += 1

            # commit once after the cursor block
            conn.commit()

        except Exception as e:
            # make sure an earlier SQL error doesn't poison the rest of the run
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        print(f"🔧 Upserted/confirmed {upserted_players} players in nhl.players")
        print(f"🔄 Seeded {seeded_maps} mappings in nhl.player_external_ids (provider='nhl')")
        print(f"✅ Inserted {inserted_rs} roster_status rows for {DATE}.")


if __name__ == "__main__":
    main()
