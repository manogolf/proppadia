#!/usr/bin/env python3
"""
refresh_all_team_rosters.py

Daily full-league NHL roster refresh:
- Fetch rosters for every team in nhl.teams
- Upsert/refresh nhl.players + nhl.player_external_ids
- Mark in-snapshot players active
- Mark out-of-snapshot players inactive (guarded by team-success threshold)

Env:
  SLATE_DATE=YYYY-MM-DD   # optional; used to pick season fallback
  DATABASE_URL / SUPABASE_DB_URL
"""

from __future__ import annotations

import datetime as dt
import os
import re
import sys
from zoneinfo import ZoneInfo

os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ET = ZoneInfo("America/New_York")
DATE = os.getenv("SLATE_DATE") or dt.datetime.now(ET).date().isoformat()
BASE = "https://api-web.nhle.com/v1"
PLACEHOLDER_RE = re.compile(r"^\s*(?:player|unknown)\s+\d+\s*$", re.IGNORECASE)


def _db_url() -> str:
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URL / DATABASE_URL")
    if "?sslmode=" not in db and "&sslmode=" not in db:
        db += ("&" if "?" in db else "?") + "sslmode=require"
    if "?gssencmode=" not in db and "&gssencmode=" not in db:
        db += ("&" if "?" in db else "?") + "gssencmode=disable"
    return db


def _session() -> requests.Session:
    r = Retry(
        total=3,
        connect=3,
        read=2,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia/nhl-full-roster-refresh"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    return s


def _season_start_year(iso_date: str) -> int:
    y, m, _ = map(int, iso_date.split("-"))
    return y if m >= 7 else y - 1


def _safe_str(v):
    return v.strip() if isinstance(v, str) and v.strip() else None


def _is_placeholder(name: str | None) -> bool:
    if not name or not str(name).strip():
        return True
    return PLACEHOLDER_RE.match(str(name)) is not None


def _normalize_pos(code: str | None) -> str:
    if not code:
        return "F"
    c = str(code).upper().strip()
    if c in {"G", "GOALIE"}:
        return "G"
    if c in {"D", "LD", "RD", "DEF", "DEFENSE", "DEFENCE"}:
        return "D"
    return "F"


def _append_from_section(out: list[dict], section: list | None, default_pos: str) -> None:
    for p in (section or []):
        pid = p.get("id") or p.get("playerId") or (p.get("player") or {}).get("id")
        if not pid:
            continue
        out.append(
            {
                "player_id": int(pid),
                "position": _normalize_pos(p.get("positionCode") or p.get("position") or default_pos),
                "first_name": _safe_str(p.get("firstName")),
                "last_name": _safe_str(p.get("lastName")),
            }
        )


def _fetch_roster(session: requests.Session, team_tri: str, when_iso: str) -> list[dict]:
    tri = str(team_tri).upper()
    season = _season_start_year(when_iso)
    urls = [
        f"{BASE}/roster/{tri}/current",
        f"{BASE}/roster/{tri}/{season}",
    ]
    for url in urls:
        resp = session.get(url, timeout=20)
        if resp.status_code == 404:
            continue
        resp.raise_for_status()
        j = resp.json() or {}
        out: list[dict] = []
        _append_from_section(out, j.get("forwards"), "F")
        _append_from_section(out, j.get("defensemen") or j.get("defense"), "D")
        _append_from_section(out, j.get("goalies"), "G")
        if not out and isinstance(j.get("roster"), dict):
            r = j["roster"]
            _append_from_section(out, r.get("forwards"), "F")
            _append_from_section(out, r.get("defensemen") or r.get("defense"), "D")
            _append_from_section(out, r.get("goalies"), "G")
        if out:
            return out
    return []


def _fetch_player_name(session: requests.Session, nhl_pid: int) -> str | None:
    try:
        resp = session.get(f"{BASE}/player/{nhl_pid}/landing", timeout=8)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        j = resp.json() or {}
        for k in ("fullName", "playerName", "name"):
            v = _safe_str(j.get(k))
            if v and not _is_placeholder(v):
                return v
        first = _safe_str(j.get("firstName"))
        last = _safe_str(j.get("lastName"))
        if first or last:
            nm = f"{first or ''} {last or ''}".strip()
            if nm and not _is_placeholder(nm):
                return nm
    except Exception:
        return None
    return None


def main() -> int:
    db = _db_url()
    session = _session()
    fetched_active_ids: set[int] = set()
    player_name_cache: dict[int, str | None] = {}
    team_success = 0
    team_fail = 0
    upserted_players = 0
    updated_existing = 0

    with psycopg.connect(db, prepare_threshold=None) as conn:
        try:
            conn.prepare_threshold = None  # type: ignore[attr-defined]
        except Exception:
            pass

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT team_id, team
                FROM nhl.teams
                WHERE team IS NOT NULL AND team <> ''
                ORDER BY team
                """
            )
            teams = cur.fetchall()

        total_teams = len(teams)
        if total_teams == 0:
            print("FAIL no teams found in nhl.teams")
            return 1

        with conn.transaction():
            with conn.cursor() as cur:
                for idx, (team_id, team_tri) in enumerate(teams, start=1):
                    print(f"[{idx}/{total_teams}] fetching roster for {team_tri} ...", flush=True)
                    try:
                        roster = _fetch_roster(session, team_tri, DATE)
                    except Exception as e:
                        team_fail += 1
                        print(f"[warn] roster fetch failed for {team_tri}: {type(e).__name__}: {e}")
                        continue

                    team_success += 1
                    print(f"[{idx}/{total_teams}] {team_tri}: roster rows={len(roster)}", flush=True)
                    for row in roster:
                        pid = int(row["player_id"])
                        fetched_active_ids.add(pid)
                        pos = _normalize_pos(row.get("position"))
                        first = _safe_str(row.get("first_name"))
                        last = _safe_str(row.get("last_name"))
                        full_name = f"{first or ''} {last or ''}".strip() if (first or last) else None
                        if not full_name or _is_placeholder(full_name):
                            if pid not in player_name_cache:
                                player_name_cache[pid] = _fetch_player_name(session, pid)
                            full_name = player_name_cache.get(pid)

                        cur.execute("SELECT full_name FROM nhl.players WHERE player_id = %s::bigint", (pid,))
                        existing = cur.fetchone()
                        if existing is None:
                            if not full_name or _is_placeholder(full_name):
                                continue
                            cur.execute(
                                """
                                INSERT INTO nhl.players
                                  (player_id, full_name, current_team_id, team_id, position, status, active, updated_at)
                                VALUES
                                  (%s::bigint, %s::text, %s::bigint, %s::int, %s::text, 'active', TRUE, now())
                                ON CONFLICT (player_id) DO NOTHING
                                """,
                                (pid, full_name, team_id, team_id, pos),
                            )
                            upserted_players += 1
                        else:
                            cur.execute(
                                """
                                UPDATE nhl.players
                                   SET current_team_id = %s::bigint,
                                       team_id = %s::int,
                                       position = COALESCE(%s::text, position),
                                       status = 'active',
                                       active = TRUE,
                                       updated_at = now(),
                                       full_name = CASE
                                                     WHEN %s::text IS NOT NULL
                                                      AND %s::text <> ''
                                                      AND %s::text !~* '^(player|unknown)\\s+\\d+$'
                                                       THEN %s::text
                                                     ELSE full_name
                                                   END
                                 WHERE player_id = %s::bigint
                                """,
                                (team_id, team_id, pos, full_name, full_name, full_name, full_name, pid),
                            )
                            updated_existing += 1

                        cur.execute(
                            """
                            INSERT INTO nhl.player_external_ids (player_id, provider, provider_player_id)
                            VALUES (%s::bigint, 'nhl', %s::text)
                            ON CONFLICT (player_id, provider) DO NOTHING
                            """,
                            (pid, str(pid)),
                        )

                # Conservative guardrail:
                # only mark inactive when nearly complete team fetch succeeds.
                min_success = max(total_teams - 1, 1)
                if team_success >= min_success and fetched_active_ids:
                    print(
                        f"[inactive-mark] applying inactive status for players absent from {len(fetched_active_ids)} active ids ...",
                        flush=True,
                    )
                    cur.execute(
                        """
                        UPDATE nhl.players p
                           SET status = 'inactive',
                               active = FALSE,
                               updated_at = now()
                          FROM nhl.player_external_ids xid
                         WHERE xid.player_id = p.player_id
                           AND xid.provider = 'nhl'
                           AND xid.provider_player_id ~ '^[0-9]+$'
                           AND NOT (xid.provider_player_id::bigint = ANY(%s::bigint[]))
                        """,
                        (sorted(fetched_active_ids),),
                    )
                    print("[inactive-mark] done", flush=True)
                else:
                    print(
                        f"[warn] skipped inactive-mark step (team_success={team_success}/{total_teams}, "
                        f"active_ids={len(fetched_active_ids)})"
                    )

        print(
            f"PASS nhl full roster refresh date={DATE} teams_ok={team_success}/{total_teams} "
            f"team_fail={team_fail} active_ids={len(fetched_active_ids)} inserted={upserted_players} "
            f"updated={updated_existing}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
