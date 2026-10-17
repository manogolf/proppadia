#!/usr/bin/env python3
"""
refresh_mlb_players_rosters.py

Full-league MLB roster refresh:
- fetch all MLB teams
- fetch active roster for each team (for a target date)
- upsert into mlb.player_ids
- if supported by schema, mark active/inactive status

Safe-by-default behavior:
- schema-aware writes: only updates columns that exist in player_ids
- inactive-mark step runs only when most team fetches succeed
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.shared.db.pg import (
    configure_mlb_ingest_connection,
    mlb_ingest_restricted_mode_enabled,
    verify_mlb_ingest_production_target,
)


ET = ZoneInfo("America/New_York")
MLB_API = "https://statsapi.mlb.com/api/v1"


def _target_date(raw: Optional[str]) -> str:
    if raw:
        dt.date.fromisoformat(raw)
        return raw
    return dt.datetime.now(ET).date().isoformat()


def _db_url() -> str:
    if mlb_ingest_restricted_mode_enabled():
        db = os.getenv("MLB_INGEST_DATABASE_URL", "").strip()
    else:
        db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URL / DATABASE_URL")
    if "?sslmode=" not in db and "&sslmode=" not in db:
        db += ("&" if "?" in db else "?") + "sslmode=require"
    if "?gssencmode=" not in db and "&gssencmode=" not in db:
        db += ("&" if "?" in db else "?") + "gssencmode=disable"
    return db


def _connect_db(db: str):
    restricted_ingest = mlb_ingest_restricted_mode_enabled()
    if restricted_ingest:
        verify_mlb_ingest_production_target(db)
    conn = psycopg.connect(db, prepare_threshold=0)
    if restricted_ingest:
        try:
            configure_mlb_ingest_connection(conn)
        except Exception:
            conn.close()
            raise
    return conn


def _session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=2,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia/mlb-full-roster-refresh"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


def _fetch_teams(s: requests.Session) -> List[Tuple[int, str]]:
    url = f"{MLB_API}/teams?sportId=1"
    resp = s.get(url, timeout=20)
    resp.raise_for_status()
    js = resp.json() or {}
    out: List[Tuple[int, str]] = []
    for row in js.get("teams", []) or []:
        tid = row.get("id")
        abbr = row.get("abbreviation")
        if tid is None or not abbr:
            continue
        try:
            out.append((int(tid), str(abbr).strip().upper()))
        except Exception:
            continue
    out.sort(key=lambda x: x[1])
    return out


def _fetch_team_roster(s: requests.Session, team_id: int, date_iso: str) -> List[Dict[str, object]]:
    url = f"{MLB_API}/teams/{team_id}/roster?rosterType=active&date={date_iso}"
    resp = s.get(url, timeout=20)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    js = resp.json() or {}
    out: List[Dict[str, object]] = []
    for row in js.get("roster", []) or []:
        person = row.get("person") or {}
        pid = person.get("id")
        name = person.get("fullName")
        if pid is None:
            continue
        try:
            pid_i = int(pid)
        except Exception:
            continue
        out.append(
            {
                "player_id": pid_i,
                "player_name": str(name).strip() if name else None,
                "position": ((row.get("position") or {}).get("abbreviation") or None),
            }
        )
    return out


def _table_columns(cur: psycopg.Cursor, table_name: str = "player_ids") -> Set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'mlb' AND table_name = %s
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


def _upsert_player(
    cur: psycopg.Cursor,
    cols: Set[str],
    *,
    player_id: int,
    player_name: Optional[str],
    team_abbr: str,
    team_id: int,
) -> bool:
    payload: Dict[str, object] = {"player_id": player_id}
    # The fixed DB entrypoint filters optional columns against the live table
    # shape, so the restricted login does not need direct player_ids ACLs.
    payload.update({"player_name": player_name, "team": team_abbr, "team_id": team_id})
    statement = "SELECT mlb.safe_upsert_player_roster(%s::jsonb)"

    # player_ids has a legacy UNIQUE(player_name, team) index in addition to
    # its canonical player_id key. Distinct official MLB identities can share
    # a name and organization, so retry only that collision without the team
    # display label. Keep the official player_id and team_id in the payload.
    cur.execute("SAVEPOINT mlb_roster_player_upsert")
    try:
        cur.execute(statement, (json.dumps(payload, separators=(",", ":")),))
    except psycopg.errors.UniqueViolation as exc:
        if getattr(getattr(exc, "diag", None), "constraint_name", None) != "unique_player_team":
            raise
        cur.execute("ROLLBACK TO SAVEPOINT mlb_roster_player_upsert")
        payload.pop("team", None)
        cur.execute(statement, (json.dumps(payload, separators=(",", ":")),))
        cur.execute("RELEASE SAVEPOINT mlb_roster_player_upsert")
        return True
    cur.execute("RELEASE SAVEPOINT mlb_roster_player_upsert")
    return False


def _mark_inactive(
    cur: psycopg.Cursor,
    cols: Set[str],
    active_ids: Iterable[int],
    *,
    successful_teams: int,
    total_teams: int,
) -> int:
    ids = sorted({int(x) for x in active_ids})
    if not ids:
        return 0
    cur.execute(
        "SELECT mlb.safe_mark_player_roster_inactive(%s::bigint[],%s,%s)",
        (ids, successful_teams, total_teams),
    )
    result = cur.fetchone()
    return int((result or (0,))[0] or 0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh full MLB active rosters into mlb.player_ids")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD ET context date (default: today ET)")
    args = parser.parse_args()

    target_date = _target_date(args.date)
    db = _db_url()
    s = _session()

    try:
        teams = _fetch_teams(s)
    except Exception as e:
        print(f"FAIL fetch teams: {type(e).__name__}: {e}")
        return 1

    if not teams:
        print("FAIL fetch teams: returned 0 teams")
        return 1

    total = len(teams)
    ok = 0
    fail = 0
    roster_rows = 0
    upserts = 0
    team_label_conflicts = 0
    active_ids: Set[int] = set()

    with _connect_db(db) as conn:
        try:
            conn.prepare_threshold = 0  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.transaction():
            with conn.cursor() as cur:
                # The fixed database entrypoint performs schema-aware writes;
                # catalog visibility is not required from this restricted role.
                cols: Set[str] = set()
                for idx, (team_id, team_abbr) in enumerate(teams, start=1):
                    print(f"[{idx}/{total}] fetching {team_abbr} roster ...", flush=True)
                    try:
                        rows = _fetch_team_roster(s, team_id, target_date)
                    except Exception as e:
                        fail += 1
                        print(f"[warn] {team_abbr} fetch failed: {type(e).__name__}: {e}")
                        continue
                    ok += 1
                    print(f"[{idx}/{total}] {team_abbr}: roster rows={len(rows)}", flush=True)
                    roster_rows += len(rows)
                    for row in rows:
                        pid = int(row["player_id"])
                        active_ids.add(pid)
                        team_label_conflicts += int(_upsert_player(
                            cur,
                            cols,
                            player_id=pid,
                            player_name=(row.get("player_name") or None),
                            team_abbr=team_abbr,
                            team_id=team_id,
                        ))
                        upserts += 1

                min_success = max(total - 1, 1)
                if ok >= min_success and active_ids:
                    changed = _mark_inactive(
                        cur, cols, active_ids,
                        successful_teams=ok, total_teams=total,
                    )
                    print(f"[inactive-mark] marker rows changed={changed}", flush=True)
                    print("[inactive-mark] done", flush=True)
                else:
                    print(
                        f"[warn] skipped inactive-mark step (team_success={ok}/{total}, active_ids={len(active_ids)})"
                    )

    print(
        f"PASS mlb full roster refresh date={target_date} "
        f"teams_ok={ok}/{total} team_fail={fail} roster_rows={roster_rows} upserts={upserts} "
        f"team_label_conflicts={team_label_conflicts}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
