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
import os
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ET = ZoneInfo("America/New_York")
MLB_API = "https://statsapi.mlb.com/api/v1"


def _target_date(raw: Optional[str]) -> str:
    if raw:
        dt.date.fromisoformat(raw)
        return raw
    return dt.datetime.now(ET).date().isoformat()


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
) -> None:
    insert_cols: List[str] = ["player_id"]
    values: List[object] = [player_id]
    update_sets: List[str] = []

    if "player_name" in cols:
        insert_cols.append("player_name")
        values.append(player_name)
        update_sets.append("player_name = COALESCE(EXCLUDED.player_name, player_ids.player_name)")

    if "team" in cols:
        insert_cols.append("team")
        values.append(team_abbr)
        update_sets.append("team = COALESCE(EXCLUDED.team, player_ids.team)")

    if "team_id" in cols:
        insert_cols.append("team_id")
        values.append(team_id)
        update_sets.append("team_id = COALESCE(EXCLUDED.team_id, player_ids.team_id)")

    if "active" in cols:
        insert_cols.append("active")
        values.append(True)
        update_sets.append("active = TRUE")

    if "status" in cols:
        insert_cols.append("status")
        values.append("active")
        update_sets.append("status = 'active'")

    if "updated_at" in cols:
        insert_cols.append("updated_at")
        values.append(dt.datetime.now(dt.timezone.utc))
        update_sets.append("updated_at = now()")

    placeholders = ", ".join(["%s"] * len(insert_cols))
    sql = f"""
        INSERT INTO mlb.player_ids ({", ".join(insert_cols)})
        VALUES ({placeholders})
        ON CONFLICT (player_id)
        DO UPDATE SET {", ".join(update_sets) if update_sets else "player_id = EXCLUDED.player_id"}
    """
    cur.execute(sql, values)


def _mark_inactive(cur: psycopg.Cursor, cols: Set[str], active_ids: Iterable[int]) -> None:
    ids = sorted({int(x) for x in active_ids})
    if not ids:
        return
    updates: List[str] = []
    if "active" in cols:
        updates.append("active = FALSE")
    if "status" in cols:
        updates.append("status = 'inactive'")
    if "updated_at" in cols:
        updates.append("updated_at = now()")
    if not updates:
        return
    sql = f"""
        UPDATE mlb.player_ids
        SET {", ".join(updates)}
        WHERE NOT (player_id = ANY(%s::bigint[]))
    """
    cur.execute(sql, (ids,))


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
    active_ids: Set[int] = set()

    with psycopg.connect(db, prepare_threshold=0) as conn:
        try:
            conn.prepare_threshold = 0  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.transaction():
            with conn.cursor() as cur:
                cols = _table_columns(cur, "player_ids")
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
                        _upsert_player(
                            cur,
                            cols,
                            player_id=pid,
                            player_name=(row.get("player_name") or None),
                            team_abbr=team_abbr,
                            team_id=team_id,
                        )
                        upserts += 1

                min_success = max(total - 1, 1)
                if ok >= min_success and active_ids:
                    print(
                        f"[inactive-mark] applying inactive status for players absent from {len(active_ids)} active ids ...",
                        flush=True,
                    )
                    _mark_inactive(cur, cols, active_ids)
                    print("[inactive-mark] done", flush=True)
                else:
                    print(
                        f"[warn] skipped inactive-mark step (team_success={ok}/{total}, active_ids={len(active_ids)})"
                    )

    print(
        f"PASS mlb full roster refresh date={target_date} "
        f"teams_ok={ok}/{total} team_fail={fail} roster_rows={roster_rows} upserts={upserts}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
