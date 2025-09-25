# backend/app/jobs/refresh_player_ids.py

from __future__ import annotations
from datetime import datetime
from typing import Dict, Any, List

import os
import time

try:
    import statsapi  # pip install MLB-StatsAPI
except Exception as e:
    statsapi = None

from backend.scripts.shared.supabase_utils import supabase
from backend.scripts.shared.team_name_map import team_id_map, normalize_team_abbreviation

def _season_today() -> int:
    # simple: use current year (adjust if you need spring training edge-cases)
    return datetime.utcnow().year

def fetch_active_rosters(season: int | None = None) -> List[Dict[str, Any]]:
    """
    Returns rows ready for upsert into public.player_ids:
      { player_id: str, player_name: str, team: 'NYY', team_id: int, updated_at: iso }
    """
    if statsapi is None:
        raise RuntimeError("MLB-StatsAPI is not installed (pip install MLB-StatsAPI).")

    if season is None:
        season = _season_today()

    rows: List[Dict[str, Any]] = []
    now_iso = datetime.utcnow().isoformat()

    # team_id_map is { 147: { abbr: 'NYY', fullName: 'New York Yankees' }, ... }
    for tid_str, info in team_id_map.items():
        tid = int(tid_str)
        abbr = info["abbr"]
        # StatsAPI roster call
        try:
            payload = statsapi.get("team_roster", {"teamId": tid, "season": season})
            roster = payload.get("roster") or []
        except Exception:
            # small backoff and retry once
            time.sleep(0.4)
            try:
                payload = statsapi.get("team_roster", {"teamId": tid, "season": season})
                roster = payload.get("roster") or []
            except Exception:
                roster = []

        for r in roster:
            person = (r or {}).get("person") or {}
            pid = person.get("id")
            name = person.get("fullName")
            if not pid or not name:
                continue

            rows.append(
                {
                    "player_id": str(pid),
                    "player_name": name,
                    "team": normalize_team_abbreviation(abbr) or abbr,
                    "team_id": tid,
                    "updated_at": now_iso,
                }
            )

    return rows

def upsert_player_ids(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Upsert by player_id (unique). Keep only current team/name per player.
    """
    inserted = 0
    batch: List[Dict[str, Any]] = []

    # PostgREST can upsert big batches, but keep it moderate
    def flush():
        nonlocal inserted, batch
        if not batch:
            return
        res = supabase.from_("player_ids").upsert(
            batch,
            on_conflict="player_id",  # unique(player_id)
        ).execute()
        if getattr(res, "error", None):
            raise RuntimeError(f"Supabase upsert error: {res.error}")
        inserted += len(batch)
        batch = []

    for row in rows:
        batch.append(row)
        if len(batch) >= 500:
            flush()
    flush()

    return {"upserted": inserted}

def refresh_player_ids() -> Dict[str, int]:
    rows = fetch_active_rosters()
    return upsert_player_ids(rows)

if __name__ == "__main__":
    out = refresh_player_ids()
    print(out)
