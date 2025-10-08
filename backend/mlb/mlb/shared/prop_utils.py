# backend/scripts/shared/prop_utils.py

from __future__ import annotations
import requests
import json
from typing import Optional, Tuple
from urllib.request import urlopen
from datetime import datetime
from dateutil import parser
from zoneinfo import ZoneInfo
from .team_name_map import team_id_map  # canonical map lives in shared/
from scripts.shared.supabase_utils import supabase
from scripts.shared.team_name_map import (
    normalizeTeamAbbreviation as norm_abbr,  # e.g., "AZ" -> "ARI"
    getTeamIdFromAbbr,                       # abbr -> team_id
)

ET = ZoneInfo("America/New_York")

# -------------------------------------------------------------------
# Model map + helpers (unchanged behavior)
# -------------------------------------------------------------------

PROP_MODEL_MAP = {
    "hits": "hits",
    "home_runs": "home_runs",
    "rbis": "rbis",
    "strikeouts_pitching": "strikeouts_pitching",
    "strikeouts_batting": "strikeouts_batting",
    "runs_scored": "runs_scored",
    "walks": "walks",
    "doubles": "doubles",
    "triples": "triples",
    "outs_recorded": "outs_recorded",
    "earned_runs": "earned_runs",
    "hits_allowed": "hits_allowed",
    "walks_allowed": "walks_allowed",
    "stolen_bases": "stolen_bases",
    "total_bases": "total_bases",
    "hits_runs_rbis": "hits_runs_rbis",
    "runs_rbis": "runs_rbis",
    "singles": "singles",
}

def normalize_prop_type(prop_type: str) -> str:
    return (
        prop_type.lower()
        .replace("(", "")
        .replace(")", "")
        .replace(" + ", "_")
        .replace(" ", "_")
        .strip("_")
    )

def get_canonical_model_name(prop_type: str) -> Optional[str]:
    key = normalize_prop_type(prop_type)
    return PROP_MODEL_MAP.get(key)

# -------------------------------------------------------------------
# DB + mapping helpers (ID-first; matches public.player_ids schema)
# -------------------------------------------------------------------


def get_player_id_by_name(name: str) -> Optional[int]:
    """
    Resolve MLB player_id using your Supabase table first:
      table: public.player_ids
      columns: player_name (text), player_id (text)
    Fallback to MLB StatsAPI if DB has no match.
    """
    if not name:
        return None
    q = name.strip()

    # 1) Exact match
    try:
        res = (
            supabase.from_("player_ids")
            .select("player_id, player_name")
            .eq("player_name", q)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", []) or []
        if rows:
            return int(rows[0]["player_id"])
    except Exception:
        pass

    # 2) Case-insensitive contains
    try:
        res = (
            supabase.from_("player_ids")
            .select("player_id, player_name")
            .ilike("player_name", f"%{q}%")
            .limit(5)
            .execute()
        )
        rows = getattr(res, "data", []) or []
        if rows:
            # Prefer case-insensitive exact among results
            for r in rows:
                if str(r.get("player_name", "")).lower() == q.lower():
                    return int(r["player_id"])
            return int(rows[0]["player_id"])
    except Exception:
        pass

    # 3) MLB StatsAPI fallback
    if statsapi:
        try:
            candidates = statsapi.lookup_player(q) or []
            exact = next(
                (c for c in candidates if str(c.get("fullName", "")).lower() == q.lower()),
                None,
            )
            if exact and "id" in exact:
                return int(exact["id"])
            active = next((c for c in candidates if c.get("active") and "id" in c), None)
            if active:
                return int(active["id"])
            if candidates and "id" in candidates[0]:
                return int(candidates[0]["id"])
        except Exception:
            pass

    return None


def get_latest_team_for_player(player_id: int) -> Tuple[Optional[str], Optional[int]]:
    """
    From public.player_ids, return (team_abbr, team_id) for the given player_id,
    preferring the most recently updated row.
    Columns: player_id (text), team (abbr), team_id (bigint), updated_at/created_at.
    """
    try:
        res = (
            supabase.from_("player_ids")
            .select("team, team_id, updated_at, created_at")
            .eq("player_id", str(player_id))  # stored as TEXT
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", []) or []
        if not rows:
            return None, None

        team_abbr = norm_abbr(rows[0].get("team"))
        tid = rows[0].get("team_id")
        if tid is None and team_abbr:
            tid = getTeamIdFromAbbr(team_abbr)
        return team_abbr, (int(tid) if tid is not None else None)
    except Exception:
        return None, None


def get_team_abbr_from_team_id(team_id: int) -> str | None:
    """UI-only convenience; do not persist abbr in storage."""
    info = team_id_map.get(int(team_id)) or team_id_map.get(str(team_id))
    return (info or {}).get("abbr")

def find_game_id_by_team_id_and_date(team_id: int, game_date: str) -> int | None:
    """
    Primary resolver (today/future only): return gamePk for team_id on game_date.

    Doubleheader rule:
      • Prefer the next upcoming start (ET) relative to now
      • If multiple upcoming, choose the earliest
      • If all are in the future (e.g., both), choose earliest
    """
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={game_date}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    games = (data.get("dates") or [{}])[0].get("games") or []
    candidates = []
    for g in games:
        teams = g.get("teams") or {}
        home = ((teams.get("home") or {}).get("team") or {}).get("id")
        away = ((teams.get("away") or {}).get("team") or {}).get("id")
        if home == team_id or away == team_id:
            start_utc = parser.isoparse(g.get("gameDate"))
            start_et = start_utc.astimezone(ET)
            candidates.append((int(g.get("gamePk")), start_et))

    if not candidates:
        return None

    now_et = datetime.now(ET)
    future = [(gid, dt) for gid, dt in candidates if dt > now_et]
    pick = min(future, key=lambda x: x[1])[0] if future else min(candidates, key=lambda x: x[1])[0]
    return int(pick)
# --- end add ---