# backend/app/routes/api/players.py
from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from urllib.parse import quote_plus
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, HTTPException, Query

from scripts.shared.supabase_utils import supabase
from scripts.shared.team_name_map import get_team_id_from_abbr

# Prefer the real helper if present; otherwise, safe stub.
try:
    from scripts.shared.prop_utils import get_latest_team_for_player
except Exception:  # pragma: no cover
    def get_latest_team_for_player(pid: int):
        return None, None

router = APIRouter()

MLB_BASE = "https://statsapi.mlb.com/api/v1"


# -----------------------------
# Helpers
# -----------------------------
def _norm_name(s: str) -> str:
    """Lowercase, strip accents, drop punctuation/suffixes, collapse spaces."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = s.replace("'", "").replace(".", "")
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    s = re.sub(r"[^a-z ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _best_match(target_norm: str, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pick the DB row whose normalized player_name best matches target."""
    best = None
    best_score = 0.0
    for r in rows:
        name = (r.get("player_name") or "").strip()
        cand_norm = _norm_name(name)
        if not cand_norm:
            continue
        score = SequenceMatcher(None, target_norm, cand_norm).ratio()
        if score > best_score:
            best_score = score
            best = r
    # Require a reasonable similarity
    return best if best_score >= 0.78 else None


def _mlb_search_people_by_name(name: str) -> List[Dict[str, Any]]:
    """Best-effort MLB name search; returns list of people dicts."""
    q = quote_plus(name.strip())
    # Primary: /people?search=
    try:
        r = requests.get(f"{MLB_BASE}/people?search={q}&sportId=1", timeout=8)
        if r.ok:
            js = r.json() or {}
            ppl = js.get("people") or []
            if isinstance(ppl, list) and ppl:
                return ppl
    except Exception:
        pass
    # Fallback: /people/search?name=
    try:
        r2 = requests.get(f"{MLB_BASE}/people/search?name={q}&sportId=1", timeout=8)
        if r2.ok:
            js2 = r2.json() or {}
            ppl = js2.get("people") or js2.get("searchResults") or []
            if isinstance(ppl, list):
                return ppl
    except Exception:
        pass
    return []


def _mlb_current_team_id(player_id: int) -> Optional[int]:
    """Hydrate current team for a player_id via MLB API."""
    try:
        r = requests.get(f"{MLB_BASE}/people/{int(player_id)}?hydrate=currentTeam", timeout=8)
        if not r.ok:
            return None
        js = r.json() or {}
        people = js.get("people") or []
        if not people:
            return None
        cur = (people[0].get("currentTeam") or {})
        tid = cur.get("id")
        return int(tid) if tid is not None else None
    except Exception:
        return None


# -----------------------------
# Route: Resolve by NAME → {player_id, team_id}
# -----------------------------
@router.get("/players/resolve")
def resolve_player(
    name: str = Query(..., min_length=2),
    date: Optional[str] = None,  # accepted for compatibility; not used
):
    """
    Resolve a player by NAME (case/diacritic tolerant).
    1) Try public.player_ids (DB-first)
    2) Fallback to MLB people search + currentTeam
    Returns: { player_id, name, team_id }.
    """
    raw = (name or "").strip()
    norm = _norm_name(raw)
    if not norm:
        raise HTTPException(status_code=400, detail="empty name")

    # Numeric fast-path: treat as player_id and fetch team
    if raw.isdigit():
        pid = int(raw)
        team_id = None
        try:
            _abbr, tid = get_latest_team_for_player(pid)
            if tid:
                team_id = int(tid)
        except Exception:
            team_id = None
        if team_id is None:
            raise HTTPException(status_code=404, detail="Team not found for player")
        return {"player_id": pid, "name": raw, "team_id": team_id}

    # 1) Broad ILIKE on raw (fast path)
    rows: List[Dict[str, Any]] = []
    try:
        res = (
            supabase.from_("player_ids")
            .select("player_id, player_name, team, team_id, updated_at")
            .ilike("player_name", f"%{raw}%")
            .order("updated_at", desc=True)
            .limit(50)
            .execute()
        )
        rows = getattr(res, "data", []) or []
    except Exception:
        rows = []

    # 2) If empty, broaden search in accent-friendly way via tokens
    if not rows:
        tokens = [t for t in norm.split(" ") if t]
        # First token
        if tokens:
            try:
                res2 = (
                    supabase.from_("player_ids")
                    .select("player_id, player_name, team, team_id, updated_at")
                    .ilike("player_name", f"%{tokens[0]}%")
                    .order("updated_at", desc=True)
                    .limit(100)
                    .execute()
                )
                rows = getattr(res2, "data", []) or []
            except Exception:
                rows = []
        # Last token
        if not rows and len(tokens) > 1:
            try:
                res3 = (
                    supabase.from_("player_ids")
                    .select("player_id, player_name, team, team_id, updated_at")
                    .ilike("player_name", f"%{tokens[-1]}%")
                    .order("updated_at", desc=True)
                    .limit(100)
                    .execute()
                )
                rows = getattr(res3, "data", []) or []
            except Exception:
                rows = []

    # 3) Pick best DB candidate by normalized fuzzy ratio
    cand = _best_match(norm, rows) if rows else None
    if not cand:
        # ---- MLB FALLBACK: not in DB; try StatsAPI ----
        ppl = _mlb_search_people_by_name(raw)
        if not ppl:
            raise HTTPException(status_code=404, detail="Player not found")
        person = ppl[0]
        try:
            pid = int(person.get("id"))
        except Exception:
            raise HTTPException(status_code=404, detail="Player not found")
        team_id = _mlb_current_team_id(pid)
        if team_id is None:
            raise HTTPException(status_code=404, detail="Team not found for player")
        return {
            "player_id": pid,
            "name": person.get("fullName") or raw,
            "team_id": int(team_id),
        }

    # ----- DB candidate path -----
    pid = int(cand["player_id"])

    # Prefer a definitive TEAM ID; fall back through several sources
    team_id: Optional[int] = None
    try:
        _abbr, latest_tid = get_latest_team_for_player(pid)
        if latest_tid:
            team_id = int(latest_tid)
    except Exception:
        team_id = None

    if team_id is None and cand.get("team_id") is not None:
        try:
            team_id = int(cand["team_id"])
        except Exception:
            team_id = None

    if team_id is None and cand.get("team"):
        try:
            mapped_tid = get_team_id_from_abbr(str(cand["team"]).strip())
            if mapped_tid is not None:
                team_id = int(mapped_tid)
        except Exception:
            team_id = None

    if team_id is None:
        raise HTTPException(status_code=404, detail="Team not found for player")

    return {
        "player_id": pid,
        "name": cand.get("player_name") or raw,
        "team_id": team_id,
    }
