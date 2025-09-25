# backend/app/routes/api/score_props.py

from __future__ import annotations
import requests
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Request
from scripts.shared.supabase_utils import supabase

router = APIRouter()

MLB = "https://statsapi.mlb.com/api/v1"

# ---------- MLB helpers ----------

def _schedule_finals(date_yyyy_mm_dd: str) -> List[int]:
    """Return gamePk list that are Final/Game Over for the date."""
    try:
        r = requests.get(f"{MLB}/schedule?sportId=1&date={date_yyyy_mm_dd}", timeout=10)
        r.raise_for_status()
        finals: List[int] = []
        for day in (r.json().get("dates") or []):
            for g in (day.get("games") or []):
                st = ((g.get("status") or {}).get("detailedState") or "").lower()
                if st in {"final", "game over"}:
                    try:
                        finals.append(int(g.get("gamePk")))
                    except Exception:
                        pass
        return finals
    except Exception:
        return []

def _fetch_boxscore(game_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(f"{MLB}/game/{int(game_id)}/boxscore", timeout=10)
        if not r.ok:
            return None
        return r.json()
    except Exception:
        return None

def _find_player_entry(box: Dict[str, Any], player_id: int) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Return (side, player_entry) where side is 'home' or 'away'."""
    pid_key = f"ID{int(player_id)}"
    teams = (box or {}).get("teams", {}) or {}
    for side in ("home", "away"):
        players = (teams.get(side, {}) or {}).get("players", {}) or {}
        if pid_key in players:
            return side, players[pid_key]
    return None, None

# Canonicalize prop_type names (handle typos/spacing/underscores)
_PROP_ALIASES = {
    "hitsrundrbis": "hits_runs_rbis",
    "hits_runs_rbis": "hits_runs_rbis",
    "runsrbis": "runs_rbis",
    "runs_rbis": "runs_rbis",
    "runs": "runs_scored",
    "runs_scored": "runs_scored",
    "earned runs": "earned_runs",
    "earned_runs": "earned_runs",
    "single": "singles",
    "singles": "singles",
}

def _canon_prop(pt: str) -> str:
    if not pt:
        return ""
    key = pt.strip().lower().replace("-", " ").replace("  ", " ").replace(" ", "_")
    return _PROP_ALIASES.get(key, key)

# ---------- extract actuals per prop ----------

def _bat(p: Dict[str, Any]) -> Dict[str, float]:
    s = ((p or {}).get("stats") or {}).get("batting") or {}
    return {
        "hits": float(s.get("hits") or 0),
        "home_runs": float(s.get("homeRuns") or 0),
        "rbis": float(s.get("rbi") or 0),
        "runs_scored": float(s.get("runs") or 0),
        "walks": float(s.get("baseOnBalls") or 0),
        "strikeouts_batting": float(s.get("strikeOuts") or 0),
        "doubles": float(s.get("doubles") or 0),
        "triples": float(s.get("triples") or 0),
        "stolen_bases": float(s.get("stolenBases") or 0),
        "total_bases": float(s.get("totalBases") or 0),
    }

def _pit(p: Dict[str, Any]) -> Dict[str, float]:
    s = ((p or {}).get("stats") or {}).get("pitching") or {}
    return {
        "strikeouts_pitching": float(s.get("strikeOuts") or 0),
        "walks_allowed": float(s.get("baseOnBalls") or 0),
        "hits_allowed": float(s.get("hits") or 0),
        "earned_runs": float(s.get("earnedRuns") or 0),
        "outs_recorded": float(s.get("outs") or 0),
    }

def _compute_actual(box: Dict[str, Any], player_id: int, prop_type: str) -> float:
    side, p = _find_player_entry(box, player_id)
    if not p:
        return 0.0

    b = _bat(p)
    t = _pit(p)

    if prop_type == "singles":
        return max(0.0, b["hits"] - b["doubles"] - b["triples"] - b["home_runs"])

    if prop_type == "hits_runs_rbis":
        return b["hits"] + b["runs_scored"] + b["rbis"]

    if prop_type == "runs_rbis":
        return b["runs_scored"] + b["rbis"]

    if prop_type == "total_bases":
        tb = b["total_bases"]
        if tb == 0.0 and b["hits"] > 0:
            tb = (b["hits"] - b["doubles"] - b["triples"] - b["home_runs"]) \
                 + 2*b["doubles"] + 3*b["triples"] + 4*b["home_runs"]
        return tb

    direct = {
        "hits": b["hits"],
        "home_runs": b["home_runs"],
        "rbis": b["rbis"],
        "runs_scored": b["runs_scored"],
        "walks": b["walks"],
        "strikeouts_batting": b["strikeouts_batting"],
        "doubles": b["doubles"],
        "triples": b["triples"],
        "stolen_bases": b["stolen_bases"],
        "strikeouts_pitching": t["strikeouts_pitching"],
        "walks_allowed": t["walks_allowed"],
        "hits_allowed": t["hits_allowed"],
        "earned_runs": t["earned_runs"],
        "outs_recorded": t["outs_recorded"],
    }
    return float(direct.get(prop_type, 0.0))

def _grade(over_under: str, line: float, actual: float) -> str:
    ou = (over_under or "").strip().lower()
    if ou not in {"over", "under"}:
        ou = "over"
    # sportsbook semantics: push on exact equality
    if abs(actual - line) < 1e-9:
        return "push"
    if ou == "over":
        return "win" if actual > line else "loss"
    else:
        return "win" if actual < line else "loss"

# ---------- DB helpers ----------

def _pending_props_for_games(game_ids: List[int]) -> List[Dict[str, Any]]:
    if not game_ids:
        return []
    try:
        res = (
            supabase.from_("player_props")
            .select("id, player_id, game_id, prop_type, prop_value, over_under")
            .in_("game_id", game_ids)
            .eq("status", "pending")
            .limit(5000)
            .execute()
        )
        return getattr(res, "data", []) or []
    except Exception:
        return []

def _safe_update_row(row_id: int, patch: Dict[str, Any]) -> None:
    # Try full patch first; if the table lacks a column, fall back to minimal
    try:
        supabase.from_("player_props").update(patch).eq("id", row_id).execute()
    except Exception:
        try:
            supabase.from_("player_props").update({"status": patch.get("status", "scored")}).eq("id", row_id).execute()
        except Exception:
            pass

# ---------- route ----------

@router.post("/score/day")
async def score_day(req: Request) -> Dict[str, Any]:
    """
    Grade all pending props for finished games on a given date.
    Body: { "game_date": "YYYY-MM-DD" }
    """
    body = await req.json()
    game_date = str(body.get("game_date") or "").strip()[:10]
    if not game_date:
        raise HTTPException(400, "game_date required (YYYY-MM-DD)")

    finals = _schedule_finals(game_date)
    if not finals:
        return {"ok": True, "graded": 0, "final_games": 0}

    rows = _pending_props_for_games(finals)
    if not rows:
        return {"ok": True, "graded": 0, "final_games": len(finals)}

    # cache boxscores per game
    box_by_gid: Dict[int, Dict[str, Any]] = {}
    graded = 0

    for r in rows:
        try:
            gid = int(r["game_id"])
            pid = int(r["player_id"])
        except (KeyError, TypeError, ValueError):
            continue  # skip bad row

        ptype_raw = str(r.get("prop_type", "")).strip()
        ptype = _canon_prop(ptype_raw)  # normalize (e.g., hitsrundrbis -> hits_runs_rbis)

        try:
            line = float(r.get("prop_value") or 0.5)
        except (TypeError, ValueError):
            line = 0.5

        ou = str(r.get("over_under") or "over").strip().lower()
        if ou not in ("over", "under"):
            ou = "over"

        # fetch/cache boxscore
        if gid not in box_by_gid:
            box = _fetch_boxscore(gid)
            if not box:
                box_by_gid[gid] = {}
                continue
            box_by_gid[gid] = box

        box = box_by_gid[gid]
        if not box:
            continue

        # optional: mark final games
        status = ((box.get("gameData") or {}).get("status") or {}).get("abstractGameState")
        if isinstance(status, str) and status.lower() == "final":
            finals.add(gid)

        actual = _compute_actual(box, pid, ptype)
        grade = _grade(ou, line, actual)

        patch = {
            "status": "scored",
            "actual_value": actual,
            "grade": grade,
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }
        _safe_update_row(int(r["id"]), patch)
        graded += 1

    return {"ok": True, "graded": graded, "final_games": len(finals)}