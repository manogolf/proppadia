# backend/app/routes/api/prepare_prop.py

from __future__ import annotations
import requests
import os

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, field_validator, model_validator
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from app.services.game_info import ensure_game_info



# Supabase client (used only to ensure FK target in game_info)
from scripts.shared.supabase_utils import supabase

def _stash_features_for_training(
    *,
    prop_type: str,
    player_id: int,
    game_id: int,
    game_date: str,
    features: Dict[str, Any],
    feature_tag: str = "v1",
    lineup_slot: Optional[int] = None,
    is_prob_sp: Optional[bool] = None,
    model_tag: Optional[str] = None,
) -> bool:
    """
    Persist the prepared feature vector so training has a single source of truth,
    regardless of whether the row came from cron or ad-hoc prepare.
    Silent no-op if Supabase isn’t configured in this process.
    """
    if supabase is None:
        return False

    row = {
        "prop_type": str(prop_type),
        "player_id": int(player_id),
        "game_id": int(game_id),
        "game_date": str(game_date)[:10],
        "feature_set_tag": str(feature_tag or "v1"),
        "features": features,  # jsonb
        "lineup_slot": lineup_slot,
        "is_probable_sp": bool(is_prob_sp) if is_prob_sp is not None else None,
        "model_tag": model_tag,
    }
    row = {k: v for k, v in row.items() if v is not None}

    supabase.from_("prop_features_precomputed").upsert(
        row,
        on_conflict="prop_type,player_id,game_id,feature_set_tag",
    ).execute()
    return True


# Team + time helpers (stable)
from scripts.shared.team_name_map import (
    get_team_id_from_abbr,   # kept only as a resilience fallback
    get_team_info_by_id,
)

# If these utilities exist in your repo; fallback shims otherwise
try:
    from scripts.shared.time_utils_backend import (
        getDayOfWeekET,
        getTimeOfDayBucketET,
    )
except Exception:
    def getDayOfWeekET(date_or_iso: str) -> str:
        s = (date_or_iso or "").strip()
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            try:
                dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
            except Exception:
                return "Mon"
        dt_et = dt.astimezone(ZoneInfo("America/New_York"))
        return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt_et.weekday()]

    def getTimeOfDayBucketET(iso_et: Optional[str]) -> str:
        if not iso_et:
            return "evening"
        try:
            dt = datetime.fromisoformat(iso_et.replace("Z", "+00:00"))
            dt_et = dt.astimezone(ZoneInfo("America/New_York"))
            return "day" if dt_et.hour < 17 else "evening"
        except Exception:
            return "evening"


router = APIRouter()

# Pitching props (soft metadata only)
PITCHING_PROPS = {
    "strikeouts_pitching", "outs_recorded", "earned_runs",
    "hits_allowed", "walks_allowed"
}

class PrepareInput(BaseModel):
    # User-entered or resolved on the client
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    team_id: Optional[int] = None           # ← OPTIONAL with default None
    team_abbr: Optional[str] = None         # ← OPTIONAL with default None

    game_date: str                          # 'YYYY-MM-DD'
    prop_type: str
    prop_value: Optional[float] = None
    over_under: Optional[str] = None

    @field_validator("game_date")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        from datetime import datetime
        try:
            d = datetime.strptime(v[:10], "%Y-%m-%d")
            return d.strftime("%Y-%m-%d")
        except Exception:
            raise ValueError("game_date must be YYYY-MM-DD")

    @field_validator("over_under")
    @classmethod
    def _normalize_ou(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        s = v.strip().lower()
        if s not in {"over", "under"}:
            raise ValueError("over_under must be 'over' or 'under'")
        return s

    @field_validator("team_abbr")
    @classmethod
    def _norm_abbr(cls, v: Optional[str]) -> Optional[str]:
        from scripts.shared.team_name_map import normalize_team_abbreviation
        return normalize_team_abbreviation(v) if v else v

    @model_validator(mode="after")
    def _require_team_id_or_abbr(self):
        # Require at least one of (team_id, team_abbr)
        if self.team_id is None and not (self.team_abbr and self.team_abbr.strip()):
            raise ValueError("Provide team_id or team_abbr.")
        return self

def _fetch_schedule_one(date_yyyy_mm_dd: str) -> Dict[str, Any]:
    """Fetch MLB schedule for a single date via StatsAPI."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_yyyy_mm_dd}"
    r = requests.get(url, timeout=10)
    if not r.ok:
        raise HTTPException(502, f"MLB schedule fetch failed ({r.status_code})")
    return r.json()


def _pick_team_game(schedule_json: Dict[str, Any], team_id: int) -> Dict[str, Any]:
    """From a schedule day payload, return the game object involving team_id."""
    dates = schedule_json.get("dates") or []
    for day in dates:
        for g in day.get("games", []):
            home = g.get("teams", {}).get("home", {}).get("team", {}) or {}
            away = g.get("teams", {}).get("away", {}).get("team", {}) or {}
            if int(home.get("id", -1)) == int(team_id) or int(away.get("id", -1)) == int(team_id):
                return g
    raise HTTPException(
        404,
        f"No scheduled game for team_id {team_id} on {schedule_json.get('dates',[{'date':'?'}])[0].get('date','?')}."
    )


def _utc_to_et_iso(utc_iso: str) -> str:
    """Convert MLB 'gameDate' (UTC) to ET ISO string (no microseconds)."""
    dt_utc = datetime.fromisoformat(utc_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
    return dt_et.replace(microsecond=0).isoformat()


def _extract_game_summary(g: Dict[str, Any]) -> Dict[str, Any]:
    """Build a compact summary from schedule game object."""
    game_id = int(g.get("gamePk"))
    teams = g.get("teams", {})
    home_team = teams.get("home", {}).get("team", {}) or {}
    away_team = teams.get("away", {}).get("team", {}) or {}

    # Abbreviations (StatsAPI often includes; if not, fallback using ID map)
    home_abbr = home_team.get("abbreviation")
    away_abbr = away_team.get("abbreviation")
    if not home_abbr:
        info = get_team_info_by_id(int(home_team.get("id")))
        home_abbr = info["abbr"] if info else None
    if not away_abbr:
        info = get_team_info_by_id(int(away_team.get("id")))
        away_abbr = info["abbr"] if info else None

    # Probable starters if present (optional)
    home_prob = teams.get("home", {}).get("probablePitcher", {}) or {}
    away_prob = teams.get("away", {}).get("probablePitcher", {}) or {}
    sp_home_id = int(home_prob.get("id")) if home_prob.get("id") else None
    sp_away_id = int(away_prob.get("id")) if away_prob.get("id") else None

    # UTC → ET
    gameDate = g.get("gameDate")  # UTC ISO
    game_time_et = _utc_to_et_iso(gameDate) if gameDate else None
    game_date = gameDate[:10] if gameDate else None

    return {
        "game_id": game_id,
        "home_team_id": int(home_team.get("id")),
        "away_team_id": int(away_team.get("id")),
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "game_time_et": game_time_et,
        "game_date": game_date,
        "sp_home_id": sp_home_id,
        "sp_away_id": sp_away_id,
    }

@router.post("/prepareProp")
async def prepare_prop(req: Request) -> Dict[str, Any]:
    """
    v2 'prepare' endpoint: take minimal user input and assemble the features/context
    needed by /predict and later /props/add, without DB-first lookups.
    """
    payload = await req.json()
    inp = PrepareInput(**payload)

    # Authoritative: team_id (frontend sends this)
    team_id = int(inp.team_id) if inp.team_id is not None else None

    # Resilience only: accept abbr if someone hits the API without team_id
    if team_id is None and inp.team_abbr:
        tid = get_team_id_from_abbr(inp.team_abbr)
        if tid is None:
            raise HTTPException(400, f"Unknown team_abbr: {inp.team_abbr}")
        team_id = int(tid)

    if team_id is None:
        raise HTTPException(400, "team_id required")

    # Pull schedule for the date and pick this team's game
    sched = _fetch_schedule_one(inp.game_date)
    game = _pick_team_game(sched, int(team_id))
    g = _extract_game_summary(game)

    # Ensure FK target present for later insert (/props/add) — idempotent/minimal
    ensure_game_info(
        SimpleNamespace(
            game_id=int(g["game_id"]),
            game_time=g.get("game_time_et"),  # ET ISO if available
            # the service ignores missing/extra attrs safely
            game_date=g.get("game_date"),
            home_team_id=g.get("home_team_id"),
            away_team_id=g.get("away_team_id"),
            sp_home_id=g.get("sp_home_id"),
            sp_away_id=g.get("sp_away_id"),
        )
    )

    # Opponent + home/away
    is_home = (int(team_id) == int(g["home_team_id"]))
    opponent_team_id = int(g["away_team_id"] if is_home else g["home_team_id"])

    # Abbreviations for model features (string cols expected by batter models)
    team_abbr = g["home_abbr"] if is_home else g["away_abbr"]
    opponent_abbr = g["away_abbr"] if is_home else g["home_abbr"]

    # Time features (ET)
    iso_time = g["game_time_et"]
    game_day_of_week = getDayOfWeekET(iso_time[:10] if iso_time else inp.game_date)
    time_of_day_bucket = getTimeOfDayBucketET(iso_time) if iso_time else "evening"

    # Probable starters (soft metadata only)
    starting_pitcher_id = None
    if inp.prop_type in PITCHING_PROPS:
        starting_pitcher_id = g["sp_home_id"] if is_home else g["sp_away_id"]

    # Build features dict (lean; /predict fills missing with 0)
    features: Dict[str, Any] = {
        # IDs + core
        "player_id": int(inp.player_id),
        "team_id": int(team_id),
        "game_id": int(g["game_id"]),
        "game_date": inp.game_date,

        # model inputs (strings used by trained models)
        "team": team_abbr,
        "opponent": opponent_abbr,

        # context used elsewhere
        "opponent_encoded": opponent_team_id,
        "is_home": is_home,
        "game_time": iso_time,
        "game_day_of_week": game_day_of_week,
        "time_of_day_bucket": time_of_day_bucket,

        # user-entered prop details (canonical names)
        "prop_type": inp.prop_type,
        "line": inp.prop_value,          # legacy name still seen in some code
        "prop_value": inp.prop_value,    # used for inserts/dedupe
        "over_under": (inp.over_under or "over"),

    }

    if inp.prop_type in PITCHING_PROPS and starting_pitcher_id is not None:
        features["starting_pitcher_id"] = starting_pitcher_id

    if inp.player_name:
        features["player_name"] = inp.player_name  # optional echo for UI

    # Echo for UI convenience
    features["team_abbr"] = team_abbr
    features["opponent_team_id"] = opponent_team_id

        # Best-effort: stash features for training/consistency with cron output.
    try:
        _stash_features_for_training(
            prop_type=features["prop_type"],
            player_id=int(features["player_id"]),
            game_id=int(features["game_id"]),
            game_date=str(features["game_date"]),
            features=features,  # keep as-is; models ignore extras
            feature_tag=os.getenv("FEATURE_SET_TAG", "v1"),
            lineup_slot=None,  # no lineup dependency here
            is_prob_sp=(features.get("starting_pitcher_id") is not None) if inp.prop_type in PITCHING_PROPS else False,
            model_tag="poisson_v1",
        )
    except Exception:
        # never block prepare() on training stash
        pass

    return {"features": features}
