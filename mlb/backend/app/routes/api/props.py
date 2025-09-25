# backend/app/routes/api/props.py

from __future__ import annotations

import requests
import os
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from app.services.game_info import ensure_game_info
from zoneinfo import ZoneInfo
from scripts.shared.team_name_map import get_team_info_by_id
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Request
from scripts.shared.supabase_utils import supabase
from app.config import COMMIT_TOKEN_SECRET, COMMIT_TOKEN_TTL
from app.security.commit_token import verify_commit_token
from scripts.shared.prop_utils import (
    get_team_abbr_from_team_id,
    get_latest_team_for_player,
)

try:
    from postgrest.exceptions import APIError as PostgrestAPIError  # new
except Exception:  # pragma: no cover
    try:
        from postgrest import APIError as PostgrestAPIError          # old
    except Exception:
        PostgrestAPIError = Exception

log = logging.getLogger(__name__)

router = APIRouter()

TABLE = "player_props"


# --- helper: resolve name by player_id from player_ids table ---
def _get_player_name_by_id(pid: int | str) -> Optional[str]:
    try:
        res = (
            supabase.from_("player_ids")
            .select("player_name")
            .eq("player_id", str(pid))
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", []) or []
        if rows:
            return rows[0].get("player_name")
    except Exception:
        pass
    return None

# --- duplicate check: mirror DB UNIQUE(prop_source, player_id, game_id, prop_type, prop_value) ---
def _dup_exists(
    *,
    prop_source: str,
    player_id: int,
    game_id: int,
    prop_type: str,
    prop_value: float,
) -> bool:
    """Return True if a matching row already exists in player_props."""
    try:
        key: Dict[str, Any] = {
            "prop_source": prop_source,
            "player_id": int(player_id),
            "game_id": int(game_id),
            "prop_type": str(prop_type),
            "prop_value": float(prop_value),
        }
        res = (
            supabase.from_(TABLE)
            .select("id")
            .match(key)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", []) or []
        return bool(rows)
    except Exception:
        return False


def _ensure_game_info_fk(*, game_id: int, features: Dict[str, Any]) -> None:
    """
    Ensure a game_info row exists for this game_id so the FK on player_props can pass.

    Strategy:
      A) Try upsert using what we already have in `features`
      B) If still missing, hit MLB schedule for game_date to enrich
      C) Final fallback: hit the direct game feed by game_id

    All steps are best-effort; errors are swallowed.
    """
    if supabase is None:
        return

    def _exists(gid: int) -> bool:
        try:
            ex = (
                supabase.from_("game_info")
                .select("game_id")
                .eq("game_id", gid)
                .limit(1)
                .execute()
            )
            return bool((getattr(ex, "data", None) or []))
        except Exception:
            return False

    def _utc_to_et_iso(utc_iso: Optional[str]) -> Optional[str]:
        if not utc_iso:
            return None
        try:
            dt_utc = datetime.fromisoformat(utc_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
            return dt_et.replace(microsecond=0).isoformat()
        except Exception:
            return None

    try:
        gid = int(game_id)
    except Exception:
        return

    # If it already exists, done.
    if _exists(gid):
        return

    # ---------- A) Use what we already have in `features`
    try:
        game_date = (str(features.get("game_date") or "")[:10]) or None
        team_id = features.get("team_id")
        opp_id  = features.get("opponent_team_id") or features.get("opponent_encoded")
        is_home = features.get("is_home")

        home_team_id = away_team_id = None
        if isinstance(is_home, (bool, int)):
            if bool(is_home):
                home_team_id = int(team_id) if team_id is not None else None
                away_team_id = int(opp_id)  if opp_id  is not None else None
            else:
                home_team_id = int(opp_id)  if opp_id  is not None else None
                away_team_id = int(team_id) if team_id is not None else None

        payload = {
            "game_id": gid,
            "game_date": game_date,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_team_abbr": features.get("home_team_abbr"),
            "away_team_abbr": features.get("away_team_abbr"),
            "game_time": features.get("game_time"),
            "starting_pitcher_id_home": features.get("starting_pitcher_id_home"),
            "starting_pitcher_id_away": features.get("starting_pitcher_id_away"),
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        # Upsert if we have at least game_id + something useful
        if len(payload) > 1:
            supabase.from_("game_info").upsert(payload, on_conflict="game_id").execute()
            if _exists(gid):
                return
    except Exception:
        pass

    # ---------- B) Enrich via MLB schedule for game_date (if we have a date)
    try:
        game_date = (str(features.get("game_date") or "")[:10]) or None
        if game_date:
            r = requests.get(
                f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={game_date}",
                timeout=10,
            )
            if r.ok:
                js = r.json()
                for day in js.get("dates", []):
                    for g in day.get("games", []):
                        if int(g.get("gamePk", -1)) == gid:
                            teams = g.get("teams", {}) or {}
                            home = (teams.get("home") or {}).get("team") or {}
                            away = (teams.get("away") or {}).get("team") or {}

                            home_id = int(home.get("id") or 0) or None
                            away_id = int(away.get("id") or 0) or None
                            home_abbr = home.get("abbreviation")
                            away_abbr = away.get("abbreviation")

                            if not home_abbr and home_id:
                                info = get_team_info_by_id(home_id) or {}
                                home_abbr = info.get("abbr")
                            if not away_abbr and away_id:
                                info = get_team_info_by_id(away_id) or {}
                                away_abbr = info.get("abbr")

                            payload = {
                                "game_id": gid,
                                "game_date": game_date,
                                "home_team_id": home_id,
                                "away_team_id": away_id,
                                "home_team_abbr": home_abbr,
                                "away_team_abbr": away_abbr,
                                "game_time": _utc_to_et_iso(g.get("gameDate")),
                                "starting_pitcher_id_home": ((teams.get("home") or {}).get("probablePitcher") or {}).get("id") or None,
                                "starting_pitcher_id_away": ((teams.get("away") or {}).get("probablePitcher") or {}).get("id") or None,
                            }
                            payload = {k: v for k, v in payload.items() if v is not None}
                            supabase.from_("game_info").upsert(payload, on_conflict="game_id").execute()
                            if _exists(gid):
                                return
    except Exception:
        pass

    # ---------- C) Final fallback: direct game feed by game_id
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/game/{gid}/feed/live", timeout=10)
        if not r.ok:
            return
        js = r.json()
        gd = js.get("gameData", {}) or {}
        teams = gd.get("teams", {}) or {}
        home_id = int((teams.get("home") or {}).get("id") or 0) or None
        away_id = int((teams.get("away") or {}).get("id") or 0) or None

        home_info = get_team_info_by_id(home_id) or {}
        away_info = get_team_info_by_id(away_id) or {}

        dt = (gd.get("datetime", {}) or {}).get("dateTime")
        official = (gd.get("datetime", {}) or {}).get("officialDate")
        game_time_iso = _utc_to_et_iso(dt)
        game_date_str = (str(features.get("game_date") or "")[:10]) or official or ((game_time_iso or "")[:10] or None)

        payload = {
            "game_id": gid,
            "game_date": game_date_str,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_team_abbr": home_info.get("abbr"),
            "away_team_abbr": away_info.get("abbr"),
            "game_time": game_time_iso,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        supabase.from_("game_info").upsert(payload, on_conflict="game_id").execute()
    except Exception:
        return


@router.post("/props/add")
async def add_prop(req: Request):
    """
    Save a user-added prop using a commit_token minted by /api/predict.
    - Enforces DB unique key exactly: (prop_source, player_id, game_id, prop_type, prop_value)
    - Requires game_id; no fallback to date
    - Keeps prop_value as-is (UI provides 0.5, 1.5, 2.5 …)
    """
    body = await req.json()
    token = body.get("commit_token")
    if not token:
        raise HTTPException(status_code=400, detail="commit_token required")

    # Verify token + unpack payload
    try:
        data = verify_commit_token(
            token,
            ttl_seconds=COMMIT_TOKEN_TTL,
            secret=COMMIT_TOKEN_SECRET,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid commit_token: {e}")

    # /api/predict puts the prepared fields under "features"
    features = data.get("features", data) if isinstance(data, dict) else {}
    if not isinstance(features, dict):
        raise HTTPException(status_code=400, detail="Malformed token payload")

    # Normalize naming: accept "line" as alias for prop_value
    if "prop_value" not in features and "line" in features:
        features["prop_value"] = features["line"]

    # Required: note game_id is mandatory per schema
    required = ("player_id", "team_id", "game_id", "game_date", "prop_type", "prop_value")
    missing = [k for k in required if k not in features or features.get(k) in (None, "")]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing)}")

    # Bind & type
    try:
        player_id: int = int(features["player_id"])
        team_id: int = int(features["team_id"])
        game_id: int = int(features["game_id"])   # MUST exist
        game_date: str = str(features["game_date"])
        prop_type: str = str(features["prop_type"])
        prop_value_num: float = float(features["prop_value"])
        over_under: str = str(features.get("over_under") or "over")
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Bad or missing field types in required fields")

    # Optional: probability carried in token or features
    prob = data.get("prob") or features.get("probability")

    # Source scope (defaults to user_added)
    prop_source: str = (
        body.get("prop_source")
        or data.get("prop_source")
        or features.get("prop_source")
        or "user_added"
    )

    # Resolve team (abbr) if missing → prefer explicit, then by team_id, then latest by player_id
    team_abbr = None
    if isinstance(features.get("team"), str) and features["team"].strip():
        team_abbr = features["team"].strip().upper()
    if not team_abbr:
        try:
            team_abbr = get_team_abbr_from_team_id(team_id)
        except Exception:
            team_abbr = None
    if not team_abbr and player_id:
        try:
            team_abbr, _ = get_latest_team_for_player(player_id)
        except Exception:
            team_abbr = None
    if not team_abbr:
        raise HTTPException(status_code=400, detail="Could not determine team (abbr) to insert")

        # Ensure player_name (NOT NULL)
    player_name = features.get("player_name")
    if not player_name and player_id:
        player_name = _get_player_name_by_id(player_id)
    if not player_name and player_id:
        # last-ditch: pull from MLB API so users aren't blocked
        try:
            r = requests.get(f"https://statsapi.mlb.com/api/v1/people/{int(player_id)}", timeout=5)
            if r.ok:
                js = r.json()
                people = js.get("people") or []
                if people:
                    player_name = (people[0] or {}).get("fullName") or None
        except Exception:
            pass
    if not player_name:
        raise HTTPException(status_code=400, detail="Could not resolve player_name")

    # Optional context
    is_home = features.get("is_home")
    home_away = "home" if isinstance(is_home, (bool, int)) and bool(is_home) else ("away" if isinstance(is_home, (bool, int)) else None)

    # Duplicate check (must exactly match DB unique tuple)
    # Duplicate check (calls helper defined above)
    if _dup_exists(
        prop_source=prop_source,
        player_id=player_id,
        game_id=game_id,
        prop_type=prop_type,
        prop_value=prop_value_num,
    ):
        return {"saved": False, "duplicate": True}

    # Build row to match public.player_props
    row: Dict[str, Any] = {
        "game_date": game_date[:10],      # YYYY-MM-DD
        "player_name": player_name,
        "team": team_abbr,                # text abbr
        "prop_type": prop_type,
        "prop_value": float(prop_value_num),  # keep exact UI value
        "over_under": over_under,             # not part of unique tuple
        "status": "pending",
        "game_id": int(game_id),
        "player_id": int(player_id),          # BIGINT
        "team_id": int(team_id),              # BIGINT
        "prop_source": prop_source,           # uniqueness scope

        # Optional context (pass through when present)
        "confidence_score": float(prob) if prob is not None else None,
        "predicted_outcome": data.get("predicted_outcome"),
        "opponent_encoded": features.get("opponent_encoded"),
        "is_home": bool(is_home) if is_home is not None else None,
        "home_away": home_away,
        "opponent_team_id": features.get("opponent_team_id"),
        "game_day_of_week": str(features["game_day_of_week"]) if "game_day_of_week" in features and features["game_day_of_week"] is not None else None,
        "time_of_day_bucket": features.get("time_of_day_bucket"),
        "opponent": features.get("opponent"),
        "game_time": features.get("game_time"),
        "starting_pitcher_id": features.get("starting_pitcher_id"),
    }

    # Drop None values to avoid NOT NULL/column issues
    row_clean = {k: v for k, v in row.items() if v is not None}

    # Insert (upsert against the DB unique columns)
# Preflight: ensure game_info exists so the FK passes (works with legacy schema)
    try:
        ensure_game_info(SimpleNamespace(
            game_id=game_id,
            game_time=features.get("game_time"),  # tz-aware ISO ok; service will normalize
        ))
    except Exception:
        pass

        # 1st attempt
        res = (
            supabase.from_(TABLE)
            .upsert(
                row_clean,
                on_conflict="prop_source,player_id,game_id,prop_type,prop_value",
            )
            .execute()
        )

        # Some client libs put the DB error into res.error (no exception raised)
        res_err = getattr(res, "error", None)
        if res_err:
            err_text = str(res_err)
            # If it’s an FK failure, try to backfill game_info and retry once
            if (
                "player_props_game_id_fkey" in err_text
                or "foreign key constraint" in err_text.lower()
                or "23503" in err_text
            ):
                try:
                    # New: ensure the FK target exists, then retry once
                    from types import SimpleNamespace
                    from app.services.game_info import ensure_game_info

                    ensure_game_info(
                        SimpleNamespace(
                            game_id=game_id,
                            game_time=features.get("game_time"),  # ISO ok; service normalizes
                        )
                    )

                    res2 = (
                        supabase.from_(TABLE)
                        .upsert(
                            row_clean,
                            on_conflict="prop_source,player_id,game_id,prop_type,prop_value",
                        )
                        .execute()
                    )
                    if getattr(res2, "error", None):
                        raise HTTPException(status_code=500, detail=f"DB insert failed: {res2.error}")
                    return {"saved": True, "row": row_clean, "backfilled_game_info": True}
                except Exception:
                    # fall through to raise original
                    raise HTTPException(status_code=400, detail=err_text)

            # Not an FK error → bubble up
            raise HTTPException(status_code=500, detail=f"DB insert failed: {res_err}")

        # success path
        return {"saved": True, "row": row_clean}

    except PostgrestAPIError as e:  # pragma: no cover
        # Some client libs raise instead of setting res.error
        msg = getattr(e, "message", "") or ""
        code = (getattr(e, "code", "") or "")  # may be '23503' for FK
        details = getattr(e, "details", "") or ""
        text = f"{msg} {details}".strip()

        # Duplicate?
        if "duplicate" in text.lower() or "unique" in text.lower():
            return {"saved": False, "duplicate": True}

        # FK to game_info missing? Backfill and retry once.
        if code == "23503" or "player_props_game_id_fkey" in text or "foreign key" in text.lower():
            try:
                _ensure_game_info_fk(game_id=game_id, features=features)
                res2 = (
                    supabase.from_(TABLE)
                    .upsert(
                        row_clean,
                        on_conflict="prop_source,player_id,game_id,prop_type,prop_value",
                    )
                    .execute()
                )
                if getattr(res2, "error", None):
                    raise HTTPException(status_code=500, detail=f"DB insert failed: {res2.error}")
                return {"saved": True, "row": row_clean, "backfilled_game_info": True}
            except Exception:
                pass

        # Anything else: surface a clear message
        raise HTTPException(status_code=400, detail=text or "Insert failed")
