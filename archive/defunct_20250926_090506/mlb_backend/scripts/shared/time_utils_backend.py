# File: backend/scripts/shared/time_utils_backend.py

from datetime import datetime
from typing import Optional
from pytz import timezone
import requests
from dateutil import parser

ET = timezone("US/Eastern")

def now_et() -> datetime:
    return datetime.now(ET)

def today_et() -> str:
    return now_et().strftime("%Y-%m-%d")

def yesterday_et() -> str:
    return (now_et() - timedelta(days=1)).strftime("%Y-%m-%d")

def format_game_time(iso_datetime: str) -> dict:
    if not iso_datetime:
        return {"etTime": "", "localTime": ""}
    dt = parser.isoparse(iso_datetime)
    et_time = dt.astimezone(ET).strftime("%H:%M")
    local_time = dt.strftime("%H:%M")
    return {"etTime": et_time, "localTime": local_time}

def format_date_et(date_string: str) -> str:
    dt = parser.isoparse(date_string).astimezone(ET)
    return dt.strftime("%b %d, %Y")

def get_day_of_week_et(iso_date: str) -> str:
    dt = parser.isoparse(iso_date).astimezone(ET)
    return dt.strftime("%A")

def get_time_of_day_bucket_et(iso_datetime):
    if isinstance(iso_datetime, str):
        dt = parser.isoparse(iso_datetime)
    elif isinstance(iso_datetime, datetime):
        dt = iso_datetime
    else:
        raise ValueError(f"Unsupported datetime input: {iso_datetime}")

    hour = dt.astimezone(ET).hour

    if hour < 12:
        return "morning"
    elif hour < 17:
        return "afternoon"
    elif hour < 20:
        return "evening"
    else:
        return "night"
def get_game_start_time_et(game_id: int) -> Optional[str]:
    try:
        sched_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk={game_id}"
        sched_res = requests.get(sched_url).json()
        sched_iso = sched_res.get("dates", [{}])[0].get("games", [{}])[0].get("gameDate")
        if sched_iso:
            return parser.isoparse(sched_iso).astimezone(ET).isoformat()
        box_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
        box_res = requests.get(box_url).json()
        box_iso = box_res.get("gameData", {}).get("datetime", {}).get("dateTime")
        if box_iso:
            return parser.isoparse(box_iso).astimezone(ET).isoformat()
    except Exception as e:
        print(f"⚠️ Could not get game time for {game_id}: {e}")
    return None
