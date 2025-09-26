# backend/app/services/games.py
from __future__ import annotations
from typing import Dict, Optional
from datetime import datetime

# If you need team abbreviation normalization / id helpers later:
# from backend.app.services.team_map import normalize_abbr, abbr_to_team_id

def enrich_game_context(team_id: int, game_date_iso: str) -> Dict:
    """
    Minimal placeholder to unblock /api/prepare_prop.
    Replace with your real enrich logic (probable pitchers, opponent, etc.).
    """
    # Keep structure consistent with what your frontend & predict expect.
    # Use None where data isn't available yet, 0/1 for booleans if your model expects ints.
    return {
        "team_id": team_id,
        "game_date": game_date_iso,
        "game_id": None,
        "is_home": 0,                 # 0/1 as int if your features expect numeric
        "opponent": None,
        "opponent_encoded": 0,        # numeric encoding expected by model
        "game_time": None,            # ISO timestamp string when you implement it
        "starting_pitcher_id": None,  # fill when you add MLB feed lookup
        # Any other fields your feature builder might read:
        "game_day_of_week": None,
        "time_of_day_bucket": None,
    }

# Optional: some code might import `context` instead.
def context(team_id: int, game_date_iso: str) -> Dict:
    return enrich_game_context(team_id, game_date_iso)
