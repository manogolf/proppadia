# backend/scripts/generateStatDerivedProps.py

import os
from supabase import create_client, Client
from datetime import datetime, timedelta
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

PROP_TYPES = [
    "Hits", "Singles", "Doubles", "Triples", "Home Runs", "Total Bases", "RBIs", "Runs Scored",
    "Hits + Runs + RBIs", "Walks", "Strikeouts (Batting)", "Stolen Bases", "Outs Recorded",
    "Strikeouts (Pitching)", "Earned Runs", "Pitches Thrown", "Pitching Hits Allowed",
    "Pitching Walks Allowed", "Pitching Total Bases Allowed"
]

def fetch_boxscore(game_id):
    url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Failed to fetch boxscore for game {game_id}: {e}")
        return None

def generate_props_for_date(target_date):
    target_date_str = target_date.strftime("%Y-%m-%d")
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={target_date_str}&hydrate=boxscore"

    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Failed to fetch schedule for {target_date_str}")
        return

    games = response.json().get("dates", [{}])[0].get("games", [])

    for game in games:
        game_id = game.get("gamePk")
        boxscore = fetch_boxscore(game_id)
        if not boxscore:
            continue

        for team_key in ["home", "away"]:
            players = boxscore["teams"][team_key]["players"]
            for player_id, player_data in players.items():
                stats = player_data.get("stats", {})
                hitting = stats.get("batting", {})
                pitching = stats.get("pitching", {})
                player_name = player_data["person"].get("fullName")

                for prop in PROP_TYPES:
                    value = extract_stat_value(prop, hitting, pitching)
                    if value is None:
                        continue  # Skip if value isn't found

                    # Construct and insert prop
                    supabase.table("model_training_props").upsert({
                        "player_name": player_name,
                        "player_id": player_data["person"].get("id"),
                        "team": player_data.get("parentTeamId"),
                        "prop_type": prop,
                        "prop_value": value,
                        "result": value,
                        "outcome": "win" if value > 0 else "loss",
                        "status": "resolved",
                        "game_date": target_date_str,
                        "game_id": game_id
                    }).execute()

def extract_stat_value(prop_type, hitting, pitching):
    mapping = {
        "Hits": hitting.get("hits"),
        "Singles": hitting.get("singles"),
        "Doubles": hitting.get("doubles"),
        "Triples": hitting.get("triples"),
        "Home Runs": hitting.get("homeRuns"),
        "Total Bases": hitting.get("totalBases"),
        "RBIs": hitting.get("rbi"),
        "Runs Scored": hitting.get("runs"),
        "Hits + Runs + RBIs": (hitting.get("hits", 0) + hitting.get("runs", 0) + hitting.get("rbi", 0)),
        "Walks": hitting.get("baseOnBalls"),
        "Strikeouts (Batting)": hitting.get("strikeOuts"),
        "Stolen Bases": hitting.get("stolenBases"),
        "Outs Recorded": pitching.get("outs"),
        "Strikeouts (Pitching)": pitching.get("strikeOuts"),
        "Earned Runs": pitching.get("earnedRuns"),
        "Pitches Thrown": pitching.get("pitchesThrown"),
        "Pitching Hits Allowed": pitching.get("hits"),
        "Pitching Walks Allowed": pitching.get("baseOnBalls"),
        "Pitching Total Bases Allowed": pitching.get("totalBases")
    }
    return mapping.get(prop_type)

if __name__ == "__main__":
    # Process yesterday's stats by default
    target_date = datetime.utcnow() - timedelta(days=1)
    generate_props_for_date(target_date)
