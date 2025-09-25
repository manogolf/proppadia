# backend/scripts/shared/mlbUtils.py (create if needed)

import requests

def getTeamWinRates():
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason&hydrate=team"
    res = requests.get(url)
    data = res.json()
    win_rates = {}

    for record in data.get("records", []):
        for team_record in record.get("teamRecords", []):
            team = team_record["team"]["abbreviation"]
            wins = team_record["wins"]
            losses = team_record["losses"]
            total = wins + losses
            if total > 0:
                win_rates[team] = round(wins / total, 3)

    return win_rates
