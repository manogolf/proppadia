from __future__ import annotations

from typing import Any, Dict

import requests


def fetch_standings(*, season: int, league_ids: str = "103,104") -> Dict[str, Any]:
    url = (
        "https://statsapi.mlb.com/api/v1/standings"
        f"?leagueId={league_ids}"
        f"&season={int(season)}"
        "&standingsTypes=regularSeason"
    )
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()

