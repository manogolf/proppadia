from __future__ import annotations

from typing import Any, Dict

import requests


def fetch_schedule(*, game_date: str) -> Dict[str, Any]:
    url = (
        "https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={game_date}"
        "&hydrate=team,linescore,probablePitcher,decisions,game(content(summary),live),boxscore"
    )
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()
