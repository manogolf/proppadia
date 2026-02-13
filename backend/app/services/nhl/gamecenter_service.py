from __future__ import annotations

from typing import Any, Dict

import httpx


async def fetch_gamecenter_landing(game_id: int) -> Dict[str, Any]:
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/landing"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers={"User-Agent": "proppadia/1.0"})
        response.raise_for_status()
        return {"ok": True, "game_id": game_id, "data": response.json()}
    except Exception as e:
        return {"ok": False, "game_id": game_id, "error": str(e)}
