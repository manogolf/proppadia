import unittest
from unittest.mock import MagicMock, patch

import httpx

from backend.app.services.nhl.gamecenter_service import fetch_gamecenter_landing


class _AsyncClientStub:
    def __init__(self, response=None, error=None):
        self._response = response
        self._error = error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, _url, headers=None):
        if self._error:
            raise self._error
        return self._response


class TestNhlGamecenterService(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_gamecenter_landing_success(self):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"gameState": "FINAL"}

        with patch(
            "backend.app.services.nhl.gamecenter_service.httpx.AsyncClient",
            return_value=_AsyncClientStub(response=response),
        ):
            out = await fetch_gamecenter_landing(2024020001)

        self.assertTrue(out["ok"])
        self.assertEqual(out["game_id"], 2024020001)
        self.assertEqual(out["data"]["gameState"], "FINAL")

    async def test_fetch_gamecenter_landing_failure(self):
        error = httpx.ConnectError("network down")
        with patch(
            "backend.app.services.nhl.gamecenter_service.httpx.AsyncClient",
            return_value=_AsyncClientStub(error=error),
        ):
            out = await fetch_gamecenter_landing(2024020001)

        self.assertFalse(out["ok"])
        self.assertEqual(out["game_id"], 2024020001)
        self.assertIn("network down", out["error"])


if __name__ == "__main__":
    unittest.main()
