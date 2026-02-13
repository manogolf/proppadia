import unittest

from backend.app.services.shared.ping_service import sport_ping


class TestSharedPingService(unittest.TestCase):
    def test_sport_ping_mlb(self):
        self.assertEqual(sport_ping("mlb"), {"sport": "mlb", "ok": True})

    def test_sport_ping_nhl(self):
        self.assertEqual(sport_ping("nhl"), {"sport": "nhl", "ok": True})

    def test_sport_ping_coerces_to_string(self):
        self.assertEqual(sport_ping(123), {"sport": "123", "ok": True})


if __name__ == "__main__":
    unittest.main()
