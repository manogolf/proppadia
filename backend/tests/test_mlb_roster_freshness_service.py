import unittest
from unittest.mock import patch

from backend.app.services.mlb import roster_freshness_service as service


class TestMlbRosterFreshnessService(unittest.TestCase):
    def test_pass_when_minimum_and_freshness_are_ok(self):
        side_effects = [
            {"has_active": True, "has_updated_at": True},
            {"n": 1200},  # total
            {"n": 1197},  # active
            {"latest_updated_at": "2026-02-15T00:00:00+00:00"},
        ]
        with patch.object(service, "pg_fetchone", side_effect=side_effects):
            out = service.get_roster_freshness(require_min=1, stale_after_hours=30)
        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "pass")
        self.assertEqual(out["total_players"], 1200)
        self.assertEqual(out["active_players"], 1197)

    def test_fail_when_under_minimum(self):
        side_effects = [
            {"has_active": False, "has_updated_at": False},
            {"n": 0},  # total
        ]
        with patch.object(service, "pg_fetchone", side_effect=side_effects):
            out = service.get_roster_freshness(require_min=1, stale_after_hours=30)
        self.assertFalse(out["ok"])
        self.assertEqual(out["status"], "fail")
        self.assertEqual(out["total_players"], 0)
        self.assertIsNone(out["active_players"])
        self.assertIsNone(out["stale"])


if __name__ == "__main__":
    unittest.main()

