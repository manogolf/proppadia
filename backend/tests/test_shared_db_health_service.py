import unittest
from unittest.mock import patch

from backend.app.services.shared.db_health_service import ping_db


class TestDbHealthService(unittest.TestCase):
    def test_ping_db_ok_true_when_row_present(self):
        with patch(
            "backend.app.services.shared.db_health_service.pg_fetchone",
            return_value=(True, {"ok": 1}, None),
        ):
            out = ping_db()
        self.assertEqual(out, {"ok": True, "err": None})

    def test_ping_db_ok_false_when_row_missing(self):
        with patch(
            "backend.app.services.shared.db_health_service.pg_fetchone",
            return_value=(False, None, "OperationalError: down"),
        ):
            out = ping_db()
        self.assertEqual(out.get("ok"), False)
        self.assertIn("OperationalError", str(out.get("err")))


if __name__ == "__main__":
    unittest.main()
