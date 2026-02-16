import unittest
from unittest.mock import patch

from backend.scripts import insert_mlb_stat_derived as script


class TestSharedInsertMlbStatDerived(unittest.TestCase):
    def test_fetch_schedule_handles_empty_dates(self):
        with patch.object(script, "_fetch_json", return_value={"dates": []}):
            rows = script._fetch_schedule("2026-02-14")
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
