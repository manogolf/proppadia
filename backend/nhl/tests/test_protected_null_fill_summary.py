"""The daily receipt records the protected NULL-fill row count (P1.3)."""
import json
import unittest

from backend.nhl.cli import _structured_child_summary, protected_null_fill_summary_sql
from backend.nhl.daily_orchestration import DailyRunRecorder


def _record(count):
    recorder = DailyRunRecorder(run_id="run", command=["daily"], phase="EARLY")
    stdout = "NHL_CHILD_SUMMARY_JSON=" + json.dumps({
        "schema_version": "NHL_PROTECTED_SOURCE_NULL_FILL_SUMMARY_V1", "slate_date": "2026-10-21",
        "filled_nulls": count,
        "database_row_counts": {"skater_game_logs_raw_protected_null_fills": count},
        "database_row_counts_complete": True,
    })
    summary = {"lane": "shared_prerequisites", "command_identity": "psql", "exit_status": 0,
               "status": "COMPLETE", "database_write_capable": True}
    summary.update(_structured_child_summary(stdout))
    recorder.record_child(summary)
    return recorder.children[-1]


class ProtectedNullFillSummaryTests(unittest.TestCase):
    def test_sql_reports_the_function_count_as_a_child_summary(self):
        sql = protected_null_fill_summary_sql("2026-10-21")
        self.assertIn("nhl.promote_skater_game_log_protected_fields(DATE '2026-10-21')", sql)
        self.assertIn("NHL_CHILD_SUMMARY_JSON=", sql)
        self.assertIn("'filled_nulls', fill.n", sql)
        with self.assertRaises(ValueError):
            protected_null_fill_summary_sql("2026-10-21'; DROP TABLE x; --")

    def test_positive_fill_count_is_kept_and_marked_committed(self):
        child = _record(7)
        self.assertEqual(7, child["filled_nulls"])
        self.assertEqual("COMMITTED", child["database_write_status"])

    def test_zero_fill_count_is_a_quantified_no_write(self):
        child = _record(0)
        self.assertEqual(0, child["filled_nulls"])
        self.assertEqual("NONE", child["database_write_status"])

    def test_unknown_before_fill_count_reaches_the_receipt(self):
        recorder = DailyRunRecorder(run_id="run", command=["daily"], phase="EARLY")
        recorder.record_child({"lane": "shared_prerequisites", "exit_status": 0,
                               "unknown_before_protected_null_fill": 3})
        self.assertEqual(3, recorder.children[-1]["unknown_before_protected_null_fill"])


if __name__ == "__main__":
    unittest.main()
