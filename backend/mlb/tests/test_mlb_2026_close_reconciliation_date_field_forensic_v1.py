from __future__ import annotations

import unittest

from backend.mlb.season_transition.close_date_field_forensic_v1 import (
    CONFLICTS, GAPS, build_report, validate_report,
)


class CloseDateFieldForensicV1Tests(unittest.TestCase):
    def test_824785_relationship_is_reconciled_without_timezone_inference(self) -> None:
        report = build_report()
        self.assertEqual(tuple(row["gamePk"] for row in report["conflict_cases"]), CONFLICTS)
        self.assertEqual(report["conflict_cases"][-1]["classification"],
                         "RESOLVED_POSTPONEMENT_RESCHEDULE")
        for case in report["conflict_cases"][:3]:
            self.assertEqual(case["classification"],
                             "MULTIPLE_OFFICIAL_APPEARANCES_RELATIONSHIP_MISSING")
            self.assertTrue(case["schedule_appearances"])
            self.assertTrue(case["feed_appearances"])
            self.assertTrue(all(row["sha256"] for row in case["schedule_appearances"] + case["feed_appearances"]))
        known = report["conflict_cases"][-1]
        self.assertTrue(any(a["relationships"].get("rescheduleDate") == "2026-09-23T17:35:00Z"
                            for a in known["schedule_appearances"]))
        self.assertTrue(any(a["relationships"].get("rescheduledFrom") == "2026-09-22T22:35:00Z"
                            for a in known["schedule_appearances"]))

    def test_exact_feed_gaps_are_absent_from_both_documented_roots(self) -> None:
        report = build_report()
        self.assertEqual(tuple(row["gamePk"] for row in report["feed_gaps"]), GAPS)
        self.assertTrue(all(row["classification"] == "RETAINED_SOURCE_GAP" for row in report["feed_gaps"]))
        self.assertEqual(len(report["feed_gap_search_roots"]), 2)
        self.assertTrue(all(row["inventory_sha256"] for row in report["feed_gap_search_roots"]))

    def test_committed_report_and_manifest_hashes_validate(self) -> None:
        self.assertTrue(validate_report()["integrity_passed"])


if __name__ == "__main__":
    unittest.main()
