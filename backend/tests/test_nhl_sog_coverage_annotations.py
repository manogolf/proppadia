from __future__ import annotations

import unittest

from backend.nhl.sog_coverage_annotations import (
    NORMAL_MARKET_OBSERVED, NORMAL_MARKET_UNMATCHED, PIPELINE_REPAIR_DELAY,
    POSTSTART_INELIGIBLE, PRESTART_EVIDENCE_UNAVAILABLE, SCHEMA_VERSION,
    classify_sog_coverage, validate_annotation,
)


class SogCoverageCauseAnnotationTests(unittest.TestCase):
    def setUp(self):
        self.annotation = {
            "schema_version": SCHEMA_VERSION,
            "slate_date": "2026-10-04",
            "game_annotations": [{
                "slate_date": "2026-10-04", "game_id": 2026020035,
                "coverage_status": "NO_VALID_PRESTART_EVIDENCE",
                "cause_classification": PIPELINE_REPAIR_DELAY,
                "cause_scope": "OPERATIONAL_PIPELINE", "affected_proposition_keys": 129,
                "market_absence_inferred": False, "bookmaker_behavior_inferred": False,
                "prediction_absence_inferred": False, "recoverable_after_start": False,
            }],
        }

    def test_annotated_game_gets_pipeline_cause_and_later_games_do_not(self):
        validated = validate_annotation(self.annotation, slate_date="2026-10-04")
        by_game = {row["game_id"]: row for row in validated["game_annotations"]}
        self.assertEqual(classify_sog_coverage(
            game_id=2026020035, timing_status="POSTSTART_INELIGIBLE", annotations=by_game),
            PIPELINE_REPAIR_DELAY)
        self.assertEqual(classify_sog_coverage(
            game_id=2026020036, market_status="MATCHED", timing_status="PRESTART_ELIGIBLE",
            annotations=by_game), NORMAL_MARKET_OBSERVED)
        self.assertEqual(classify_sog_coverage(
            game_id=2026020037, market_status="UNMATCHED", timing_status="PRESTART_ELIGIBLE",
            annotations=by_game), NORMAL_MARKET_UNMATCHED)

    def test_reader_distinguishes_all_coverage_states(self):
        states = [
            classify_sog_coverage(game_id=1, market_status="MATCHED"),
            classify_sog_coverage(game_id=1, market_status="UNMATCHED"),
            classify_sog_coverage(game_id=1, timing_status="POSTSTART_INELIGIBLE"),
            classify_sog_coverage(game_id=1, timing_status="NO_VALID_PRESTART_EVIDENCE"),
            classify_sog_coverage(game_id=2026020035, timing_status="POSTSTART_INELIGIBLE",
                                  annotations={2026020035: self.annotation["game_annotations"][0]}),
        ]
        self.assertEqual(states, [NORMAL_MARKET_OBSERVED, NORMAL_MARKET_UNMATCHED,
                                  POSTSTART_INELIGIBLE, PRESTART_EVIDENCE_UNAVAILABLE,
                                  PIPELINE_REPAIR_DELAY])

    def test_pipeline_cause_does_not_infer_bookmaker_absence_and_is_historical_only(self):
        validated = validate_annotation(self.annotation, slate_date="2026-10-04")
        row = validated["game_annotations"][0]
        self.assertFalse(row["market_absence_inferred"])
        self.assertFalse(row["bookmaker_behavior_inferred"])
        self.assertEqual(row["affected_proposition_keys"], 129)
        # A future run has no incident mapping unless one is explicitly supplied.
        self.assertEqual(classify_sog_coverage(
            game_id=2027020001, timing_status="NO_VALID_PRESTART_EVIDENCE"),
            PRESTART_EVIDENCE_UNAVAILABLE)


if __name__ == "__main__":
    unittest.main()
