import unittest

from backend.mlb.scripts import audit_mlb_market_strong_agreement_separation_live_timing_v3 as audit


class AgreementSeparationLiveTimingV3Test(unittest.TestCase):
    def test_acquisition_options_reconcile(self):
        options = {item["option"]: item for item in audit.acquisition_options()}
        self.assertEqual(options["EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST"]["incremental_study_credits_no_failures"], 0)
        self.assertEqual(options["ADD_SEPARATE_LIVE_TEN_BOOK_H2H_REQUEST"]["incremental_study_credits_no_failures"], 18)
        self.assertEqual(options["HISTORICAL_SNAPSHOT_FOR_EVERY_DATE"]["incremental_study_credits_no_failures"], 180)
        self.assertEqual(options["ADD_SEPARATE_LIVE_TEN_BOOK_H2H_REQUEST"]["incremental_if_historical_recovery_every_date"], 198)

    def test_freeze_is_post_commit_and_nonpromotional(self):
        freeze = audit.freeze_payload("2026-09-09T23:30:00Z")
        capture = freeze["designated_live_capture"]
        self.assertTrue(capture["request_must_begin_after_prediction_barrier"])
        self.assertEqual(capture["bookmakers"], list(audit.FROZEN_BOOKS))
        self.assertEqual(capture["bookmaker_groups"], 1)
        self.assertEqual(capture["maximum_prediction_commit_to_first_request_seconds"], 300)
        self.assertFalse(freeze["constraints"]["scheduler_changed"])
        self.assertFalse(freeze["constraints"]["promotion_authorized"])

    def test_failure_recovery_cannot_choose_on_price_or_outcome(self):
        recovery = audit.freeze_payload("2026-09-09T23:30:00Z")["failure_recovery"]
        self.assertTrue(recovery["outcomes_or_prices_may_not_control_recovery"])
        self.assertIn("classify that cell", recovery["bookmaker_absence_or_one_sided_market_in_successful_live_response"])
        self.assertIn("demonstrably uncharged", recovery["immediate_live_retry"])


if __name__ == "__main__":
    unittest.main()
