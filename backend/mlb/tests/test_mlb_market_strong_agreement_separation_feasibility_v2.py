import unittest

from backend.mlb.scripts import audit_mlb_market_strong_agreement_separation_feasibility_v2 as audit


class AgreementSeparationFeasibilityV2Test(unittest.TestCase):
    def test_power_derivation_reproduces_separate_group_ceiling(self):
        power = audit.power_derivation()
        self.assertEqual(power["baseline_win_rate"], 31/51)
        self.assertEqual(power["agreement_ceiling"], 438)
        self.assertEqual(power["comparison_ceiling"], 294)
        self.assertEqual(power["total_after_separate_group_ceilings"], 732)
        self.assertLess(power["total_exact"], 732)

    def test_amendment_retains_long_target_and_has_nonpromotional_layer(self):
        freeze = audit.amendment_payload("2026-09-09T23:00:00Z", "a"*64, "b"*64)
        self.assertEqual(freeze["long_horizon_confirmatory_target"]["eligible_resolved_games"], 732)
        self.assertTrue(freeze["long_horizon_confirmatory_target"]["not_a_2026_terminal_gate"])
        self.assertEqual(set(freeze["bounded_2026_regular_season_layer"]["categories"]),
                         audit.BOUNDED_CATEGORIES)
        self.assertFalse(freeze["constraints"]["promotion_authorized"])
        self.assertEqual(freeze["acquisition"]["exact_credit_ceiling"], 180)

    def test_cost_evidence_uses_observed_headers(self):
        summary, evidence = audit.cost_evidence()
        historical = evidence[evidence.evidence_class.eq("IDENTICAL_TEN_BOOK_HISTORICAL_H2H")]
        self.assertEqual(len(historical), 29)
        self.assertTrue(historical.x_requests_last.eq(10).all())
        self.assertEqual(summary["current_pinnacle_three_market_observed_cost_each"], 3)
        self.assertTrue(summary["current_capture_expansion_conclusion"].startswith("NOT_VERIFIED"))


if __name__ == "__main__":
    unittest.main()
