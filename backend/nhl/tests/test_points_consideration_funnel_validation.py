import unittest

from backend.nhl.scripts.grade_points_consideration_funnel_validation import flags_from_dry


class FrozenFunnelRuleTests(unittest.TestCase):
    def test_outcome_fields_do_not_affect_v1_classification(self):
        row = {
            "selected_production_edge": "0.01",
            "signed_selected_model_minus_no_vig_market_gap": "0.151",
            "model_side": "OVER",
            "market_side": "UNDER",
            "official_points": "999",
            "grade_status": "SETTLED",
        }
        first = flags_from_dry(row)
        row["official_points"] = "0"
        row["grade_status"] = "UNRESOLVED"
        self.assertEqual(first, flags_from_dry(row))
        self.assertEqual(first["first_exclusion_stage"], "STAGE_2")

    def test_stage_order_and_strict_gap_boundary(self):
        self.assertEqual(
            flags_from_dry({
                "selected_production_edge": "-0.001",
                "signed_selected_model_minus_no_vig_market_gap": "0.30",
                "model_side": "OVER",
                "market_side": "UNDER",
            })["first_exclusion_stage"],
            "STAGE_1",
        )
        self.assertTrue(flags_from_dry({
            "selected_production_edge": "0.0",
            "signed_selected_model_minus_no_vig_market_gap": "0.15",
            "model_side": "UNDER",
            "market_side": "UNDER",
        })["survivor"])
        self.assertEqual(flags_from_dry({
            "selected_production_edge": "0.0",
            "signed_selected_model_minus_no_vig_market_gap": "0.0",
            "model_side": "UNDER",
            "market_side": "OVER",
        })["first_exclusion_stage"], "STAGE_3")


if __name__ == "__main__":
    unittest.main()
