import importlib.util
from pathlib import Path
import unittest

import pandas as pd


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "evaluate_nhl_sog_fixed_blend_prospective.py"
SPEC = importlib.util.spec_from_file_location("evaluate_sog_fixed_blend_prospective", SCRIPT)
EVALUATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EVALUATOR)


class ProspectiveEvaluationHelpersTest(unittest.TestCase):
    def test_paired_delta_supports_overall_and_grouped_comparisons(self):
        frame = pd.DataFrame({
            "brier_component_candidate": [0.1, 0.4],
            "brier_component_reference": [0.2, 0.3],
            "logloss_component_candidate": [0.2, 0.5],
            "logloss_component_reference": [0.3, 0.4],
            "abs_error_candidate": [0.1, 0.4],
            "abs_error_reference": [0.2, 0.3],
            "sq_error_candidate": [0.1, 0.4],
            "sq_error_reference": [0.2, 0.3],
            "count_abs_error_candidate": [1.0, 3.0],
            "count_abs_error_reference": [2.0, 2.0],
            "count_sq_error_candidate": [1.0, 9.0],
            "count_sq_error_reference": [4.0, 4.0],
            "line": [1.5, 2.5],
        })
        overall = EVALUATOR.paired_delta(frame, [], "candidate", "reference")
        grouped = EVALUATOR.paired_delta(frame, ["line"], "candidate", "reference")
        self.assertEqual(len(overall), 1)
        self.assertEqual(overall[0]["n"], 2)
        self.assertAlmostEqual(overall[0]["delta_brier_component_mean"], 0.0)
        self.assertEqual(len(grouped), 2)

    def test_markdown_table_escapes_pipes_and_formats_values(self):
        rendered = EVALUATOR.markdown_table(pd.DataFrame({"metric": ["a|b"], "value": [0.125]}))
        self.assertIn("a\\|b", rendered)
        self.assertIn("0.125", rendered)


if __name__ == "__main__":
    unittest.main()
