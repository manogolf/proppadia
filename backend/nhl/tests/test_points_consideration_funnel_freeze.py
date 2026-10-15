import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "backend/nhl/scripts/freeze_nhl_points_consideration_funnel.py"
SPEC = importlib.util.spec_from_file_location("freeze_points_funnel", SCRIPT)
freeze = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(freeze)


class FrozenPointsFunnelTests(unittest.TestCase):
    def test_strict_stage_rules_and_order(self):
        self.assertEqual(freeze.classify(-0.001, 0.50, "UNDER", "OVER")["first_exclusion_stage"], "STAGE_1")
        self.assertEqual(freeze.classify(0.0, 0.1500001, "UNDER", "OVER")["first_exclusion_stage"], "STAGE_2")
        self.assertEqual(freeze.classify(0.0, 0.15, "UNDER", "OVER")["first_exclusion_stage"], "STAGE_3")
        self.assertEqual(freeze.classify(0.0, 0.10, "UNDER", "UNDER")["first_exclusion_stage"], "")
        self.assertEqual(freeze.classify(None, 0.10, "UNDER", "UNDER")["edge_status"], "EDGE_UNAVAILABLE")
        self.assertTrue(freeze.classify(None, None, "UNDER", "UNDER")["survivor"])

    def test_oct14_pre_outcome_binding_reproduces_prior_counts(self):
        path = freeze.DISCOVERY / "current_day_dry_view_2026-10-14.csv"
        rows = freeze.read_csv(path)
        counts = {}
        for line in ("0.5", "1.5", "2.5"):
            subset = [r for r in rows if float(r["line"]) == float(line)]
            classified = [freeze.classify(freeze.nfloat(r["selected_production_edge"]),
                          freeze.nfloat(r["signed_selected_model_minus_no_vig_market_gap"]),
                          r["model_side"], r["market_side"]) for r in subset]
            counts[line] = (len(subset), sum(not x["stage1_excluded"] for x in classified),
                            sum(not x["stage1_excluded"] and not x["stage2_excluded"] for x in classified),
                            sum(x["survivor"] for x in classified))
        self.assertEqual(counts, {"0.5": (58,49,23,20), "1.5": (31,28,19,19), "2.5": (4,4,4,4)})

    def test_discovery_baseline_reconciles_to_committed_package(self):
        rows = freeze.read_csv(freeze.DISCOVERY / "canonical_funnel_table.csv")
        counts = {r["stage"]: int(r["remaining_n"]) for r in rows
                  if r["line"] == "0.5" and r["stage"] in {"STAGE_0","STAGE_1","STAGE_2","STAGE_3"}}
        self.assertEqual(tuple(counts[k] for k in ("STAGE_0","STAGE_1","STAGE_2","STAGE_3")),
                         (658,538,263,227))

    def test_append_only_records_reject_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.csv"
            fields = ["date","id","classification"]
            freeze.append_only_csv(path,[{"date":"2026-10-14","id":"p1","classification":"STAGE_2"}],fields,("date","id"))
            with self.assertRaises(AssertionError):
                freeze.append_only_csv(path,[{"date":"2026-10-14","id":"p1","classification":"SURVIVOR"}],fields,("date","id"))

    def test_outcome_cannot_change_classification_and_populations_are_separate(self):
        row = {"selected_production_edge":"0.01","signed_selected_model_minus_no_vig_market_gap":"0.16","model_side":"UNDER","market_side":"OVER","official_points":""}
        baseline = freeze.classify_row(row)
        row["official_points"] = "9"
        with_outcome = freeze.classify_row(row)
        self.assertEqual(baseline, with_outcome)
        self.assertNotEqual(freeze.DISCOVERY, freeze.OUT)
        self.assertTrue(freeze.OUT.is_relative_to(freeze.ROOT / "artifacts/analysis/nhl/points_consideration_funnel_freeze"))
        self.assertEqual(freeze.DISCOVERY_COUNTS["0.5"], {
            "stage_0":658,"stage_1_survivors":538,"stage_2_survivors":263,"stage_3_survivors":227})
        summary = __import__("json").loads((freeze.OUT/"summary.json").read_text())
        self.assertTrue(summary["discovery_and_validation_separate"])
        self.assertEqual(summary["prospective_finalized_dates"], 0)
        ledger = freeze.read_csv(freeze.OUT/"prospective_ledger.csv")
        self.assertEqual(len(ledger), 93)
        self.assertTrue(all(r["official_points"] == "" and r["record_type"] == "CLASSIFICATION" for r in ledger))


if __name__ == "__main__":
    unittest.main()
