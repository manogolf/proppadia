"""Grade readers resolve sealed packages to their verified restatements."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.reconciliation_grades import restated_grade_path


ROOT = Path(__file__).resolve().parents[3]
OCT18 = (ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-10-18"
         / "reconciliation=a44cf217c8e17bcb688e")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def seal(directory: Path) -> None:
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))


class RestatedGradePathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nhl_restated_grades_")
        root = Path(self.temp.name) / "postgame_reconciliation"
        self.package = root / "2026-10-18" / "reconciliation=fixture"
        self.package.mkdir(parents=True)
        (self.package / "graded_sog.csv").write_text("grading_state\nUNRESOLVED_UNGRADED\n")
        (self.package / "graded_saves.csv").write_text("grading_status\nSETTLED\n")
        seal(self.package)
        self.restatement = root / "learning_restatements" / "2026-10-18" / "restatement=fixture"
        self.restatement.mkdir(parents=True)
        (self.restatement / "graded_sog.csv").write_text("grading_state\nNO_APPEARANCE_UNGRADED\n")
        self.write_lineage("NHL_PHASE_RESTATEMENT_V4", sha(self.package / "SHA256SUMS"))

    def tearDown(self):
        self.temp.cleanup()

    def write_lineage(self, contract, package_manifest):
        (self.restatement / "lineage.json").write_text(json.dumps({
            "contract_version": contract, "source_package": str(self.package),
            "source_hashes": {"SHA256SUMS": package_manifest}}))
        seal(self.restatement)

    def test_bound_restatement_is_preferred(self):
        self.assertEqual(restated_grade_path(self.package, "graded_sog.csv"),
                         self.restatement / "graded_sog.csv")

    def test_cross_market_restatement_sits_beside_a_phase_restatement(self):
        # Two restatements for one package: each file resolves to the one
        # that carries it.
        cross = self.restatement.parent / "restatement=crossmarket"
        cross.mkdir()
        (cross / "graded_resilient_moneyline_production_champion.csv").write_text("game_id\n2026020120\n")
        (cross / "lineage.json").write_text(json.dumps({
            "contract_version": "NHL_RESILIENT_CROSS_MARKET_RESTATEMENT_V1",
            "source_package": str(self.package),
            "source_hashes": {"SHA256SUMS": sha(self.package / "SHA256SUMS")}}))
        seal(cross)
        self.assertEqual(
            restated_grade_path(self.package, "graded_resilient_moneyline_production_champion.csv"),
            cross / "graded_resilient_moneyline_production_champion.csv")
        self.assertEqual(restated_grade_path(self.package, "graded_sog.csv"),
                         self.restatement / "graded_sog.csv")

    def test_unrestated_files_and_older_contracts_read_the_package(self):
        self.assertEqual(restated_grade_path(self.package, "graded_saves.csv"),
                         self.package / "graded_saves.csv")
        self.write_lineage("NHL_PHASE_RESTATEMENT_V3", sha(self.package / "SHA256SUMS"))
        self.assertEqual(restated_grade_path(self.package, "graded_sog.csv"),
                         self.package / "graded_sog.csv")

    def test_restatement_of_a_different_package_manifest_is_ignored(self):
        self.write_lineage("NHL_PHASE_RESTATEMENT_V4", "0" * 64)
        self.assertEqual(restated_grade_path(self.package, "graded_sog.csv"),
                         self.package / "graded_sog.csv")

    def test_tampered_restatement_is_refused(self):
        (self.restatement / "graded_sog.csv").write_text("grading_state\nWIN\n")
        with self.assertRaisesRegex(RuntimeError, "RESTATEMENT_MANIFEST_MISMATCH"):
            restated_grade_path(self.package, "graded_sog.csv")

    @unittest.skipUnless(OCT18.is_dir(), "retained 2026-10-18 package is a local artifact")
    def test_october_18_resolves_to_its_v4_restatement(self):
        path = restated_grade_path(OCT18, "graded_sog.csv")
        self.assertIn("learning_restatements", str(path))
        states = pd.read_csv(path).grading_state.value_counts().to_dict()
        self.assertNotIn("UNRESOLVED_UNGRADED", states)


if __name__ == "__main__":
    unittest.main()
