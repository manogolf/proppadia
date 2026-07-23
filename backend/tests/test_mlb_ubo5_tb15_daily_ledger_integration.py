import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
ART = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib"


class DailyLedgerIntegrationTest(unittest.TestCase):
    def run_apply(self, feature_path: Path, temp: Path):
        wide = temp / "wide.csv"
        pd.DataFrame([{
            "game_id": 1, "player_id": 2, "prop_type": "total_bases", "p_over_1_5": .45,
        }]).to_csv(wide, index=False)
        health = temp / "health.json"
        subprocess.run([
            sys.executable, "-m", "backend.mlb.scripts.apply_mlb_ubo5_tb15_production_route",
            "--slate-date", "2026-07-23", "--wide-csv", str(wide),
            "--feature-ledger", str(feature_path), "--artifact", str(ART),
            "--ledger-out", str(temp / "route.csv"), "--health-out", str(health),
        ], cwd=ROOT, check=True, capture_output=True, text=True)
        return json.loads(health.read_text())

    def test_missing_is_integration_error(self):
        with tempfile.TemporaryDirectory() as raw:
            health = self.run_apply(Path(raw) / "missing.parquet", Path(raw))
            self.assertEqual(health["integration_status"], "ERROR_FEATURE_LEDGER_MISSING")
            self.assertEqual(health["routed_rows"], 0)

    def test_valid_empty_is_not_missing(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "empty.parquet"
            pd.DataFrame(columns=["game_pk"]).to_parquet(path, index=False)
            health = self.run_apply(path, Path(raw))
            self.assertEqual(health["integration_status"], "NO_CURRENT_CANDIDATES")
            self.assertEqual(health["fallback_rows"], 0)

    def test_malformed_fails_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "bad.csv"
            path.write_text("not,a,usable,ledger\n1,2,3,4\n")
            health = self.run_apply(path, Path(raw))
            self.assertEqual(health["integration_status"], "ERROR_MALFORMED_FEATURE_LEDGER")
            self.assertEqual(health["routed_rows"], 0)


if __name__ == "__main__":
    unittest.main()
