from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.scripts.run_nhl_8rain_export import (
    resolve_package_argument,
    validate_export_report,
    validate_package,
)


class NHL8RainDirectExportTests(unittest.TestCase):
    def _package(self, root: Path, slate: str = "2026-10-01") -> Path:
        package = root / "package"
        package.mkdir()
        pd.DataFrame([{
            "game_id": 1, "game_date": slate,
            "home_team": "BOS", "away_team": "NYR",
        }]).to_csv(package / "schedule_event_identity.csv", index=False)
        (package / "daily_execution_status.json").write_text(json.dumps({
            "slate_date": slate, "substantive_state_sha256": "a" * 64,
        }))
        for name in (
            "v2_immutable_predictions.csv", "puck_line_v1_immutable_predictions.csv",
            "raw_market_response.json",
        ):
            (package / name).write_text("fixture\n")
        entries = []
        for path in sorted(package.iterdir()):
            if path.name == "SHA256SUMS":
                continue
            entries.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
        (package / "SHA256SUMS").write_text("\n".join(entries) + "\n")
        return package

    def test_cli_package_overrides_environment_and_environment_remains_supported(self):
        selected = resolve_package_argument("/cli/package", "/env/package")
        self.assertEqual(selected, Path("/cli/package"))
        self.assertEqual(
            resolve_package_argument(None, "/env/package"), Path("/env/package"))

    def test_missing_package_fails_clearly(self):
        with self.assertRaisesRegex(ValueError, "PACKAGE_NOT_FOUND"):
            validate_package(Path("/missing/nhl/package"), current_slate="2026-10-01")

    def test_package_requires_current_slate_and_valid_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            package = self._package(Path(tmp))
            manifest_hash, state_hash = validate_package(
                package, current_slate="2026-10-01")
            self.assertEqual(len(manifest_hash), 64)
            self.assertEqual(state_hash, "a" * 64)
            with self.assertRaisesRegex(ValueError, "PACKAGE_SLATE_MISMATCH"):
                validate_package(package, current_slate="2026-10-02")

    def test_bin_subcommand_accepts_package_and_cli_argument_wins_over_env(self):
        result = subprocess.run(
            ["bash", "bin/nhl_ops.sh", "eight-rain-export", "--package", "/cli/missing"],
            env={"PATH": "/usr/bin:/bin", "NHL_CROSS_MARKET_PACKAGE": "/env/missing"},
            capture_output=True, text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PACKAGE_NOT_FOUND:/cli/missing", result.stderr)

    def test_export_validation_failure_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "EXPORT_VALIDATION_FAILED"):
            validate_export_report({
                "package_state_sha256": "state",
                "package_manifest_sha256": "manifest",
                "validation": {
                    "rows": 4, "duplicate_rows": 0, "pair_failures": 1,
                    "probability_pair_failures": 0,
                },
            }, state_hash="state", manifest_hash="manifest")


if __name__ == "__main__":
    unittest.main()
