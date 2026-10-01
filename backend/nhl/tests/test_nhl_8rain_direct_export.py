from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import os
import io
from contextlib import redirect_stdout
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.scripts.run_nhl_8rain_export import (
    current_et_slate,
    resolve_package_selection,
    resolve_package_argument,
    select_latest_refresh_package,
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

    def _refresh_package(
        self, root: Path, *, slate: str = "2026-10-01", stamp: str = "2026-10-01T15:00:00Z",
        run_type: str = "REFRESH", mode: str = "SHADOW_RESEARCH_ONLY", state: str = "a",
    ) -> Path:
        state_hash = state * 64
        package = (root / "season=2026" / f"slate_date={slate}" / f"run_type={run_type}"
                   / f"state={state_hash}")
        package.mkdir(parents=True)
        pd.DataFrame([{
            "canonical_season": 2026, "slate_date": slate, "game_id": 1, "game_date": slate,
            "home_team": "BOS", "away_team": "NYR",
        }]).to_csv(package / "schedule_event_identity.csv", index=False)
        pd.DataFrame([{
            "game_id": 1, "slate_date": slate, "v2_home_win_probability": .55,
            "v2_away_win_probability": .45, "model_version": "NHL_MONEYLINE_V2_TEST",
        }]).to_csv(package / "v2_immutable_predictions.csv", index=False)
        pd.DataFrame([{
            "game_id": 1, "slate_date": slate, "away_by_2_plus_probability": .25,
            "one_goal_game_probability": .40, "home_by_2_plus_probability": .35,
        }]).to_csv(package / "puck_line_v1_immutable_predictions.csv", index=False)
        (package / "raw_market_response.json").write_text("{}\n")
        (package / "daily_execution_status.json").write_text(json.dumps({
            "mode": mode, "slate_date": slate, "run_type": run_type,
            "run_timestamp_utc": stamp, "substantive_state_sha256": state_hash,
            "scheduled_games": 1, "v2_predictions_created": 1,
            "puck_line_v1_predictions_created": 1,
        }))
        entries = []
        for path in sorted(package.iterdir()):
            entries.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
        (package / "SHA256SUMS").write_text("\n".join(entries) + "\n")
        return package

    def test_cli_package_overrides_environment_and_environment_remains_supported(self):
        selected = resolve_package_argument("/cli/package", "/env/package")
        self.assertEqual(selected, Path("/cli/package"))
        self.assertEqual(
            resolve_package_argument(None, "/env/package"), Path("/env/package"))

    def test_latest_refresh_selects_newest_valid_capture_timestamp_not_mtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = self._refresh_package(root, stamp="2026-10-01T14:00:00Z", state="b")
            newest = self._refresh_package(root, stamp="2026-10-01T15:00:00Z", state="c")
            os.utime(newest, (1, 1))
            selected, skipped = select_latest_refresh_package(
                current_slate="2026-10-01", cross_market_root=root)
            self.assertEqual(selected, newest.resolve())
            self.assertEqual(skipped, [])
            self.assertNotEqual(selected, older.resolve())

    def test_failed_refresh_is_skipped_with_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid = self._refresh_package(root, stamp="2026-10-01T14:00:00Z", state="d")
            failed = self._refresh_package(
                root, stamp="2026-10-01T16:00:00Z", mode="FAILED", state="e")
            selected, skipped = select_latest_refresh_package(
                current_slate="2026-10-01", cross_market_root=root)
            self.assertEqual(selected, valid.resolve())
            self.assertTrue(any(str(failed) in item and "REFRESH_STATUS_NOT_COMPLETE" in item
                                for item in skipped))

    def test_prior_day_refresh_is_not_selected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._refresh_package(root, slate="2026-09-30", state="f")
            with self.assertRaisesRegex(ValueError, "CURRENT_DAY_REFRESH_PACKAGE_MISSING:2026-10-01"):
                select_latest_refresh_package(current_slate="2026-10-01", cross_market_root=root)

    def test_auto_package_is_not_selected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._refresh_package(root, run_type="AUTO", state="1")
            with self.assertRaisesRegex(ValueError, "CURRENT_DAY_REFRESH_PACKAGE_MISSING"):
                select_latest_refresh_package(current_slate="2026-10-01", cross_market_root=root)

    def test_cli_precedence_is_explicit_package_then_latest_then_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            latest = self._refresh_package(root, state="2")
            explicit, notes = resolve_package_selection(
                "/manual/immutable/package", latest_refresh=True, env_package="/env/package",
                current_slate="2026-10-01", cross_market_root=root)
            self.assertEqual(explicit, Path("/manual/immutable/package"))
            self.assertTrue(any("explicit --package wins" in note for note in notes))
            selected, notes = resolve_package_selection(
                None, latest_refresh=True, env_package="/env/package",
                current_slate="2026-10-01", cross_market_root=root)
            self.assertEqual(selected, latest.resolve())
            self.assertEqual(notes, [])
            environment, _ = resolve_package_selection(
                None, latest_refresh=False, env_package="/env/package",
                current_slate="2026-10-01", cross_market_root=root)
            self.assertEqual(environment, Path("/env/package"))

    def test_latest_refresh_cli_flag_routes_resolved_package_to_existing_export(self):
        from backend.nhl.scripts import run_nhl_8rain_export as runner

        summary = {
            "slate": "2026-10-01", "state_prefix": "a" * 12, "rows": 4, "pairs": 2,
            "markets": {"Moneyline": 2, "Puck Line": 2, "SOG": 0, "Points": 0, "Saves": 0},
            "mapping_exclusions": 0, "validation": "PASS", "csv_path": "/tmp/out.csv",
            "lineage_path": "/tmp/out_lineage.json",
        }
        with patch.object(runner, "current_et_slate", return_value="2026-10-01"), \
             patch.object(runner, "select_latest_refresh_package",
                          return_value=(Path("/immutable/refresh").resolve(), [])), \
             patch.object(runner, "run_export", return_value=summary) as export, \
             patch.dict(os.environ, {"NHL_CROSS_MARKET_PACKAGE": "/stale/env/package"}), \
             redirect_stdout(io.StringIO()):
            result = runner.main(["--latest-refresh"])
        self.assertEqual(result, 0)
        export.assert_called_once_with(Path("/immutable/refresh").resolve(), current_slate="2026-10-01")

    def test_no_valid_current_day_refresh_fails_clearly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._refresh_package(root, mode="FAILED", state="3")
            with self.assertRaisesRegex(ValueError, "NO_VALID_CURRENT_DAY_REFRESH_PACKAGE:2026-10-01"):
                select_latest_refresh_package(current_slate="2026-10-01", cross_market_root=root)

    def test_latest_resolution_is_absolute_for_export_lineage_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            slate = current_et_slate()
            package = self._refresh_package(root, slate=slate, state="4")
            selected, _ = resolve_package_selection(
                None, latest_refresh=True, env_package=None,
                current_slate=slate, cross_market_root=root)
            manifest_hash, state_hash = validate_package(selected, current_slate=slate)
            self.assertEqual(selected, package.resolve())
            self.assertEqual(state_hash, "4" * 64)
            self.assertEqual(len(manifest_hash), 64)

    def test_latest_resolved_package_is_recorded_and_export_contract_is_unchanged(self):
        from backend.nhl.eightrain_adapter import UPLOAD_COLUMNS
        from backend.nhl.scripts.export_nhl_8rain_upload import main as export_main

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            slate = current_et_slate()
            package = self._refresh_package(root, slate=slate, state="5")
            selected, _ = resolve_package_selection(
                None, latest_refresh=True, env_package=None,
                current_slate=slate, cross_market_root=root)
            catalog = root / "catalog"
            catalog.mkdir()
            (catalog / "model_spec.json").write_text(json.dumps({
                "league": {"code": "nhl"},
                "markets": {"h2h": {"bet": ["home", "away"]},
                            "spread": {"bet": ["home", "away"]}},
                "stats": [],
            }))
            (catalog / "teams.json").write_text(json.dumps({"data": [
                {"abbreviation": "BOS", "code": "bos-boston-bruins"},
                {"abbreviation": "NYR", "code": "nyr-new-york-rangers"},
            ]}))
            (catalog / "players.json").write_text(json.dumps({"data": []}))
            output_dir = root / "immutable_exports"
            with patch("sys.argv", [
                "export_nhl_8rain_upload.py", "--package-dir", str(selected),
                "--catalog-dir", str(catalog), "--date", slate,
                "--immutable-output-dir", str(output_dir),
            ]), redirect_stdout(io.StringIO()) as captured:
                export_main()
            report = json.loads(captured.getvalue())
            csv_path = Path(report["csv_path"])
            lineage_path = csv_path.with_name(f"{csv_path.stem}_lineage.json")
            lineage = json.loads(lineage_path.read_text())
            self.assertEqual(lineage["package_dir"], str(selected))
            self.assertEqual(lineage["package_state_sha256"], "5" * 64)
            exported = pd.read_csv(csv_path)
            self.assertEqual(list(exported.columns), UPLOAD_COLUMNS)
            self.assertEqual(len(exported), 4)
            tz_suffix = datetime.now(ZoneInfo("America/New_York")).tzname()
            self.assertRegex(csv_path.name, rf"^nhl_8rain_raw_manual_upload_{slate}_\d{{8}}T\d{{12}}{tz_suffix}_555555555555\.csv$")

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
