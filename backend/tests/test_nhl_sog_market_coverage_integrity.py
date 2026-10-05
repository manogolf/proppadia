from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, sha256_file
from backend.nhl.performance_summary import discover_daily_market_coverage, summarize_frames
from backend.nhl.sog_attachment_integrity import (
    audit_sog_attachment, retain_sog_attachment_package,
    verify_sog_integrity_package,
)
from backend.nhl import cli as nhl_cli


SLATE = "2026-10-03"
RUN_ID = "nhldaily_fixture"
ODDS_SHA = "a" * 64


def write_manifest(directory: Path) -> str:
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    path = directory / "SHA256SUMS"
    path.write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))
    return sha256_file(path)


def make_fixture(root: Path, *, attachment: pd.DataFrame | None = None,
                 unmatched: pd.DataFrame | None = None):
    pred = root / "predictions.csv"
    names = root / "names.csv"
    attached = root / "sog_with_market.csv"
    unmatched_path = root / "unmatched_sog.csv"
    pred_frame = pd.DataFrame({
        "game_date": [SLATE, SLATE], "game_id": [2026020022, 2026020022],
        "player_id": [101, 102], "poisson_source": ["d10", "season"],
        "p_over_1_5": [0.6, 0.4], "p_over_2_5": [0.2, 0.1],
    })
    pred_frame.to_csv(pred, index=False)
    names_frame = pd.DataFrame({
        "game_date": [SLATE, SLATE], "game_id": [2026020022, 2026020022],
        "player_id": [101, 102], "team_id": [1, 2],
        "team_code": ["AAA", "BBB"], "full_name": ["Player One", "Player Two"],
    })
    names_frame.to_csv(names, index=False)
    default = pd.DataFrame({
        "game_date": [SLATE] * 4,
        "game_id": [2026020022] * 4,
        "player_id": [101, 101, 102, 102],
        "line": [1.5, 2.5, 1.5, 2.5],
        "price_over": [-110, -115, pd.NA, pd.NA],
        "p_over_mkt": [0.5, 0.52, pd.NA, pd.NA],
    })
    default_unmatched = default[pd.to_numeric(default.p_over_mkt, errors="coerce").isna()].copy()
    (attachment if attachment is not None else default).to_csv(attached, index=False)
    (unmatched if unmatched is not None else default_unmatched).to_csv(unmatched_path, index=False)

    odds = root / "odds" / "observation=fixture"
    odds.mkdir(parents=True)
    (odds / "raw_response.json").write_text("[]\n")
    (odds / "events_response.json").write_text("[]\n")
    (odds / "observation_summary.json").write_text(json.dumps({
        "slate_date": SLATE, "season": 2026, "phase": "EARLY",
        "classification": "CAPTURED_NONEMPTY", "strictly_prestart": True,
        "observation_timestamp_utc": "2026-10-03T17:00:00Z",
        "first_puck_utc": "2026-10-03T23:00:00Z",
        "parent_daily_run_id": RUN_ID,
        "canonical_game_set_hash": canonical_game_set_hash([2026020022]),
    }, indent=2) + "\n")
    (odds / "RUN_COMPLETE.json").write_text("{}\n")
    write_manifest(odds)
    return pred, names, attached, unmatched_path, odds


class SogMarketCoverageIntegrityTests(unittest.TestCase):
    def test_game_specific_timing_is_strict_at_start_and_key_counts_reconcile(self):
        with tempfile.TemporaryDirectory() as raw:
            pred, names, attached, unmatched, odds = make_fixture(Path(raw))
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
                canonical_game_starts_utc={2026020022: "2026-10-03T17:00:00Z"},
            )
            timing = report["game_specific_prestart"]
            self.assertEqual(timing["prestart_eligible_prediction_keys"], 0)
            self.assertEqual(timing["poststart_ineligible_prediction_keys"], 4)
            self.assertEqual(timing["ineligible_game_count"], 1)
            self.assertEqual(timing["prestart_matched"] + timing["prestart_unmatched"], 0)

    def test_daily_builder_retains_run_bound_integrity_package(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, source_attachment, source_unmatched, odds = make_fixture(root)
            site = root / "site"
            retained = root / "retained"
            site.mkdir()

            def fake_run(_command):
                source_attachment.rename(site / "sog_with_market.csv")
                source_unmatched.rename(site / "unmatched_sog.csv")

            manifest_sha = sha256_file(odds / "SHA256SUMS")
            odds_lineage = {
                "odds_observation_manifest_sha256": manifest_sha,
                "odds_observation_replayed": False,
            }
            with patch.object(nhl_cli, "SITE_DIR", site), \
                    patch.object(nhl_cli, "SOG_ATTACHMENT_ROOT", retained), \
                    patch.object(nhl_cli, "export_names_csv", return_value=names), \
                    patch.object(nhl_cli, "run", side_effect=fake_run), \
                    patch.object(nhl_cli, "validate_odds_observation",
                                  return_value=odds_lineage):
                nhl_cli.build_sog(
                    SLATE, odds_json=odds / "raw_response.json",
                    events_json=odds / "events_response.json", pred_path=pred,
                    expected_pred_sha256=sha256_file(pred),
                    parent_daily_run_id=RUN_ID, odds_observation_dir=odds,
                    expected_odds_manifest_sha256=manifest_sha, odds_phase="EARLY",
                )

            package = retained / "season=2026" / f"slate_date={SLATE}" / f"run_id={RUN_ID}"
            report, _ = verify_sog_integrity_package(
                package / "sog_attachment_integrity.json", SLATE)
            self.assertEqual(report["parent_daily_run_id"], RUN_ID)
            self.assertEqual(report["prediction_artifact_sha256"], sha256_file(pred))
            self.assertEqual(report["odds_observation_manifest_sha256"], manifest_sha)
            self.assertEqual(report["status"], "PASS")
            self.assertTrue((package / "sog_with_market.csv").is_file())
            self.assertTrue((package / "unmatched_sog.csv").is_file())

    def test_exact_player_game_prop_line_population_and_counts(self):
        with tempfile.TemporaryDirectory() as raw:
            pred, names, attached, unmatched, odds = make_fixture(Path(raw))
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["counts"]["prediction_row_count"], 4)
        self.assertEqual(report["counts"]["matched_count"], 2)
        self.assertEqual(report["counts"]["unmatched_count"], 2)
        self.assertEqual(report["counts"]["ambiguous_count"], 0)
        self.assertEqual(report["prediction_identity_columns"], ["poisson_source"])
        self.assertEqual(report["canonical_key_columns"], ["game_date", "game_id", "player_id", "prop", "line"])

    def test_different_line_does_not_count_as_match(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            frame = pd.read_csv(attached)
            frame.loc[0, "line"] = 3.5
            frame.to_csv(attached, index=False)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["counts"]["missing_prediction_key_count"], 1)
        self.assertEqual(report["counts"]["extra_attachment_key_count"], 1)

    def test_duplicate_prediction_and_attachment_keys_fail(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            source = pd.read_csv(pred)
            pd.concat([source, source.iloc[[0]]]).to_csv(pred, index=False)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
        self.assertEqual(report["status"], "FAIL")
        self.assertGreater(report["counts"]["duplicate_prediction_key_count"], 0)
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            frame = pd.read_csv(attached)
            pd.concat([frame, frame.iloc[[0]]]).to_csv(attached, index=False)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
        self.assertEqual(report["status"], "FAIL")
        self.assertGreater(report["counts"]["duplicate_attachment_key_count"], 0)

    def test_same_slate_and_missing_or_extra_keys_fail(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            frame = pd.read_csv(attached).iloc[:-1]
            frame.to_csv(attached, index=False)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["counts"]["missing_prediction_key_count"], 1)

            extra_root = root / "extra"
            extra_root.mkdir()
            pred, names, attached, unmatched, odds = make_fixture(extra_root)
            frame = pd.read_csv(attached)
            extra_row = frame.iloc[[0]].copy()
            extra_row.loc[:, "game_id"] = 2026020022
            extra_row.loc[:, "line"] = 4.5
            extra_row.loc[:, "price_over"] = float("nan")
            extra_row.loc[:, "p_over_mkt"] = float("nan")
            frame = pd.concat([frame, extra_row], ignore_index=True)
            frame.to_csv(attached, index=False)
            extra_report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
            self.assertEqual(extra_report["counts"]["extra_attachment_key_count"], 1)

            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date="2026-10-02", parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=ODDS_SHA,
                names_path=names,
            )
        self.assertEqual(report["checks"]["same_slate"], False)

    def test_retained_package_binds_prediction_odds_parent_and_attachment(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            actual_odds_sha = write_manifest(odds)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=actual_odds_sha,
                names_path=names,
            )
            report.update({"odds_observation_phase": "EARLY", "odds_observation_season": 2026})
            package_report, package_manifest = retain_sog_attachment_package(
                package_path=root / "retained" / f"run_id={RUN_ID}", integrity=report,
                attachment_path=attached, unmatched_path=unmatched, names_path=names,
            )
            loaded, verified_manifest = verify_sog_integrity_package(package_report, SLATE)
            self.assertEqual(verified_manifest, sha256_file(package_manifest))
            self.assertEqual(loaded["parent_daily_run_id"], RUN_ID)
            self.assertEqual(loaded["prediction_artifact_sha256"], sha256_file(pred))
            self.assertEqual(loaded["odds_observation_manifest_sha256"], actual_odds_sha)

    def test_prediction_sha_and_odds_manifest_mismatch_reject_retained_evidence(self):
        for wrong_binding in ("prediction", "odds"):
            with self.subTest(wrong_binding=wrong_binding), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                pred, names, attached, unmatched, odds = make_fixture(root)
                actual_odds_sha = write_manifest(odds)
                report = audit_sog_attachment(
                    prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                    slate_date=SLATE, parent_daily_run_id=RUN_ID,
                    odds_observation_path=odds,
                    odds_observation_manifest_sha256=("f" * 64 if wrong_binding == "odds"
                                                      else actual_odds_sha),
                    names_path=names,
                )
                report.update({"odds_observation_phase": "EARLY", "odds_observation_season": 2026})
                if wrong_binding == "prediction":
                    report["prediction_artifact_sha256"] = "f" * 64
                _, _ = retain_sog_attachment_package(
                    package_path=root / "retained", integrity=report,
                    attachment_path=attached, unmatched_path=unmatched, names_path=names,
                )
                with self.assertRaises(ValueError):
                    verify_sog_integrity_package(
                        root / "retained" / "sog_attachment_integrity.json", SLATE)

    def test_parent_run_binding_rejects_wrong_odds_observation_parent(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            summary_path = odds / "observation_summary.json"
            summary = json.loads(summary_path.read_text())
            summary["parent_daily_run_id"] = "other-run"
            summary_path.write_text(json.dumps(summary) + "\n")
            actual_odds_sha = write_manifest(odds)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds,
                odds_observation_manifest_sha256=actual_odds_sha, names_path=names,
            )
            report.update({"odds_observation_phase": "EARLY", "odds_observation_season": 2026})
            report_path, _ = retain_sog_attachment_package(
                package_path=root / "retained", integrity=report,
                attachment_path=attached, unmatched_path=unmatched, names_path=names,
            )
            with self.assertRaises(ValueError):
                verify_sog_integrity_package(report_path, SLATE)

    def test_reconstructed_coverage_flows_to_summary_and_does_not_change_grades(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pred, names, attached, unmatched, odds = make_fixture(root)
            receipt_dir = root / "artifacts/operational/nhl/daily_runs" / f"run_id={RUN_ID}"
            receipt_dir.mkdir(parents=True)
            receipt = {"slate_date": SLATE, "parent_daily_run_id": RUN_ID,
                       "final_classification": "READY"}
            (receipt_dir / "parent_receipt.json").write_text(json.dumps(receipt) + "\n")
            (receipt_dir / "RUN_COMPLETE.json").write_text(json.dumps({
                "parent_daily_run_id": RUN_ID, "final_classification": "READY"}) + "\n")
            receipt_sha = write_manifest(receipt_dir)
            odds_sha = write_manifest(odds)
            report = audit_sog_attachment(
                prediction_path=pred, attachment_path=attached, unmatched_path=unmatched,
                slate_date=SLATE, parent_daily_run_id=RUN_ID,
                odds_observation_path=odds, odds_observation_manifest_sha256=odds_sha,
                names_path=names, reconstructed=True,
                source_daily_receipt_manifest_sha256=receipt_sha,
            )
            report.update({
                "odds_observation_phase": "EARLY", "odds_observation_season": 2026,
                "source_daily_receipt_path": str(receipt_dir / "parent_receipt.json"),
                "reconstruction_identity": "c" * 64,
                "reconstruction_contract": "NHL_SOG_ATTACHMENT_RECONSTRUCTION_V2",
            })
            package = (root / "artifacts/operational/nhl/sog_market_coverage_reconstructions"
                       / "season=2026" / f"slate_date={SLATE}" / "reconstruction=fixture")
            package_report, _ = retain_sog_attachment_package(
                package_path=package, integrity=report, attachment_path=attached,
                unmatched_path=unmatched, names_path=names,
            )
            grades = {"sog": pd.DataFrame({"prediction_correct": [True, False]})}
            coverage = discover_daily_market_coverage(
                slate_date=SLATE, grades=grades,
                daily_run_root=root / "artifacts/operational/nhl/daily_runs",
                integrity_archive_root=root / "empty_archive",
            )
            summary = summarize_frames(
                slate_date=SLATE, games=1, phase="REGULAR_SEASON",
                reconciliation_status="REUSED", package_identity="fixture",
                grades=grades, source_artifacts={}, market_coverage=coverage,
            )
            self.assertEqual(summary["models"]["sog"]["market_coverage"]["status"], "AVAILABLE")
            self.assertEqual(summary["models"]["sog"]["market_coverage"]["matched"], 2)
            self.assertEqual(summary["models"]["sog"]["market_coverage"]["unmatched"], 2)
            self.assertFalse(summary["models"]["sog"]["market_coverage"]["affects_grading_denominator"])


if __name__ == "__main__":
    unittest.main()
