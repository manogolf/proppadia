from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl import cli
from backend.nhl.daily_capture import sha256_file, verify_package
from backend.nhl.market_attachment_retention import (
    retain_market_attachment_package,
    verify_market_attachment_package,
)


SLATE = "2026-10-05"
RUN = "nhldaily_retention_fixture"


def manifest(directory: Path) -> str:
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(
        f"{sha256_file(p)}  {p.name}\n" for p in files))
    return sha256_file(directory / "SHA256SUMS")


def fixture(root: Path, lane: str):
    daily = root / "daily"
    daily.mkdir(parents=True)
    prediction = daily / f"{lane}_predictions.csv"
    attached = daily / f"{lane}_with_market.csv"
    unmatched = daily / f"unmatched_{lane}.csv"
    ambiguous = daily / "ambiguous_saves_alias_matches.csv" if lane == "saves" else None
    if lane == "points":
        predictions = pd.DataFrame({
            "game_date": [SLATE, SLATE], "game_id": [100, 100],
            "player_id": [10, 11], "line": [.5, 1.5],
            "prob_over": [.7, .4], "parent_daily_run_id": [RUN, RUN],
        })
        attachment = pd.DataFrame({
            "game_date": [SLATE, SLATE], "game_id": [100, 100],
            "player_id": [10, 11], "line": [.5, 1.5], "p_over": [.7, .4],
            "price_over": [-110, pd.NA],
        })
        unresolved = attachment.iloc[[1]][["game_date", "game_id", "player_id", "line"]]
    else:
        predictions = pd.DataFrame({
            "game_date": [SLATE], "game_id": [100], "player_id": [20],
            "p_over_18_5": [.7], "p_over_19_5": [.5],
            "parent_daily_run_id": [RUN],
        })
        attachment = pd.DataFrame({
            "game_date": [SLATE, SLATE], "game_id": [100, 100],
            "player_id": [20, 20], "line": [18.5, 19.5], "p_over": [.7, .5],
            "price_over": [-110, pd.NA],
            "attachment_status": ["MATCHED", "AMBIGUOUS_ALIAS_MATCH"],
        })
        unresolved = attachment.iloc[[1]][["game_date", "game_id", "player_id", "line"]]
        pd.DataFrame({"player_id": [20], "game_id": [100], "line": [19.5],
                      "alias_reason": ["fixture ambiguity"]}).to_csv(ambiguous, index=False)
    predictions.to_csv(prediction, index=False)
    attachment.to_csv(attached, index=False)
    unresolved.to_csv(unmatched, index=False)

    odds = root / "odds" / "observation=fixture"
    odds.mkdir(parents=True)
    summary = {
        "slate_date": SLATE, "parent_daily_run_id": RUN,
        "invocation_id": "odds_fixture", "observation_timestamp_utc": "2026-10-05T18:00:00Z",
        "canonical_game_set_hash": "game-set-hash",
    }
    (odds / "observation_summary.json").write_text(json.dumps(summary))
    (odds / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
    odds_sha = manifest(odds)

    row_count = 2
    matched, unmatched_count, ambiguous_count = (1, 1, 0) if lane == "points" else (1, 0, 1)
    integrity = {
        "schema_version": "NHL_ATTACHMENT_INTEGRITY_V1", "lane": lane,
        "status": "PASS", "integrity_status": "PASS", "slate_date": SLATE,
        "parent_daily_run_id": RUN, "prediction_artifact_sha256": sha256_file(prediction),
        "prediction_artifact_path": str(prediction.resolve()),
        "attachment_path": str(attached.resolve()), "unmatched_path": str(unmatched.resolve()),
        "odds_observation_path": str(odds.resolve()),
        "odds_observation_manifest_sha256": odds_sha,
        "odds_observation_timestamp_utc": summary["observation_timestamp_utc"],
        "counts": {"prediction_row_count": row_count, "attachment_row_count": row_count,
                   "unique_prediction_key_count": row_count,
                   "unique_attachment_key_count": row_count,
                   "duplicate_prediction_key_count": 0, "duplicate_attachment_key_count": 0,
                   "missing_prediction_key_count": 0, "extra_attachment_key_count": 0,
                   "matched_count": matched, "unmatched_count": unmatched_count,
                   "ambiguous_count": ambiguous_count},
        "checks": {"prediction_keys_unique": True, "attachment_keys_unique": True,
                   "prediction_attachment_key_set_equal": True, "lineage_matches": True,
                   "output_count_equals_prediction_count": True, "statuses_exhaustive": True},
    }
    return prediction, attached, unmatched, ambiguous, odds_sha, integrity


class PointsSavesMarketAttachmentRetentionTests(unittest.TestCase):
    def make_package(self, root: Path, lane: str, run_id: str = RUN,
                     start: str = "2026-10-05T23:00:00Z"):
        prediction, attached, unmatched, ambiguous, odds_sha, integrity = fixture(root, lane)
        integrity["parent_daily_run_id"] = run_id
        frame = pd.read_csv(prediction)
        frame["parent_daily_run_id"] = run_id
        frame.to_csv(prediction, index=False)
        integrity["prediction_artifact_sha256"] = sha256_file(prediction)
        integrity["odds_observation_manifest_sha256"] = odds_sha
        integrity["odds_observation_path"] = str((root / "odds" / "observation=fixture").resolve())
        attached_frame = pd.read_csv(attached)
        attached_frame["parent_daily_run_id"] = run_id
        attached_frame["prediction_artifact_sha256"] = sha256_file(prediction)
        if run_id != RUN:
            obs = root / "odds" / "observation=fixture" / "observation_summary.json"
            data = json.loads(obs.read_text())
            data["parent_daily_run_id"] = run_id
            obs.write_text(json.dumps(data))
            manifest(root / "odds" / "observation=fixture")
            odds_sha = sha256_file(root / "odds" / "observation=fixture" / "SHA256SUMS")
            integrity["odds_observation_manifest_sha256"] = odds_sha
        attached_frame["odds_observation_manifest_sha256"] = odds_sha
        attached_frame.to_csv(attached, index=False)
        pkg = root / f"{lane}_market_attachments" / f"run_id={run_id}"
        report, package_sha = retain_market_attachment_package(
            lane=lane, package_path=pkg, integrity=integrity, prediction_path=prediction,
            attachment_path=attached, unmatched_path=unmatched, ambiguous_path=ambiguous,
            canonical_game_set_sha256="game-set-hash",
            canonical_game_starts_utc={100: start},
        )
        return pkg, report, package_sha, prediction, attached, unmatched, ambiguous, odds_sha

    def test_points_package_is_complete_bound_and_preserves_site_outputs(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pkg, report_path, package_sha, prediction, attached, unmatched, _, odds_sha = self.make_package(root, "points")
            report, verified_sha = verify_market_attachment_package(
                report_path, lane="points", slate_date=SLATE)
            self.assertEqual(package_sha, verified_sha)
            self.assertEqual(verify_package(pkg), package_sha)
            self.assertEqual(set(p.name for p in pkg.iterdir()), {
                "points_attachment_integrity.json", "points_with_market.csv",
                "unmatched_points.csv", "RUN_COMPLETE.json", "SHA256SUMS"})
            self.assertEqual(report["parent_daily_run_id"], RUN)
            self.assertEqual(report["prediction_artifact_sha256"], sha256_file(prediction))
            self.assertEqual(report["odds_observation_manifest_sha256"], odds_sha)
            self.assertEqual(report["exact_proposition_key_count"], 2)
            self.assertEqual(report["counts"]["matched_count"], 1)
            self.assertEqual(report["counts"]["unmatched_count"], 1)
            self.assertEqual(report["game_specific_timing"]["games"]["100"][
                "timing_classification"], "PRESTART_ELIGIBLE")
            self.assertEqual(sha256_file(attached), sha256_file(pkg / "points_with_market.csv"))
            self.assertEqual(sha256_file(unmatched), sha256_file(pkg / "unmatched_points.csv"))

    def test_saves_package_retains_ambiguity_inventory_and_counts(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pkg, report_path, _, _, _, _, _, odds_sha = self.make_package(root, "saves")
            report, _ = verify_market_attachment_package(report_path, lane="saves", slate_date=SLATE)
            self.assertEqual(report["odds_observation_manifest_sha256"], odds_sha)
            self.assertEqual(report["proposition"], "goalie_saves")
            self.assertEqual(report["counts"]["matched_count"], 1)
            self.assertEqual(report["counts"]["ambiguous_count"], 1)
            self.assertTrue((pkg / "ambiguous_saves_alias_matches.csv").is_file())
            self.assertIn("ambiguous_inventory_sha256", report)

    def test_poststart_observation_is_retained_with_explicit_timing_classification(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pkg, report_path, _, *_ = self.make_package(
                root, "points", start="2026-10-05T17:00:00Z")
            report, _ = verify_market_attachment_package(
                report_path, lane="points", slate_date=SLATE)
            self.assertEqual(report["game_specific_timing"]["games"]["100"][
                "timing_classification"], "POSTSTART_INELIGIBLE")

    def test_distinct_intraday_run_ids_get_distinct_create_only_packages(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            first = self.make_package(root / "first", "points", "run-morning")
            second = self.make_package(root / "second", "points", "run-midday")
            self.assertNotEqual(first[0], second[0])
            with self.assertRaises(FileExistsError):
                retain_market_attachment_package(
                    lane="points", package_path=first[0], integrity={"status": "PASS"},
                    prediction_path=first[3], attachment_path=first[4], unmatched_path=first[5])

    def test_receipt_outputs_reference_every_retained_points_and_saves_file(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            for lane, expected in (
                ("points", {"points_attachment_integrity.json", "points_with_market.csv",
                             "unmatched_points.csv", "RUN_COMPLETE.json", "SHA256SUMS"}),
                ("saves", {"saves_attachment_integrity.json", "saves_with_market.csv",
                            "unmatched_saves.csv", "ambiguous_saves_alias_matches.csv",
                            "RUN_COMPLETE.json", "SHA256SUMS"}),
            ):
                pkg, _, package_sha, *_ = self.make_package(root / lane, lane)
                outputs = cli._market_attachment_retention_receipt_outputs(
                    pkg, lane=lane, parent_daily_run_id=RUN, manifest_sha256=package_sha)
                self.assertEqual({Path(o["path"]).name for o in outputs[1:]}, expected)
                self.assertEqual(outputs[0]["manifest_sha256"], package_sha)

    def test_corrupt_completed_package_fails_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            pkg, report, _, *_ = self.make_package(root, "points")
            with (pkg / "points_with_market.csv").open("a") as stream:
                stream.write("tamper\n")
            with self.assertRaises(RuntimeError):
                verify_market_attachment_package(report, lane="points", slate_date=SLATE)


if __name__ == "__main__":
    unittest.main()
