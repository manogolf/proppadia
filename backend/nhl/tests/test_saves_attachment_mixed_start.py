from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.attachment_integrity import (
    AttachmentIntegrityError,
    canonical_attachment_keys,
    prediction_rows,
    validate_attachment_frame,
)
from backend.nhl.daily_capture import sha256_file
from backend.nhl.scripts import build_saves_with_market as saves


ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "nhldaily_20261010T194717289295Z_170f1044"
PRED = ROOT / "backend/nhl/data/processed/daily_runs" / RUN_ID / "saves_predictions.csv"
NAMES = ROOT / "backend/nhl/exports/daily/names/names_2026-10-10.csv"
ODDS_DIR = ROOT / (
    "artifacts/operational/nhl/odds_observations/season=2026/"
    "slate_date=2026-10-10/"
    "observation=20261010T195002.114873Z_f95e92436bb0c09a"
)
PRED_SHA256 = "08522b88589b0adc8e25b387c6e7dfdd33c657ef2d1f57f105f675327df97c94"
ODDS_SHA256 = "84ee05d3fd7a9c1426b982fb96a93b05772d34550a0f1d94d6adb0751ddfc3c2"
SLATE_GAME_IDS = list(range(2026020070, 2026020084))
STARTED_GAME_IDS = {2026020070, 2026020071}
requires_retained_oct10 = unittest.skipUnless(
    PRED.is_file() and NAMES.is_file() and ODDS_DIR.is_dir(),
    "retained 2026-10-10 Saves prediction run and odds observation are local artifacts")


class SavesAttachmentMixedStartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        out_dir = Path(self.temp.name)
        self.out = out_dir / "saves_with_market.csv"
        self.unmatched = out_dir / "unmatched_saves.csv"
        self.ambiguous = out_dir / "ambiguous_saves_alias_matches.csv"
        self.report = out_dir / "saves_attachment_integrity.json"

    def run_builder(
        self, *, prediction_sha: str = PRED_SHA256,
        odds_sha: str = ODDS_SHA256, parent_run_id: str = RUN_ID,
    ) -> None:
        args = [
            "build_saves_with_market.py", "--pred", str(PRED),
            "--names", str(NAMES), "--out", str(self.out),
            "--unmatched", str(self.unmatched), "--ambiguous", str(self.ambiguous),
            "--integrity-report", str(self.report), "--strict-current-run",
            "--parent-run-id", parent_run_id, "--expected-pred-sha256", prediction_sha,
            "--odds-json", str(ODDS_DIR / "raw_response.json"),
            "--odds-observation-dir", str(ODDS_DIR),
            "--expected-odds-manifest-sha256", odds_sha,
            "--odds-season", "2026", "--odds-phase", "EARLY",
        ]
        with patch.dict(os.environ, {"SLATE_DATE": "2026-10-10"}), patch(
            "sys.argv", args
        ):
            saves.main()

    @requires_retained_oct10
    def test_retained_eligible_subset_attaches_without_started_game_predictions(self):
        self.assertEqual(sha256_file(PRED), PRED_SHA256)
        self.assertEqual(sha256_file(ODDS_DIR / "SHA256SUMS"), ODDS_SHA256)
        self.run_builder()

        predictions = pd.read_csv(PRED)
        attachment = pd.read_csv(self.out)
        report = json.loads(self.report.read_text())
        keys = canonical_attachment_keys(attachment)
        prediction_keys = canonical_attachment_keys(
            prediction_rows(PRED, lane="saves"))

        self.assertEqual(len(predictions), 53)
        self.assertEqual(predictions[["game_id", "player_id"]].drop_duplicates().shape[0], 53)
        self.assertEqual(predictions.filter(regex=r"^p_over_").shape[1], 13)
        self.assertEqual(len(prediction_keys), 689)
        self.assertEqual(len(set(prediction_keys)), 689)
        self.assertEqual(len(attachment), 689)
        self.assertEqual(len(set(keys)), 689)
        self.assertEqual(set(predictions.game_id.astype(int)), set(range(2026020072, 2026020084)))
        self.assertFalse(set(predictions.game_id.astype(int)) & STARTED_GAME_IDS)
        self.assertEqual(report["odds_observation_canonical_game_ids"], SLATE_GAME_IDS)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["counts"]["matched_count"], int(attachment.attachment_status.eq("MATCHED").sum()))
        self.assertEqual(report["counts"]["unmatched_count"], int(attachment.attachment_status.eq("UNMATCHED").sum()))
        self.assertEqual(report["counts"]["ambiguous_count"], int(attachment.attachment_status.eq("AMBIGUOUS_ALIAS_MATCH").sum()))
        self.assertEqual(report["counts"]["duplicate_attachment_key_count"], 0)
        self.assertEqual(report["counts"]["missing_prediction_key_count"], 0)
        self.assertEqual(report["counts"]["extra_attachment_key_count"], 0)
        unmatched = pd.read_csv(self.unmatched)
        self.assertEqual(len(unmatched), report["counts"]["unmatched_count"]
                         + report["counts"]["ambiguous_count"])
        self.assertGreater(len(unmatched), 0)
        self.assertEqual(sha256_file(PRED), PRED_SHA256)
        self.assertEqual(sha256_file(ODDS_DIR / "SHA256SUMS"), ODDS_SHA256)

    @requires_retained_oct10
    def test_prediction_hash_mismatch_is_rejected(self):
        with self.assertRaisesRegex(AttachmentIntegrityError, "PREDICTION_ARTIFACT_HASH_MISMATCH"):
            self.run_builder(prediction_sha="0" * 64)

    @requires_retained_oct10
    def test_odds_manifest_mismatch_is_rejected(self):
        with self.assertRaisesRegex(AttachmentIntegrityError, "ODDS_OBSERVATION_MANIFEST_HASH_MISMATCH"):
            self.run_builder(odds_sha="0" * 64)

    @requires_retained_oct10
    def test_parent_run_mismatch_is_rejected(self):
        with self.assertRaisesRegex(AttachmentIntegrityError, "PREDICTION_PARENT_RUN_ID_MISMATCH"):
            self.run_builder(parent_run_id="substituted-run")

    def test_equivalent_aliases_collapse_to_one_canonical_match(self):
        predictions = pd.DataFrame({
            "full_name": ["Alex Lyon"], "line": [18.5],
        }, index=[0])
        aliases = pd.DataFrame([
            {"normalized_alias": "alex lyon", "alias_value_prediction": "Alex Lyon",
             "alias_type_prediction": "AUTHORITATIVE_FULL_NAME", "alias_rank_prediction": 0,
             "provider_player_identity": "alex lyon", "provider_player_name": "Alex Lyon",
             "market_identity": "one-market", "line_str": "18.5", "price_over": -110,
             "price_under": None},
            {"normalized_alias": "a lyon", "alias_value_prediction": "a lyon",
             "alias_type_prediction": "INITIAL_LAST", "alias_rank_prediction": 2,
             "provider_player_identity": "alex lyon", "provider_player_name": "Alex Lyon",
             "market_identity": "one-market", "line_str": "18.5", "price_over": -110,
             "price_under": None},
        ])
        candidates = pd.DataFrame([
            {"prediction_index": 0, **row}
            for row in aliases.to_dict("records")
        ])
        result, ambiguous = saves.reduce_match_candidates(predictions, candidates)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "attachment_status"], "MATCHED")
        self.assertTrue(ambiguous.empty)

    def test_ambiguous_alias_candidates_are_quarantined(self):
        predictions = pd.DataFrame({"full_name": ["Alex Lyon"], "line": [18.5]}, index=[0])
        candidates = pd.DataFrame([
            {"prediction_index": 0, "market_identity": market, "price_over": price,
             "price_under": None, "alias_rank_prediction": 0,
             "alias_type_prediction": "AUTHORITATIVE_FULL_NAME",
             "alias_value_prediction": "Alex Lyon", "provider_player_identity": identity}
            for market, price, identity in (
                ("market-a", -110, "alex lyon"), ("market-b", 105, "andrew lyon"))
        ])
        result, ambiguous = saves.reduce_match_candidates(predictions, candidates)
        self.assertEqual(result.loc[0, "attachment_status"], "AMBIGUOUS_ALIAS_MATCH")
        self.assertTrue(pd.isna(result.loc[0, "price_over"]))
        self.assertEqual(len(ambiguous), 2)

    def test_integrity_accepts_a_matched_under_only_market(self):
        prediction = pd.DataFrame({
            "game_date": ["2026-10-10"], "game_id": [2026020072],
            "player_id": [8470001], "line": [18.5],
        })
        attachment = prediction.assign(
            attachment_status="MATCHED", price_over=pd.NA, price_under=-104,
            parent_daily_run_id=RUN_ID, prediction_artifact_sha256=PRED_SHA256,
            odds_observation_manifest_sha256=ODDS_SHA256,
        )
        result = validate_attachment_frame(
            prediction_frame=prediction, attachment_frame=attachment,
            expected_parent_daily_run_id=RUN_ID,
            expected_prediction_sha256=PRED_SHA256,
            expected_odds_manifest_sha256=ODDS_SHA256,
        )
        self.assertTrue(all(result["checks"].values()))
        self.assertEqual(result["counts"]["matched_count"], 1)

    def test_duplicate_missing_and_extra_keys_still_fail_integrity(self):
        prediction = pd.DataFrame({
            "game_date": ["2026-10-10", "2026-10-10"],
            "game_id": [2026020072, 2026020072],
            "player_id": [8470001, 8470002], "line": [18.5, 18.5],
        })
        attachment = prediction.assign(attachment_status="UNMATCHED")
        with self.assertRaisesRegex(AttachmentIntegrityError, "ATTACHMENT_INTEGRITY_FAILED"):
            validate_attachment_frame(
                prediction_frame=prediction,
                attachment_frame=pd.concat([attachment, attachment.iloc[[0]]], ignore_index=True),
            )
        with self.assertRaisesRegex(AttachmentIntegrityError, "ATTACHMENT_INTEGRITY_FAILED"):
            validate_attachment_frame(
                prediction_frame=prediction, attachment_frame=attachment.iloc[[0]].copy())
        extra = attachment.copy()
        extra.loc[1, "player_id"] = 8470099
        with self.assertRaisesRegex(AttachmentIntegrityError, "ATTACHMENT_INTEGRITY_FAILED"):
            validate_attachment_frame(prediction_frame=prediction, attachment_frame=extra)


class SavesMiddayAttachmentRegressionTests(unittest.TestCase):
    """Offline regression for the retained Oct. 11 midday observation."""

    run_id = "nhldaily_20261011T195547605240Z_932ee4ee"
    prediction_path = ROOT / "backend/nhl/data/processed/daily_runs" / run_id / "saves_predictions.csv"
    odds_dir = ROOT / (
        "artifacts/operational/nhl/odds_observations/season=2026/"
        "slate_date=2026-10-11/observation=20261011T195856.810055Z_d0b3ebafeeddccfa"
    )
    prediction_sha = "cb23dc21b4f5e62d233018caca60df4473409e7f60a8eb2814cea9af9e891957"
    odds_sha = "88f43db1700a700abc71017766227dac16511c855acc3baf4ff59a537df83650"
    full_game_set_hash = "9e6df258f6f50edb36b077c118385acdcaa8b458cf43372cbb2d7ad6e0efdcf0"
    game_ids = {2026020084, 2026020085, 2026020086}

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def replay(self, output_dir: Path) -> tuple[pd.DataFrame, dict]:
        output_dir.mkdir(parents=True)
        out = output_dir / "saves_with_market.csv"
        unmatched = output_dir / "unmatched_saves.csv"
        ambiguous = output_dir / "ambiguous_saves_alias_matches.csv"
        report_path = output_dir / "saves_attachment_integrity.json"
        args = [
            "build_saves_with_market.py", "--pred", str(self.prediction_path),
            "--names", str(ROOT / "backend/nhl/exports/daily/names/names_2026-10-11.csv"),
            "--out", str(out), "--unmatched", str(unmatched),
            "--ambiguous", str(ambiguous), "--integrity-report", str(report_path),
            "--strict-current-run", "--parent-run-id", self.run_id,
            "--expected-pred-sha256", self.prediction_sha,
            "--odds-json", str(self.odds_dir / "raw_response.json"),
            "--odds-observation-dir", str(self.odds_dir),
            "--expected-odds-manifest-sha256", self.odds_sha,
            "--odds-season", "2026", "--odds-phase", "EARLY",
        ]
        with patch.dict(os.environ, {"SLATE_DATE": "2026-10-11"}), patch("sys.argv", args):
            saves.main()
        return pd.read_csv(out), json.loads(report_path.read_text())

    @unittest.skipUnless(prediction_path.is_file() and odds_dir.is_dir(),
                         "retained 2026-10-11 Saves prediction run and odds observation are local artifacts")
    def test_exact_midday_replay_preserves_keys_and_is_deterministic(self):
        self.assertEqual(sha256_file(self.prediction_path), self.prediction_sha)
        self.assertEqual(sha256_file(self.odds_dir / "SHA256SUMS"), self.odds_sha)
        first, report = self.replay(Path(self.temp.name) / "first")
        second, second_report = self.replay(Path(self.temp.name) / "second")
        predictions = prediction_rows(self.prediction_path, lane="saves")
        self.assertEqual(len(predictions), 182)
        self.assertEqual(len(set(canonical_attachment_keys(predictions))), 182)
        self.assertEqual(len(first), 182)
        self.assertEqual(len(set(canonical_attachment_keys(first))), 182)
        self.assertEqual(set(first.game_id.astype(int)), self.game_ids)
        self.assertEqual(set(first.parent_daily_run_id), {self.run_id})
        self.assertEqual(set(first.prediction_artifact_sha256), {self.prediction_sha})
        self.assertEqual(set(first.odds_observation_manifest_sha256), {self.odds_sha})
        wide_predictions = pd.read_csv(self.prediction_path)
        self.assertEqual(set(wide_predictions.canonical_game_set_hash),
                         {self.full_game_set_hash})
        self.assertEqual(report["odds_observation_canonical_game_ids"],
                         sorted(self.game_ids))
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["counts"]["matched_count"], 13)
        self.assertEqual(report["counts"]["unmatched_count"], 169)
        self.assertEqual(report["counts"]["ambiguous_count"], 0)
        self.assertEqual(report["counts"]["duplicate_prediction_key_count"], 0)
        self.assertEqual(report["counts"]["missing_prediction_key_count"], 0)
        self.assertEqual(report["counts"]["extra_attachment_key_count"], 0)
        self.assertEqual(sha256_file(Path(self.temp.name) / "first/saves_with_market.csv"),
                         sha256_file(Path(self.temp.name) / "second/saves_with_market.csv"))
        self.assertEqual(report["counts"], second_report["counts"])


if __name__ == "__main__":
    unittest.main()
