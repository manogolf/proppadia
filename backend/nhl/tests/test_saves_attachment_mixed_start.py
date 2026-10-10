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

    def test_prediction_hash_mismatch_is_rejected(self):
        with self.assertRaisesRegex(AttachmentIntegrityError, "PREDICTION_ARTIFACT_HASH_MISMATCH"):
            self.run_builder(prediction_sha="0" * 64)

    def test_odds_manifest_mismatch_is_rejected(self):
        with self.assertRaisesRegex(AttachmentIntegrityError, "ODDS_OBSERVATION_MANIFEST_HASH_MISMATCH"):
            self.run_builder(odds_sha="0" * 64)

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


if __name__ == "__main__":
    unittest.main()
