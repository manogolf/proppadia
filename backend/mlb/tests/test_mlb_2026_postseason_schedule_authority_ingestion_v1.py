from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import unittest
from unittest.mock import patch

from backend.mlb.scripts import build_mlb_versioned_file_phase_authority_v1 as builder
from backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v3 import (
    CLOSE, CLOSE_SHA, PKS, ROUNDS, SOURCE_SHA, validate_child,
)
from backend.mlb.public_game_predictions.durable_store_v1 import append_prediction_rows, append_outcome_grade
from backend.mlb.public_game_predictions.phase_gating_v1 import MoneylinePhaseGateError, classify_moneyline_row
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT, sha256_file

SOURCE = REPO_ROOT / (
    "artifacts/ops/mlb_public_game_moneyline_history_schedules/2026-10-01/"
    "20261001T123006472388Z_2026-08-05_2026-10-01_"
    "25c70d96c0f7b52c46ae06082e10d2ebdd1ac2148be79f56de540670b9244fb0.json"
)


def _build_from_payload(tmp_path: Path, payload: dict, selected=(849841,)):
    tmp_path = Path(tempfile.mkdtemp(prefix=".phase-ingestion-test-", dir=REPO_ROOT))
    source = tmp_path / "schedule.json"
    raw = json.dumps(payload, separators=(",", ":")).encode()
    source.write_bytes(raw)
    source_rel = source.relative_to(REPO_ROOT).as_posix()
    source_set = tmp_path / "source_set.json"
    source_set.write_text(json.dumps({
        "contract_name": builder.SOURCE_SET_CONTRACT_NAME,
        "schema_version": 1,
        "source_set_id": "test-source-set",
        "parent_descriptor_path": "backend/mlb/season_transition/authority_snapshots/v2/descriptor.json",
        "parent_descriptor_sha256": "cde49ebcaf789db1f0a33e512886d64ef1832c356f536ac554e56fd68a16a807",
        "sources": [{"source_path": source_rel, "source_sha256": hashlib.sha256(raw).hexdigest(),
                     "source_bytes": len(raw), "selected_game_pks": list(selected)}],
    }))
    try:
        return builder.build_candidate_snapshot(source_set_path=source_set, output_dir=tmp_path / "candidate")
    finally:
        shutil.rmtree(tmp_path)


class PostseasonScheduleAuthorityIngestionTests(unittest.TestCase):
 def test_candidate_is_exact_append_only_schedule_extension_and_hash_bound(self):
    result = validate_child(allow_candidate=True)
    self.assertEqual(result["new_game_pks"], list(PKS))
    parent = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v2/canonical_game_phase_full_snapshot.jsonl"
    child = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v3/canonical_game_phase_full_snapshot.jsonl"
    self.assertTrue(child.read_bytes().startswith(parent.read_bytes()))
    self.assertEqual(sha256_file(SOURCE), SOURCE_SHA)
    rows = [json.loads(line) for line in child.read_text().splitlines() if line.strip()]
    additions = {row["game_pk"]: row for row in rows[-len(PKS):]}
    self.assertEqual(set(additions), set(PKS))
    self.assertTrue(all(additions[pk]["source_round"] == ROUNDS[pk] for pk in PKS))
    self.assertTrue(all(additions[pk]["season_phase"] == "POSTSEASON" for pk in PKS))
    self.assertEqual(sha256_file(CLOSE), CLOSE_SHA)
    decisions = [classify_moneyline_row({"game_id": pk, "game_date": "2026-10-01"}) for pk in PKS]
    self.assertTrue(all(item.evaluation_partition == "POSTSEASON" for item in decisions))


 def test_selected_duplicate_identity_fails_closed(self):
    game = {"gamePk": 849841, "gameType": "F", "season": "2026",
            "gameDate": "2026-09-30T18:00:00Z", "officialDate": "2026-09-30",
            "seriesDescription": "NL Wild Card Series",
            "status": {"abstractGameState": "Final", "codedGameState": "F",
                       "detailedState": "Final", "statusCode": "F"}}
    with self.assertRaisesRegex(builder.SnapshotBuildError, "SELECTED_GAME_PK_NOT_UNIQUE_OR_MISSING"):
        _build_from_payload(Path("unused"), {"dates": [{"games": [game, game]}]})


 def test_missing_status_fails_closed(self):
    game = {"gamePk": 849841, "gameType": "F", "season": "2026",
            "gameDate": "2026-09-30T18:00:00Z", "officialDate": "2026-09-30",
            "seriesDescription": "NL Wild Card Series",
            "status": {"abstractGameState": "Final", "codedGameState": "F",
                       "detailedState": "Final", "statusCode": "F"}}
    game["status"] = None
    with self.assertRaisesRegex(builder.SnapshotBuildError, "SCHEDULE_STATUS_INCOMPLETE"):
        _build_from_payload(Path("unused"), {"dates": [{"games": [game]}]})

 def test_conflicting_status_fails_closed(self):
    game = {"gamePk": 849841, "gameType": "F", "season": "2026",
            "gameDate": "2026-09-30T18:00:00Z", "officialDate": "2026-09-30",
            "seriesDescription": "NL Wild Card Series",
            "status": {"abstractGameState": "Final", "codedGameState": "S",
                       "detailedState": "Final", "statusCode": "S"}}
    with self.assertRaisesRegex(builder.SnapshotBuildError, "SCHEDULE_STATUS_CONFLICT"):
        _build_from_payload(Path("unused"), {"dates": [{"games": [game]}]})

 def test_conflicting_identity_fields_fail_closed(self):
    game = {"gamePk": 849841, "gameType": "F", "season": "2026",
            "gameDate": "2026-09-30T18:00:00Z", "officialDate": "2026-09-30",
            "seriesDescription": "NL Wild Card Series",
            "status": {"abstractGameState": "Final", "codedGameState": "F",
                       "detailedState": "Final", "statusCode": "F"},
            "gameData": {"game": {"pk": 849842}}}
    with self.assertRaisesRegex(builder.SnapshotBuildError, "SCHEDULE_GAME_PK_CONFLICT"):
        _build_from_payload(Path("unused"), {"dates": [{"games": [game]}]})


 def test_schedule_source_hash_mismatch_fails_closed(self):
    source_set = json.loads((REPO_ROOT / "docs/contracts/mlb_2026_postseason_schedule_authority_ingestion_v1/source_set.json").read_text())
    source_set["sources"][0]["source_sha256"] = "0" * 64
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "source_set.json"
        path.write_text(json.dumps(source_set))
        with self.assertRaisesRegex(builder.SnapshotBuildError, "RETAINED_EVIDENCE_HASH_MISMATCH"):
            builder._source_set(path, root=REPO_ROOT)

 def test_missing_schedule_evidence_fails_closed(self):
    source_set = json.loads((REPO_ROOT / "docs/contracts/mlb_2026_postseason_schedule_authority_ingestion_v1/source_set.json").read_text())
    source_set["sources"][0]["source_path"] = "artifacts/ops/missing_retained_schedule.json"
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "source_set.json"
        path.write_text(json.dumps(source_set))
        with self.assertRaisesRegex(builder.SnapshotBuildError, "RETAINED_EVIDENCE_MISSING"):
            builder._source_set(path, root=REPO_ROOT)

 def test_prediction_and_grading_writes_require_exact_game_authority(self):
    with patch("backend.mlb.public_game_predictions.durable_store_v1.pg_connect") as connect:
        with self.assertRaises(MoneylinePhaseGateError):
            append_prediction_rows([{"admission_status": "ADMITTED_SHADOW", "game_id": 849840,
                                     "game_date": "2026-10-01"}])
        with self.assertRaises(MoneylinePhaseGateError):
            append_outcome_grade({"official_status": "Final", "game_id": 849840,
                                  "game_date": "2026-10-01"})
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
