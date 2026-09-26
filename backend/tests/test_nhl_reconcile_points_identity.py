import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.prediction_only.core import digest
from backend.nhl.postgame_reconcile.core import _verify_prop_source


GAME_ID = 2026010001
START = "2026-09-25T23:00:00Z"
OBSERVED = "2026-09-25T19:00:00Z"
PROBABILITY_CONSTRUCTION = "EQUAL_WEIGHT_ISOTONIC_EXCEEDANCE_V1"


class PointsIdentityValidatorTests(unittest.TestCase):
    def _write_run(self, root: Path, lane: str, *, points_identity="writer",
                   points_construction=PROBABILITY_CONSTRUCTION) -> Path:
        run_id = f"{lane.lower()}-fixture"
        run = root / f"run_id={run_id}"
        run.mkdir()
        points = lane == "POINTS"
        entity_column = "player_id" if points else "goalie_id"
        entity_id = 8470001 if points else 8470002
        lines = [0.5, 1.5, 2.5] if points else [18.5 + n for n in range(13)]
        rows = []
        for line in lines:
            payload = {"run_id": run_id, "game_id": GAME_ID,
                       entity_column: entity_id, "line": line,
                       "model_version": "fixture-model"}
            if points and points_identity == "writer":
                payload["probability_construction"] = points_construction
            identity = digest(payload)
            row = {
                "run_id": run_id, "game_id": GAME_ID, entity_column: entity_id,
                "line": line, "prob_over": 0.5, "phase": "FINAL_PREGAME",
                "prediction_timestamp_utc": OBSERVED,
                "input_cutoff_timestamp_utc": OBSERVED,
                "model_version": "fixture-model", "prediction_identity": identity,
                "scheduled_start_time_utc": START, "game_type_code": 1,
                "market_qualified": False, "price": None,
            }
            if points:
                row.update({
                    "prediction_eligible": True,
                    "ladder_coherence_decision": "PASS_LADDER_COHERENCE",
                    "probability_construction": points_construction,
                })
            else:
                row.update({
                    "starter_state": "UNKNOWN_NO_AUTHORIZED_PREGAME_STARTER_SOURCE",
                    "selected_starter": False, "prediction_semantics": "CONDITIONAL_ON_START",
                })
            rows.append(row)

        predictions_name = ("immutable_predictions.csv" if points
                            else "immutable_conditional_predictions.csv")
        pd.DataFrame(rows).to_csv(run / predictions_name, index=False)
        pd.DataFrame([{"game_id": GAME_ID, "scheduled_start_time_utc": START}]).to_csv(
            run / "canonical_game_spine.csv", index=False)
        (run / "input_exclusions.csv").write_text("game_id,player_id,reason\n")
        if points:
            (run / "prediction_exclusions.csv").write_text("game_id,player_id,reason\n")
        metadata = {
            "lane": lane, "slate_date": "2026-09-25", "phase": "FINAL_PREGAME",
            "run_id": run_id, "games": 1, "prediction_rows": len(rows),
            "player_games" if points else "goalie_games": 1,
            "observation_timestamp_utc": OBSERVED,
            "actual_write_timestamp_utc": OBSERVED,
        }
        (run / "run_metadata.json").write_text(json.dumps(metadata))
        (run / "RUN_COMPLETE.json").write_text(
            json.dumps({"status": "COMPLETE", "run_id": run_id}))
        entries = []
        for path in sorted(p for p in run.iterdir() if p.is_file() and p.name != "SHA256SUMS"):
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            entries.append(f"{checksum}  {path.name}\n")
        (run / "SHA256SUMS").write_text("".join(entries))
        return run

    def _validate(self, run: Path, lane: str):
        schedule = pd.DataFrame([{
            "game_id": GAME_ID, "scheduled_start_time_utc": START,
            "game_type_code": 1,
        }])
        return _verify_prop_source(run=run, lane=lane,
                                   slate_date="2026-09-25", schedule=schedule)

    def test_points_artifact_with_probability_construction_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self._write_run(Path(tmp), "POINTS")
            result = self._validate(run, "POINTS")
            self.assertEqual(result["status"], "PROSPECTIVE_SOURCE_BOUND")
            self.assertEqual(result["prediction_rows"], 3)

    def test_points_identity_without_or_with_changed_construction_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = self._write_run(root, "POINTS", points_identity="omitted")
            with self.assertRaisesRegex(
                    RuntimeError, "IMMUTABLE_POINTS_PREDICTION_IDENTITY_MISMATCH"):
                self._validate(run, "POINTS")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = self._write_run(root, "POINTS")
            predictions_path = run / "immutable_predictions.csv"
            frame = pd.read_csv(predictions_path)
            frame["probability_construction"] = "CHANGED_CONSTRUCTION"
            frame.to_csv(predictions_path, index=False)
            entries = []
            for path in sorted(p for p in run.iterdir()
                               if p.is_file() and p.name != "SHA256SUMS"):
                entries.append(
                    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
            (run / "SHA256SUMS").write_text("".join(entries))
            with self.assertRaisesRegex(
                    RuntimeError, "IMMUTABLE_POINTS_PREDICTION_IDENTITY_MISMATCH"):
                self._validate(run, "POINTS")

    def test_saves_identity_contract_is_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self._write_run(Path(tmp), "SAVES")
            result = self._validate(run, "SAVES")
            self.assertEqual(result["status"], "PROSPECTIVE_SOURCE_BOUND")
            self.assertEqual(result["prediction_rows"], 13)


if __name__ == "__main__":
    unittest.main()
