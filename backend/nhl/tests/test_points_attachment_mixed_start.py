from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.attachment_integrity import AttachmentIntegrityError, validate_odds_observation
from backend.nhl.daily_capture import _manifest, canonical_game_set_hash, verify_package
from backend.nhl.scripts.build_points_with_market import _prediction_canonical_game_set_hash


class PointsAttachmentMixedStartTests(unittest.TestCase):
    def make_observation(self, root: Path, game_ids: list[int]) -> tuple[Path, str, str]:
        observation = root / "observation"
        observation.mkdir()
        (observation / "raw_response.json").write_text("{}")
        (observation / "observation_summary.json").write_text(json.dumps({
            "parent_daily_run_id": "mixed-run", "slate_date": "2026-10-10",
            "season": 2026, "phase": "EARLY", "classification": "CAPTURED_NONEMPTY",
            "canonical_game_ids": game_ids,
            "canonical_game_set_hash": canonical_game_set_hash(game_ids),
        }))
        (observation / "RUN_COMPLETE.json").write_text("{}")
        _manifest(observation)
        return observation, str(observation / "raw_response.json"), verify_package(observation)

    def test_prediction_subset_uses_full_canonical_hash_and_passes_observation_binding(self):
        canonical = [2026020070, 2026020071, *range(2026020072, 2026020084)]
        eligible = list(range(2026020072, 2026020084))
        frame = pd.DataFrame({
            "game_id": eligible,
            "canonical_game_set_hash": [canonical_game_set_hash(canonical)] * len(eligible),
        })
        self.assertEqual(_prediction_canonical_game_set_hash(frame),
                         canonical_game_set_hash(canonical))
        with tempfile.TemporaryDirectory() as tmp:
            observation, odds, manifest_hash = self.make_observation(Path(tmp), canonical)
            result = validate_odds_observation(
                observation_dir=observation, odds_json=Path(odds),
                expected_manifest_sha256=manifest_hash,
                expected_parent_daily_run_id="mixed-run", expected_slate_date="2026-10-10",
                expected_season=2026, expected_phase="EARLY",
                expected_game_set_hash=_prediction_canonical_game_set_hash(frame),
                expected_prediction_game_ids=frame.game_id.tolist(),
            )
            self.assertEqual(result["odds_observation_canonical_game_ids"], canonical)

    def test_prediction_game_outside_observation_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            canonical = [2026020072, 2026020073]
            observation, odds, manifest_hash = self.make_observation(Path(tmp), canonical)
            with self.assertRaisesRegex(AttachmentIntegrityError,
                                        "PREDICTION_GAMES_OUTSIDE_ODDS_OBSERVATION"):
                validate_odds_observation(
                    observation_dir=observation, odds_json=Path(odds),
                    expected_manifest_sha256=manifest_hash,
                    expected_parent_daily_run_id="mixed-run", expected_slate_date="2026-10-10",
                    expected_game_set_hash=canonical_game_set_hash(canonical),
                    expected_prediction_game_ids=[2026020072, 2026020074],
                )

    def test_inconsistent_prediction_canonical_hashes_fail_closed(self):
        frame = pd.DataFrame({"canonical_game_set_hash": ["a", "b"]})
        with self.assertRaisesRegex(AttachmentIntegrityError,
                                    "PREDICTION_CANONICAL_GAME_SET_HASH_INVALID"):
            _prediction_canonical_game_set_hash(frame)


if __name__ == "__main__":
    unittest.main()
