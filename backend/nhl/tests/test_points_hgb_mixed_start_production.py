from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.points_hgb_shadow import build_production_prediction_artifact
from backend.nhl.points_hgb_promotion import assert_hgb_promotion_ready, selected_production_authority
from backend.nhl.attachment_integrity import prediction_rows


PARENT_RUN = "nhldaily_20261010T191633346908Z_3a7fc5ed"
SLATE_DATE = "2026-10-10"
CUTOFF = "2026-10-10T19:19:17.449144Z"
CANONICAL_IDS = list(range(2026020070, 2026020084))
ELIGIBLE_IDS = CANONICAL_IDS[1:]
EXCLUDED_IDS = CANONICAL_IDS[:1]


def _capture(tmp_path: Path, game_ids: list[int], *, phoenix: bool = True,
             started_ids: list[int] | None = None) -> Path:
    capture = tmp_path / "capture"
    capture.mkdir()
    started_ids = EXCLUDED_IDS if started_ids is None else started_ids
    starts = {game_id: ("2026-10-10T19:00:00Z" if game_id in started_ids
                        else "2026-10-10T20:00:00Z") for game_id in CANONICAL_IDS}
    # Match the live retry's 631 eligible HGB player-game identities.
    counts = {game_id: 48 for game_id in ELIGIBLE_IDS}
    for game_id in ELIGIBLE_IDS[:7]:
        counts[game_id] += 1
    rows = []
    player_id = 100000
    for game_id in game_ids:
        for _ in range(counts.get(game_id, 1)):
            rows.append({
                "game_id": game_id, "player_id": player_id,
                "game_date": SLATE_DATE, "game_start_utc": starts[game_id],
                "expected_points": 0.7,
                "prob_over_0_5": 0.55, "prob_over_1_5": 0.24,
                "prob_over_2_5": 0.08,
            })
            player_id += 1
    frame = pd.DataFrame(rows)
    frame.to_csv(capture / "features.csv", index=False)
    frame[["game_id", "player_id", "expected_points", "prob_over_0_5",
           "prob_over_1_5", "prob_over_2_5"]].to_csv(capture / "predictions.csv", index=False)
    if phoenix:
        phoenix_rows = []
        for row in rows:
            for line, probability in ((0.5, 0.6), (1.5, 0.3), (2.5, 0.1)):
                phoenix_rows.append({"game_id": row["game_id"], "player_id": row["player_id"],
                                     "line": line, "prob_over": probability})
        pd.DataFrame(phoenix_rows).to_csv(capture / "phoenix_control_predictions.csv", index=False)
    phoenix_hash = (hashlib.sha256((capture / "phoenix_control_predictions.csv").read_bytes()).hexdigest()
                    if phoenix else None)
    prediction_hash = hashlib.sha256((capture / "predictions.csv").read_bytes()).hexdigest()
    (capture / "receipt.json").write_text(json.dumps({
        "parent_run_id": PARENT_RUN,
        "identity_count": len(frame),
        "prediction_sha256": prediction_hash,
        "canonical_game_ids": CANONICAL_IDS,
        "canonical_game_set_hash": canonical_game_set_hash(CANONICAL_IDS),
        "eligible_pregame_game_ids": sorted(frame.game_id.unique().tolist()),
        "started_excluded_game_ids": sorted(started_ids),
        "phoenix_control": ({"status": "BOUND", "parent_run_id": PARENT_RUN,
                             "prediction_sha256": phoenix_hash}
                            if phoenix else {"status": "UNAVAILABLE_PHOENIX_SHADOW_FAILED"}),
    }))
    return capture


def _canonical_games(started_ids=None):
    started_ids = EXCLUDED_IDS if started_ids is None else started_ids
    return [SimpleNamespace(
        game_id=game_id,
        start_time_utc=("2026-10-10T19:00:00Z" if game_id in started_ids
                        else "2026-10-10T20:00:00Z"),
        home_team_id=1, away_team_id=2,
    ) for game_id in CANONICAL_IDS]


def _build(capture: Path, output: Path, *, started_ids=None):
    return build_production_prediction_artifact(
        capture_path=capture, output_path=output, canonical_games=_canonical_games(started_ids),
        slate_date=SLATE_DATE, parent_run_id=PARENT_RUN, feature_cutoff_utc=CUTOFF,
        canonical_game_set_sha256=canonical_game_set_hash(CANONICAL_IDS),
    )


class HgbMixedStartProductionTests(unittest.TestCase):
    def make_capture(self, game_ids, *, phoenix=True):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return _capture(Path(tmp.name), game_ids, phoenix=phoenix), Path(tmp.name)

    def test_hgb_production_accepts_631_identities_on_13_of_14_canonical_games(self):
        capture, root = self.make_capture(ELIGIBLE_IDS)
        identity = _build(capture, root / "points.csv")
        output = pd.read_csv(identity["path"])
        self.assertEqual(identity["canonical_game_count"], 14)
        self.assertEqual(identity["identity_count"], 631)
        self.assertEqual(identity["row_count"], 631 * 3)
        self.assertEqual(identity["eligible_pregame_game_ids"], ELIGIBLE_IDS)
        self.assertEqual(identity["started_excluded_game_ids"], EXCLUDED_IDS)
        self.assertEqual(set(output.game_id.unique()), set(ELIGIBLE_IDS))
        self.assertEqual(set(output.line.astype(float)), {0.5, 1.5, 2.5})
        attachment_population = prediction_rows(identity["path"], lane="points")
        self.assertEqual(len(attachment_population), 631 * 3)

    def test_started_game_in_capture_is_rejected(self):
        capture, root = self.make_capture([*EXCLUDED_IDS, *ELIGIBLE_IDS])
        with self.assertRaisesRegex(ValueError, "STARTED_GAME_PRESENT_IN_HGB_PRODUCTION_OUTPUT"):
            _build(capture, root / "points.csv")

    def test_missing_eligible_game_population_is_rejected(self):
        capture, root = self.make_capture(ELIGIBLE_IDS[1:])
        with self.assertRaisesRegex(ValueError, "HGB_PRODUCTION_ELIGIBLE_GAME_COVERAGE_MISMATCH"):
            _build(capture, root / "points.csv")

    def test_all_pregame_behavior_is_unchanged_and_lineage_is_full_slate(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        capture = _capture(root, CANONICAL_IDS, started_ids=[])
        identity = _build(capture, root / "points.csv", started_ids=[])
        self.assertEqual(identity["canonical_game_count"], 14)
        self.assertEqual(identity["canonical_game_ids"], CANONICAL_IDS)
        self.assertEqual(identity["eligible_pregame_game_ids"], CANONICAL_IDS)
        self.assertEqual(identity["started_excluded_game_ids"], [])
        self.assertEqual(identity["identity_count"], 632)

    def test_phoenix_control_uses_exact_same_eligible_player_game_set(self):
        capture, root = self.make_capture(ELIGIBLE_IDS)
        phoenix_path = capture / "phoenix_control_predictions.csv"
        phoenix = pd.read_csv(phoenix_path)
        first_key = phoenix[["game_id", "player_id"]].iloc[0]
        phoenix = phoenix.loc[~((phoenix.game_id == first_key.game_id)
                                & (phoenix.player_id == first_key.player_id))]
        phoenix.to_csv(phoenix_path, index=False)
        receipt_path = capture / "receipt.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["phoenix_control"]["prediction_sha256"] = hashlib.sha256(
            phoenix_path.read_bytes()).hexdigest()
        receipt_path.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(ValueError, "HGB_PRODUCTION_PHOENIX_ELIGIBLE_IDENTITY_MISMATCH"):
            _build(capture, root / "points.csv")

    def test_empty_eligible_slate_fails_with_clean_no_pregame_reason(self):
        games = [SimpleNamespace(game_id=gid, start_time_utc="2026-10-10T19:00:00Z",
                                 home_team_id=1, away_team_id=2) for gid in CANONICAL_IDS]
        with self.assertRaisesRegex(RuntimeError, "SCORING_INPUT_NO_PREGAME_GAMES_REMAIN"):
            from backend.nhl.prediction_lineage import prepare_scoring_input
            with tempfile.TemporaryDirectory() as tmp:
                source = Path(tmp) / "features.csv"
                pd.DataFrame([{"game_id": CANONICAL_IDS[0], "player_id": 1,
                               "game_date": SLATE_DATE}]).to_csv(source, index=False)
                prepare_scoring_input(
                    source_path=source, output_path=Path(tmp) / "scoring.csv",
                    canonical_games=games, slate=SLATE_DATE, parent_daily_run_id=PARENT_RUN,
                    feature_input_cutoff_utc=CUTOFF,
                    expected_game_set_hash=canonical_game_set_hash(CANONICAL_IDS),
                )

    def test_current_hgb_authority_and_all_promotion_gates_remain_ready(self):
        authority = selected_production_authority()
        self.assertEqual(authority["authority_version"], 2)
        self.assertEqual(authority["production_authority"], "NHL_POINTS_COUNT_HGB_V1")
        self.assertEqual(authority["shadow_authorities"], ["phoenix_v2"])
        promotion = assert_hgb_promotion_ready()
        self.assertEqual(promotion["classification"], "READY_FOR_HGB_PRODUCTION_PROMOTION")
        self.assertTrue(all(promotion["gates"][gate]["status"] == "PASS" for gate in "ABCDEFGHIJ"))


if __name__ == "__main__":
    unittest.main()
