import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.postgame_reconcile.core import (
    publish_reconciliation,
    reconciliation_lock,
    validate_final_slate,
)


SLATE = "2026-09-19"
START = "2026-09-19T23:00:00Z"
PRED = "2026-09-19T20:45:51Z"


def canonical():
    return pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010001,
        "scheduled_start_time_utc": START, "home_team_id": 19, "away_team_id": 25,
        "home_team_code": "STL", "away_team_code": "DAL", "game_type_code": 1,
        "retained_game_state": "FINAL",
    }])


def official(state="FINAL", home=19):
    return pd.DataFrame([{
        "game_id": 2026010001, "game_state": state, "home_team_id": home,
        "away_team_id": 25, "home_score": 3, "away_score": 2,
    }])


def boxscore(saves=25):
    return {2026010001: {"playerByGameStats": {
        "homeTeam": {
            "forwards": [{"playerId": 101, "goals": 1, "assists": 1, "sog": 3, "toi": "18:00"}],
            "defense": [],
            "goalies": [{"playerId": 201, "saves": saves, "shotsAgainst": 27, "toi": "60:00"}],
        },
        "awayTeam": {
            "forwards": [{"playerId": 102, "goals": 0, "assists": 0, "sog": 1, "toi": "12:00"}],
            "defense": [],
            "goalies": [{"playerId": 202, "saves": 20, "shotsAgainst": 23, "toi": "60:00"}],
        },
    }}}


def prediction_tree(root: Path):
    main = root / "constructed/mainline/season=2026/slate_date=2026-09-19/run_type=MIDDAY/state=fixture"
    props = root / "constructed/independent_props"
    main.mkdir(parents=True)
    props.mkdir(parents=True)
    common = {
        "canonical_season": 2026, "slate_date": SLATE, "game_id": 2026010001,
        "prediction_creation_time_utc": PRED, "scheduled_start_time_utc": START,
        "game_type_code": 1,
    }
    pd.DataFrame([{**common, "model_favored_team": "STL", "v2_home_win_probability": .53}]).to_csv(main / "v2_immutable_predictions.csv", index=False)
    pd.DataFrame([{**common, "home_minus_1_5_cover_probability": .30}]).to_csv(main / "puck_line_v1_immutable_predictions.csv", index=False)
    pd.DataFrame([{"game_id": 2026010001, "player_id": 101, "line": .5, "prob_over": .4, "model": "points"}]).to_csv(props / "points_immutable_predictions.csv", index=False)
    pd.DataFrame([
        {"game_id": 2026010001, "goalie_id": 201, "line": 24.5, "prob_over": .5},
        {"game_id": 2026010001, "goalie_id": 202, "line": 24.5, "prob_over": .4},
    ]).to_csv(props / "saves_conditional_start_predictions.csv", index=False)
    (props / "lane_prediction_summary.json").write_text(json.dumps({"observation_timestamp_utc": PRED}))


class PostgameReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="nhl_postgame_fixture_")
        self.root = Path(self.tmp.name)
        self.pred = self.root / "pred"
        self.out = self.root / "out"
        prediction_tree(self.pred)

    def tearDown(self):
        self.tmp.cleanup()

    def publish(self, **overrides):
        kwargs = dict(canonical=canonical(), official=official(), boxscores=boxscore(),
                      slate_date=SLATE, prediction_root=self.pred, output_root=self.out,
                      observed_at="2026-09-20T04:00:00Z")
        kwargs.update(overrides)
        return publish_reconciliation(**kwargs)

    def test_all_final_valid_points_conditional_saves_and_absent_sog_prediction(self):
        with patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch.dict(os.environ, {"ODDS_API_KEY": "fixture-secret"}):
            destination, status = self.publish()
        self.assertEqual(status, "COMPLETE_NEW_APPEND_ONLY")
        points = pd.read_csv(destination / "graded_points.csv")
        saves = pd.read_csv(destination / "graded_saves.csv")
        sog = pd.read_csv(destination / "graded_sog_outcomes_only.csv")
        self.assertEqual(points.official_points.iloc[0], 2)
        self.assertEqual(points.grading_status.iloc[0], "PRESEASON_NON_EVALUATION")
        self.assertEqual(saves.grading_status.value_counts().to_dict(), {
            "PRESEASON_NON_EVALUATION_CONDITIONAL_STARTER": 2,
        })
        self.assertTrue(sog.grading_status.eq("NO_SEPTEMBER_19_PREDICTION_GRADE").all())
        summary = json.loads((destination / "summary.json").read_text())
        self.assertEqual(summary["bookmaker_requests"], 0)
        self.assertEqual(summary["paid_credits"], 0)

    def test_unfinished_game_fails_before_collector(self):
        called = []
        with self.assertRaisesRegex(RuntimeError, "NOT_OFFICIAL_FINAL"):
            self.publish(official=official("LIVE"), collector=lambda: called.append(True))
        self.assertEqual(called, [])

    def test_identity_mismatch_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "HOME_IDENTITY_MISMATCH"):
            validate_final_slate(canonical(), official(home=99), SLATE)

    def test_idempotent_second_pass_inserts_zero_and_does_not_recollect(self):
        called = []
        first, state1 = self.publish(collector=lambda: called.append("first"))
        second, state2 = self.publish(collector=lambda: called.append("second"))
        self.assertEqual(first, second)
        self.assertEqual(state1, "COMPLETE_NEW_APPEND_ONLY")
        self.assertEqual(state2, "IDEMPOTENT_EXISTING_ZERO_INSERTS")
        self.assertEqual(called, ["first"])

    def test_conflicting_retained_outcome_fails_closed(self):
        self.publish()
        changed = official().copy()
        changed["home_score"] = 4
        with self.assertRaisesRegex(RuntimeError, "CONFLICTING_RETAINED_OUTCOME"):
            self.publish(official=changed)

    def test_lock_is_acquired_and_released_on_failure(self):
        with self.assertRaisesRegex(RuntimeError, "fixture"):
            with reconciliation_lock(self.out, SLATE):
                raise RuntimeError("fixture")
        with reconciliation_lock(self.out, SLATE):
            self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
