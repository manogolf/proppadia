import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.performance_summary import (
    SCHEMA_VERSION,
    generate_from_artifacts,
    render_markdown,
    summarize_frames,
)
from backend.nhl import postgame_learning


def grade_frames():
    moneyline = pd.DataFrame({
        "grading_status": ["REGULAR_SEASON_GRADED"] * 3,
        "prediction_correct": [True, False, None],
        "v2_home_win_probability": [0.7, 0.6, 0.5],
        "home_team": ["A", "A", "A"],
        "model_favored_team": ["A", "A", "B"],
        "control_name": ["ML_REF"] * 3,
    })
    puck = pd.DataFrame({
        "grading_status": ["REGULAR_SEASON_GRADED"] * 2,
        "prediction_correct": [True, False],
        "actual_margin_class": ["HOME_BY_2_PLUS", "ONE_GOAL_GAME"],
        "control_name": ["PL_REF"] * 2,
    })
    sog = pd.DataFrame({
        "contract_arm": ["ARM_A"] * 5,
        "grading_state": ["WIN", "LOSS", "PUSH", "UNRESOLVED_UNGRADED", "WIN"],
        "selected_side": ["OVER", "UNDER", "OVER", "UNDER", "OVER"],
        "line": [1.5, 1.5, 1.5, 1.5, 2.5],
        "p_over": [.6, .3, .7, .2, .55],
        "p_under": [.4, .7, .3, .8, .45],
        "model_version": ["sog-v1"] * 5,
    })
    points = pd.DataFrame({
        "grading_status": ["SETTLED", "SETTLED", "PUSH", "PARTICIPATION_STATUS_UNRESOLVED"],
        "prediction_correct": [True, False, None, None],
        "settled_side": ["OVER", "UNDER", "PUSH", None],
        "model_side": ["OVER", "OVER", "UNDER", "OVER"],
        "line": [.5, .5, .5, 1.5],
        "prob_over": [.8, .7, .5, .2],
        "market_qualified": [True, False, True, False],
        "model_version": ["points-v1"] * 4,
    })
    saves = pd.DataFrame({
        "grading_status": ["SETTLED", "SETTLED", "PUSH", "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL", "STARTER_STATUS_UNRESOLVED"],
        "prediction_correct": [True, False, None, None, None],
        "model_side": ["OVER", "UNDER", "OVER", "OVER", "UNDER"],
        "line": [20.5, 20.5, 20.5, 20.5, 21.5],
        "prob_over": [.8, .2, .5, .6, .4],
        "actual_start_flag": [True, True, True, False, None],
        "model_version": ["saves-v1"] * 5,
    })
    return {"moneyline": moneyline, "puck_line": puck, "sog": sog,
            "points": points, "saves": saves}


class NHLPerformanceSummaryTests(unittest.TestCase):
    def summary(self, grades=None, **kwargs):
        return summarize_frames(
            slate_date="2026-10-02", games=5, phase="REGULAR_SEASON",
            reconciliation_status="CREATED", package_identity="pkg-hash",
            grades=grades or grade_frames(), source_artifacts={"grade": "abc"},
            generated_at_utc="2026-10-03T15:00:00Z", **kwargs)

    def test_moneyline_correct_incorrect_and_unresolved(self):
        row = self.summary()["models"]["moneyline"]["reference"]
        self.assertEqual((row["correct"], row["incorrect"], row["unresolved"]), (1, 1, 1))

    def test_puck_line_correct_margin_class_counts(self):
        row = self.summary()["models"]["puck_line"]["reference"]
        self.assertEqual((row["correct_realized_margin_class"], row["incorrect"]), (1, 1))
        self.assertEqual(row["realized_margin_class_counts"]["ONE_GOAL_GAME"], 1)

    def test_sog_win_loss_push_unresolved(self):
        row = self.summary()["models"]["sog"]["ARM_A"]["overall"]
        self.assertEqual((row["wins"], row["losses"], row["pushes"], row["unresolved"]), (2, 1, 1, 1))

    def test_sog_line_counts(self):
        row = self.summary()["models"]["sog"]["ARM_A"]["by_line"]["1.5"]
        self.assertEqual((row["settled"], row["wins"], row["losses"], row["pushes"], row["unresolved"]),
                         (3, 1, 1, 1, 1))

    def test_points_win_loss_push_unresolved(self):
        row = self.summary()["models"]["points"]["reference"]["overall"]
        self.assertEqual((row["wins"], row["losses"], row["pushes"], row["unresolved"]), (1, 1, 1, 1))

    def test_points_by_line(self):
        row = self.summary()["models"]["points"]["reference"]["by_line"]["0.5"]
        self.assertEqual((row["settled"], row["wins"], row["losses"], row["pushes"]), (3, 1, 1, 1))
        self.assertEqual(self.summary()["models"]["points"]["realized_points_definition"],
                         "official_goals + official_assists")

    def test_saves_win_loss_push(self):
        row = self.summary()["models"]["saves"]["reference"]["overall"]
        self.assertEqual((row["wins"], row["losses"], row["pushes"]), (1, 1, 1))

    def test_confirmed_nonstarter_not_counted_as_loss(self):
        row = self.summary()["models"]["saves"]["reference"]["overall"]
        self.assertEqual(row["confirmed_did_not_start_not_gradeable"], 1)
        self.assertEqual(row["losses"], 1)

    def test_unresolved_starter_excluded_from_settled_denominator(self):
        row = self.summary()["models"]["saves"]["reference"]["overall"]
        self.assertEqual(row["starter_status_unresolved"], 1)
        self.assertEqual(row["settled"], 3)

    def test_push_does_not_enter_win_rate_denominator(self):
        model = self.summary()["models"]["points"]["reference"]
        row = model["overall"]
        self.assertEqual(row["win_rate"], 0.5)
        self.assertIn("pushes excluded", model["win_rate_denominator"])

    def test_reference_and_challenger_are_separate(self):
        challenger = pd.DataFrame({"evaluation_status": ["REGULAR_SEASON_GRADED"],
                                   "correct": [True], "model_version": ["challenger-v1"]})
        models = self.summary(challengers={"moneyline": challenger})["models"]["moneyline"]
        self.assertEqual(models["reference"]["graded"], 2)
        self.assertEqual(models["challenger"]["graded"], 1)

    def test_unscored_sog_rows_not_added_to_losses(self):
        with tempfile.TemporaryDirectory() as raw:
            package = Path(raw)
            pd.DataFrame({"exclusion_reason": ["NO_ROLE", "NO_ROLE"]}).to_csv(
                package / "graded_sog_source_exclusions.csv", index=False)
            pd.DataFrame({"reason": ["NO_PREDICTION"]}).to_csv(
                package / "graded_sog_missing_predictions.csv", index=False)
            summary = self.summary(package=package)
            row = summary["models"]["sog"]["ARM_A"]
        self.assertEqual(row["overall"]["losses"], 1)
        self.assertEqual(summary["models"]["sog"]["unscored"]["unscored_identities"], 3)
        self.assertEqual(summary["models"]["sog"]["unscored"]["unscored_reason_counts"],
                         {"SOURCE_EXCLUSION:NO_ROLE": 2, "MISSING_PREDICTION:NO_PREDICTION": 1})

    def test_market_quote_counts_are_context_only(self):
        summary = self.summary()["models"]["points"]
        self.assertEqual(summary["reference"]["overall"]["settled"], 3)
        self.assertFalse(summary["market_coverage_context"]["affects_grading_denominator"])

    def test_json_schema_and_deterministic_substantive_fields(self):
        left, right = self.summary(), self.summary()
        self.assertEqual(left["schema_version"], SCHEMA_VERSION)
        self.assertEqual(left, right)

    def test_markdown_is_rendered_from_summary_object(self):
        summary = self.summary()
        markdown = render_markdown(summary)
        self.assertIn("# NHL Daily Performance — 2026-10-02", markdown)
        self.assertIn("| Line | Settled | W | L | P | Unresolved | Win % |", markdown)
        self.assertIn("No selection policy or promotion rule was applied.", markdown)
        self.assertIn("Realized total: official goals + official assists.", markdown)
        self.assertIn("Market Coverage Context", markdown)
        self.assertIn("Quote matching is descriptive and does not affect grading.", markdown)

    def test_immutable_generation_is_idempotent_and_markdown_matches_json(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            package = root / "2026-10-02" / "reconciliation=fixture"
            restatement = root / "learning_restatements" / package.parent.name / "restatement=fixture"
            package.mkdir(parents=True)
            restatement.mkdir(parents=True)
            (package / "summary.json").write_text(json.dumps({"slate_date": "2026-10-02", "games": 5,
                "status": "COMPLETE", "substantive_identity": "fixture"}))
            (package / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
            (package / "canonical_admitted_slate.csv").write_text("game_type_code\n2\n")
            (package / "canonical_game_outcomes.csv").write_text("game_id,game_type_code,official_final\n1,2,True\n")
            (package / "SHA256SUMS").write_text("source\n")
            (restatement / "lineage.json").write_text("{}")
            for lane, frame in grade_frames().items():
                frame.to_csv(restatement / f"graded_{lane}.csv", index=False)
            first = generate_from_artifacts(package=package, restatement=restatement,
                reconciliation_status="CREATED", output_root=root / "summaries")
            first_bytes = first[0].read_bytes()
            second = generate_from_artifacts(package=package, restatement=restatement,
                reconciliation_status="CREATED", output_root=root / "summaries")
            self.assertEqual(first_bytes, second[0].read_bytes())
            payload = json.loads(first[0].read_text())
            self.assertEqual(first[1].read_text(), render_markdown(payload))

    def test_daily_learning_result_contains_summary_paths(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            package = root / "2026-10-02" / "reconciliation=fixture"
            package.mkdir(parents=True)
            for filename, content in {
                "summary.json": {"slate_date": "2026-10-02", "games": 1, "status": "COMPLETE",
                                 "substantive_identity": "fixture"},
                "RUN_COMPLETE.json": {"status": "COMPLETE"},
            }.items():
                (package / filename).write_text(json.dumps(content))
            (package / "canonical_game_outcomes.csv").write_text(
                "game_id,game_type_code,official_final\n1,2,True\n")
            restatement = root / "restatement"
            restatement.mkdir()
            (restatement / "lineage.json").write_text("{}")
            for lane, frame in grade_frames().items():
                frame.to_csv(restatement / f"graded_{lane}.csv", index=False)
            with patch.object(postgame_learning, "_existing_reconciliation", return_value=package), \
                 patch.object(postgame_learning, "verify_reconciliation_package", return_value={"games": 1}), \
                 patch.object(postgame_learning, "_write_phase_restatement", return_value=restatement), \
                 patch.object(postgame_learning, "_grade_final_capture", return_value=("NOT_AVAILABLE", None)):
                # Add the immutable source files required by the summary builder.
                (package / "canonical_admitted_slate.csv").write_text("game_type_code\n2\n")
                for lane, frame in grade_frames().items():
                    source = package / f"graded_{lane}.csv"
                    if lane == "sog":
                        frame.to_csv(source, index=False)
                (package / "graded_sog_source_exclusions.csv").write_text("exclusion_reason\n")
                (package / "graded_sog_missing_predictions.csv").write_text("reason\n")
                (package / "SHA256SUMS").write_text("fixture\n")
                result = postgame_learning.ensure_prior_learning(
                    "2026-10-02", reconciliation_root=root, create_if_missing=False)
            self.assertTrue(Path(result["performance_summary_json"]).is_file())
            self.assertTrue(Path(result["performance_summary_md"]).is_file())


if __name__ == "__main__":
    unittest.main()
