import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.performance_summary import (
    SCHEMA_VERSION,
    discover_daily_market_coverage,
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
        self.assertEqual(summary["market_coverage"]["status"],
                         "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE")
        self.assertIsNone(summary["market_coverage"]["matched"])
        self.assertFalse(summary["market_coverage"]["affects_grading_denominator"])

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
        self.assertIn("retained bound coverage evidence unavailable", markdown)

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

    def write_daily_evidence(self, root, lane, *, slate="2026-10-02", report_status="PASS",
                              prediction_hash_matches=True, duplicate_count=0,
                              missing_count=0, extra_count=0):
        daily_root = root / "daily_runs"
        archive_root = root / "odds_history"
        run_id = f"nhldaily_fixture_{lane}"
        run_dir = daily_root / f"run_id={run_id}"
        run_dir.mkdir(parents=True)
        archive_dir = archive_root / slate
        archive_dir.mkdir(parents=True, exist_ok=True)
        pred_path = run_dir / f"{lane}_predictions.csv"
        if lane == "points":
            pd.DataFrame({"player_id": [11, 12], "game_id": [101, 101],
                          "game_date": [slate, slate], "line": [.5, 1.5],
                          "prob_over": [.6, .4]}).to_csv(pred_path, index=False)
            grade = pd.DataFrame({"player_id": [11, 12], "game_id": [101, 101],
                                  "line": [.5, 1.5], "prediction_correct": [True, False],
                                  "grading_status": ["SETTLED", "SETTLED"]})
            matched, unmatched = 1, 1
            conditional_count = row_count = 2
        else:
            pd.DataFrame({"player_id": [22], "game_id": [201], "game_date": [slate],
                          "p_over_18_5": [.7], "p_over_19_5": [.5]}).to_csv(pred_path, index=False)
            grade = pd.DataFrame({"goalie_id": [22, 22], "game_id": [201, 201],
                                  "line": [18.5, 19.5], "prediction_correct": [True, False],
                                  "grading_status": ["SETTLED", "SETTLED"]})
            matched, unmatched = 1, 1
            conditional_count, row_count = 2, 1
        pred_sha = hashlib.sha256(pred_path.read_bytes()).hexdigest()
        odds_sha = "a" * 64
        report = {
            "schema_version": "NHL_ATTACHMENT_INTEGRITY_V1", "lane": lane,
            "status": report_status, "parent_daily_run_id": run_id,
            "prediction_artifact_path": str(pred_path.resolve()),
            "prediction_artifact_sha256": pred_sha if prediction_hash_matches else "b" * 64,
            "odds_observation_manifest_sha256": odds_sha,
            "counts": {
                "prediction_row_count": 2, "attachment_row_count": 2,
                "matched_count": matched, "unmatched_count": unmatched,
                "ambiguous_count": 0, "missing_prediction_key_count": missing_count,
                "extra_attachment_key_count": extra_count,
                "duplicate_prediction_key_count": duplicate_count,
                "duplicate_attachment_key_count": 0,
                "unique_prediction_key_count": 2 - duplicate_count,
                "unique_attachment_key_count": 2,
            },
            "checks": {
                "prediction_keys_unique": duplicate_count == 0,
                "attachment_keys_unique": True,
                "prediction_attachment_key_set_equal": not (missing_count or extra_count),
                "lineage_matches": True, "output_count_equals_prediction_count": True,
                "statuses_exhaustive": True,
            },
        }
        report_path = archive_dir / f"{lane}_attachment_integrity.json"
        report_path.write_text(json.dumps(report))
        report_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
        prediction_identity = {
            "path": str(pred_path.resolve()), "sha256": pred_sha,
            "row_count": row_count, "conditional_prediction_count": conditional_count,
        }
        receipt = {
            "slate_date": slate, "parent_daily_run_id": run_id,
            "ended_at_utc": "2026-10-03T18:00:00Z",
            "lanes": {
                lane: {"status": "COMPLETE", "outputs": [prediction_identity]},
                f"{lane}_attachment": {
                    "status": "COMPLETE", "inputs": [{"manifest_sha256": odds_sha}],
                    "outputs": [{"path": str((root / "site" / report_path.name).resolve()),
                                 "sha256": report_sha, "status": report_status}],
                },
            },
        }
        receipt_path = run_dir / "parent_receipt.json"
        receipt_path.write_text(json.dumps(receipt))
        marker_path = run_dir / "RUN_COMPLETE.json"
        marker_path.write_text(json.dumps({"parent_daily_run_id": run_id,
                                           "final_classification": "READY"}))
        files = [receipt_path, marker_path]
        (run_dir / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(file.read_bytes()).hexdigest()}  {file.name}\n" for file in files))
        return daily_root, archive_root, grade

    def test_points_and_saves_retained_integrity_counts_are_bound(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            daily, archive, points = self.write_daily_evidence(root, "points")
            _, _, saves = self.write_daily_evidence(root, "saves")
            coverage = discover_daily_market_coverage(
                slate_date="2026-10-02", grades={"points": points, "saves": saves},
                daily_run_root=daily, integrity_archive_root=archive)
        self.assertEqual(coverage["points"]["status"], "AVAILABLE")
        self.assertEqual((coverage["points"]["prediction_rows"], coverage["points"]["matched"],
                          coverage["points"]["unmatched"], coverage["points"]["ambiguous"]),
                         (2, 1, 1, 0))
        self.assertEqual(coverage["saves"]["status"], "AVAILABLE")
        self.assertEqual((coverage["saves"]["prediction_rows"], coverage["saves"]["matched"],
                          coverage["saves"]["unmatched"], coverage["saves"]["ambiguous"]),
                         (2, 1, 1, 0))

    def test_wrong_slate_receipt_is_not_selected(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            daily, archive, points = self.write_daily_evidence(root, "points", slate="2026-10-01")
            result = discover_daily_market_coverage(slate_date="2026-10-02",
                grades={"points": points}, daily_run_root=daily, integrity_archive_root=archive)
        self.assertEqual(result["points"]["status"], "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE")

    def test_prediction_sha_mismatch_rejects_coverage(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            daily, archive, points = self.write_daily_evidence(
                root, "points", prediction_hash_matches=False)
            result = discover_daily_market_coverage(slate_date="2026-10-02",
                grades={"points": points}, daily_run_root=daily, integrity_archive_root=archive)
        self.assertIsNone(result["points"]["matched"])

    def test_failed_integrity_report_rejects_coverage(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            daily, archive, points = self.write_daily_evidence(root, "points", report_status="FAIL")
            result = discover_daily_market_coverage(slate_date="2026-10-02",
                grades={"points": points}, daily_run_root=daily, integrity_archive_root=archive)
        self.assertEqual(result["points"]["status"], "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE")

    def test_key_integrity_failures_reject_coverage(self):
        for values in ({"duplicate_count": 1}, {"missing_count": 1}, {"extra_count": 1}):
            with self.subTest(values=values), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                daily, archive, points = self.write_daily_evidence(root, "points", **values)
                result = discover_daily_market_coverage(slate_date="2026-10-02",
                    grades={"points": points}, daily_run_root=daily, integrity_archive_root=archive)
            self.assertIsNone(result["points"]["matched"])

    def test_unavailable_market_coverage_uses_null_counts_not_zero(self):
        coverage = self.summary()["models"]
        for lane in ("points", "saves", "sog"):
            self.assertEqual(coverage[lane]["market_coverage"]["status"],
                             "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE")
            self.assertIsNone(coverage[lane]["market_coverage"]["matched"])

    def test_sog_coverage_remains_unavailable_without_bound_report(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            daily, archive, points = self.write_daily_evidence(root, "points")
            coverage = discover_daily_market_coverage(slate_date="2026-10-02",
                grades={"points": points, "sog": grade_frames()["sog"]},
                daily_run_root=daily, integrity_archive_root=archive)
        self.assertEqual(coverage["sog"]["status"], "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE")
        self.assertIsNone(coverage["sog"]["matched"])

    def test_market_coverage_does_not_change_grade_denominators(self):
        baseline = self.summary()["models"]
        coverage = {lane: {"status": "AVAILABLE", "prediction_rows": 4, "matched": 1,
                           "unmatched": 3, "ambiguous": 0, "match_rate": .25}
                    for lane in ("points", "saves", "sog")}
        after = self.summary(market_coverage=coverage)["models"]
        for lane in ("points", "saves"):
            self.assertEqual(baseline[lane]["reference"]["overall"],
                             after[lane]["reference"]["overall"])
        for arm in ("ARM_A",):
            self.assertEqual(baseline["sog"][arm]["overall"], after["sog"][arm]["overall"])

    def test_markdown_uses_same_market_coverage_object_as_json(self):
        coverage = {"points": {"status": "AVAILABLE", "prediction_rows": 1866,
                                "matched": 609, "unmatched": 1257, "ambiguous": 0,
                                "match_rate": 609 / 1866}}
        summary = self.summary(market_coverage=coverage)
        self.assertEqual(summary["models"]["points"]["market_coverage"],
                         {**coverage["points"], "affects_grading_denominator": False})
        self.assertIn("Points: 609 matched; 1257 unmatched; 1866 total", render_markdown(summary))

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
                 patch.object(postgame_learning, "_grade_final_capture", return_value=("NOT_AVAILABLE", None)), \
                 patch.object(postgame_learning, "discover_daily_market_coverage", return_value={
                     "points": {"status": "AVAILABLE", "prediction_rows": 4,
                                "matched": 1, "unmatched": 3, "ambiguous": 0,
                                "match_rate": 0.25,
                                "source_artifact_sha256": "1" * 64,
                                "daily_receipt_manifest_sha256": "2" * 64,
                                "prediction_artifact_sha256": "3" * 64,
                                "odds_observation_manifest_sha256": "4" * 64},
                 }) as coverage_call:
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
            coverage_call.assert_called_once()
            self.assertTrue(Path(result["performance_summary_json"]).is_file())
            self.assertTrue(Path(result["performance_summary_md"]).is_file())
            summary = json.loads(Path(result["performance_summary_json"]).read_text())
            self.assertEqual(summary["models"]["points"]["market_coverage"]["matched"], 1)


if __name__ == "__main__":
    unittest.main()
