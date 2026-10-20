from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.postgame_reconcile import core
from backend.nhl.prediction_only.core import digest as prediction_only_digest
from backend.nhl.sog_cold_start.core import digest as sog_digest


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ResilientCrossMarketAdapterTests(unittest.TestCase):
    def test_empty_candidate_population_returns_empty_role_grades(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "operational"
            slate_date = "2026-10-18"
            schedule = pd.DataFrame([{
                "game_id": 2026020135,
                "scheduled_start_time_utc": "2026-10-18T22:00:00Z",
                "game_type_code": 2,
            }])
            grades, index = core._resilient_cross_market_grades(
                slate_date=slate_date, operational_root=root, schedule=schedule,
                games=pd.DataFrame(), skaters=pd.DataFrame(), goalies=pd.DataFrame(),
            )
            self.assertEqual(index["lanes"]["MONEYLINE"]["coverage_status"], "EMPTY")
            self.assertEqual(len(grades["resilient_moneyline_production_champion"]), 0)
            self.assertEqual(len(grades["resilient_moneyline_shadow"]), 0)

    def test_receipt_bound_production_shadow_and_puck_are_separate_grades(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "operational"
            slate_date = "2026-10-18"
            source = root / "cross_market_shadow/season=2026/slate_date=2026-10-18/run_type=REFRESH/state=state1"
            source.mkdir(parents=True)
            roster = root / "roster/observation=one"
            roster.mkdir(parents=True)
            (roster / "SHA256SUMS").write_text("fixture manifest\n")
            (roster / "observation_summary.json").write_text(json.dumps({"season": 2026, "phase": "MIDDAY"}))
            roster_manifest = sha(roster / "SHA256SUMS")
            observer_links = {}
            source_prop_runs = {}
            for lane, filename, rows in (
                ("POINTS", "immutable_predictions.csv", [
                    {"run_id": "points1", "game_id": 2026020135, "player_id": 77,
                     "line": line, "prob_over": .4 + line / 10, "model_version": "POINTS_V1",
                     "probability_construction": "COUNT_DISTRIBUTION", "phase": "MIDDAY",
                     "prediction_timestamp_utc": "2026-10-18T18:40:00Z",
                     "input_cutoff_timestamp_utc": "2026-10-18T18:30:00Z",
                     "feature_cutoff_timestamp_utc": "2026-10-18T18:30:00Z",
                     "scheduled_start_time_utc": "2026-10-18T22:00:00Z", "game_type_code": 2,
                     "slate_date": slate_date, "canonical_season": 2026,
                     "prediction_eligible": True, "market_qualified": False, "price": None,
                     "prediction_identity": prediction_only_digest({
                         "run_id": "points1", "game_id": 2026020135, "player_id": 77,
                         "line": float(line), "model_version": "POINTS_V1",
                         "probability_construction": "COUNT_DISTRIBUTION"})}
                    for line in (.5, 1.5, 2.5)]),
                ("SAVES", "immutable_conditional_predictions.csv", [
                    {"run_id": "saves1", "game_id": 2026020135, "goalie_id": 88,
                     "line": line, "prob_over": .4, "model_version": "SAVES_V1",
                     "prediction_semantics": "CONDITIONAL_STARTER", "phase": "MIDDAY",
                     "prediction_timestamp_utc": "2026-10-18T18:40:00Z",
                     "input_cutoff_timestamp_utc": "2026-10-18T18:30:00Z",
                     "scheduled_start_time_utc": "2026-10-18T22:00:00Z", "game_type_code": 2,
                     "slate_date": slate_date, "canonical_season": 2026,
                     "expected_saves": 25.0, "market_qualified": False, "price": None,
                     "prediction_identity": prediction_only_digest({
                         "run_id": "saves1", "game_id": 2026020135, "goalie_id": 88,
                         "line": float(line), "model_version": "SAVES_V1"})}
                    for line in (18.5 + n for n in range(13))]),
                ("SOG", "immutable_predictions.csv", [{
                    "canonical_season": 2026, "slate_date": slate_date,
                    "game_id": 2026020135, "player_id": 77, "line": 2.5,
                    "phase": "MIDDAY", "prediction_timestamp_utc": "2026-10-18T18:40:00Z",
                    "feature_cutoff_utc": "2026-10-18T18:30:00Z",
                    "input_cutoff_utc": "2026-10-18T18:30:00Z",
                    "scheduled_start_time_utc": "2026-10-18T22:00:00Z", "game_type_code": 2,
                    "contract_arm": "A_BASE", "contract_sha256": core._sha(core.SOG_CONTRACT_PATH),
                    "selected_side": "OVER", "prob_over": .6,
                    "prediction_identity": sog_digest({
                        "contract": core._sha(core.SOG_CONTRACT_PATH), "arm": "A_BASE",
                        "phase": "MIDDAY", "slate_date": slate_date,
                        "game_id": 2026020135, "player_id": 77, "line": 2.5,
                    }),
                }]),
            ):
                source_prop = root / f"{lane.lower()}_prediction_only/season=2026/slate_date={slate_date}/phase=MIDDAY/run_id={lane.lower()}1"
                source_prop.mkdir(parents=True)
                source_prop_runs[lane] = source_prop
                pd.DataFrame(rows).to_csv(source_prop / filename, index=False)
                pd.DataFrame([{
                    "game_id": 2026020135,
                    "scheduled_start_time_utc": "2026-10-18T22:00:00Z",
                }]).to_csv(source_prop / "canonical_game_spine.csv", index=False)
                (source_prop / "run_metadata.json").write_text(json.dumps({
                    "run_id": f"{lane.lower()}1", "lane": lane,
                    "slate_date": slate_date, "phase": "MIDDAY",
                }))
                (source_prop / "RUN_COMPLETE.json").write_text(json.dumps({
                    "status": "COMPLETE", "completed_at_utc": "2026-10-18T18:42:00Z",
                }))
                (source_prop / "SHA256SUMS").write_text("".join(
                    f"{sha(path)}  {path.name}\n" for path in sorted(source_prop.iterdir())
                ))
                child_status = root / f"{lane.lower()}_prediction_only/status/{slate_date}/capture.json"
                child_status.parent.mkdir(parents=True)
                child_status.write_text(json.dumps({
                    "status": "CAPTURED", "slate_date": slate_date,
                    "phase": "MIDDAY", "run_dir": str(source_prop),
                    "durable_publication_timestamp_utc": "2026-10-18T18:42:01Z",
                }))
                observer_links[lane] = {"status": "CAPTURED", "receipt_path": str(child_status)}
            production = pd.DataFrame([{
                "game_id": 2026020135, "slate_date": slate_date,
                "scheduled_start_time_utc": "2026-10-18T22:00:00Z",
                "prediction_creation_time_utc": "2026-10-18T18:40:00Z",
                "home_team": "AAA", "away_team": "BBB",
                "challenger_favored_team": "AAA", "challenger_home_win_probability": .6,
                "challenger_away_win_probability": .4,
                "model_version": "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3",
                "model_parameter_sha256": "modelsha", "operational_role": "PRODUCTION_CHAMPION",
            }])
            shadow = pd.DataFrame([{
                "game_id": 2026020135, "slate_date": slate_date,
                "scheduled_start_time_utc": "2026-10-18T22:00:00Z",
                "prediction_creation_time_utc": "2026-10-18T18:40:00Z",
                "home_team": "AAA", "away_team": "BBB", "model_favored_team": "BBB",
            }])
            puck = pd.DataFrame([{
                "game_id": 2026020135, "slate_date": slate_date,
                "scheduled_start_time_utc": "2026-10-18T22:00:00Z",
                "prediction_creation_time_utc": "2026-10-18T18:40:00Z",
                "home_team": "AAA", "away_team": "BBB",
                "away_by_2_plus_probability": .2, "one_goal_game_probability": .5,
                "home_by_2_plus_probability": .3,
            }])
            artifacts = {
                "moneyline_production_predictions.csv": production,
                "moneyline_schedule_logit_v2_shadow_predictions.csv": shadow,
                "puck_line_v1_immutable_predictions.csv": puck,
            }
            for name, frame in artifacts.items():
                frame.to_csv(source / name, index=False)
            status = {
                "slate_date": slate_date, "run_type": "REFRESH", "substantive_state_sha256": "state1",
                "moneyline_production_model": "ML_V3", "moneyline_shadow_model": "ML_V2",
                "moneyline_shot_finishing_challenger_v3_feature_policy": "ML_FEATURE_V3",
                "moneyline_shadow_feature_policy": "ML_FEATURE_V2",
                "puck_line_v2_shot_prior_feature_policy": "PL_FEATURE_V1",
            }
            (source / "daily_execution_status.json").write_text(json.dumps(status))
            entries = []
            for path in sorted(source.iterdir()):
                entries.append(f"{sha(path)}  {path.name}\n")
            (source / "SHA256SUMS").write_text("".join(entries))

            receipt_path = root / "daily_runs/run_id=run1/parent_receipt.json"
            receipt_path.parent.mkdir(parents=True)
            output_rows = [
                {"path": str(source / name), "sha256": sha(source / name)}
                for name in ("moneyline_production_predictions.csv",
                             "moneyline_schedule_logit_v2_shadow_predictions.csv")
            ]
            receipt = {
                "slate_date": slate_date, "feature_cutoff_utc": "2026-10-18T18:35:00Z",
                "ended_at_utc": "2026-10-18T18:45:00Z",
                "prior_day_learning": {"certificate": {
                    "status": "CERTIFIED_FINAL", "package_path": "/verified/cert",
                    "package_manifest_sha256": "cert-hash",
                }},
                "roster_observation": {"path": str(roster), "manifest_sha256": roster_manifest},
                "lanes": {"moneyline": {"ended_at_utc": "2026-10-18T18:44:00Z",
                                          "outputs": output_rows}},
            }
            receipt_path.write_text(json.dumps(receipt))
            # Natural daily receipts can carry an explicit null for an
            # unavailable prerequisite section. Such unrelated receipts must
            # not abort discovery of separately linked, valid sources.
            null_receipt = root / "daily_runs/run_id=run-null/parent_receipt.json"
            null_receipt.parent.mkdir(parents=True)
            null_receipt.write_text(json.dumps({
                "slate_date": slate_date, "prior_day_learning": None,
            }))
            observer_parent = root / f"cross_market_shadow/orchestration_status/{slate_date}/capture.json"
            observer_parent.parent.mkdir(parents=True)
            observer_parent.write_text(json.dumps({
                "slate_date": slate_date, "player_prop_observers": observer_links,
                "prior_day_reconciliation_certificate": {
                    "status": "CERTIFIED_FINAL", "package_path": "/verified/cert",
                    "package_manifest_sha256": "cert-hash",
                },
                "current_slate_roster_observation": {
                    "status": "VERIFIED_CURRENT_SLATE_ROSTER", "package_path": str(roster),
                    "package_manifest_sha256": roster_manifest,
                },
            }))
            orphan = root / "points_prediction_only/season=2026" / f"slate_date={slate_date}" / "phase=MIDDAY" / "run_id=points-orphan"
            orphan.mkdir(parents=True)
            linked_points_status = json.loads(Path(observer_links["POINTS"]["receipt_path"]).read_text())
            linked_points_run = Path(linked_points_status["run_dir"])
            (orphan / "immutable_predictions.csv").write_bytes(
                (linked_points_run / "immutable_predictions.csv").read_bytes())
            (orphan / "run_metadata.json").write_text(json.dumps({
                "run_id": "points-orphan", "lane": "POINTS",
                "slate_date": slate_date, "phase": "MIDDAY",
            }))
            (orphan / "SHA256SUMS").write_text("".join(
                f"{sha(path)}  {path.name}\n" for path in sorted(orphan.iterdir())
            ))
            orphan_status = root / f"points_prediction_only/status/{slate_date}/orphan.json"
            orphan_status.write_text(json.dumps({
                "status": "CAPTURED", "slate_date": slate_date,
                "phase": "MIDDAY", "run_dir": str(orphan),
            }))

            schedule = pd.DataFrame([{
                "game_id": 2026020135, "scheduled_start_time_utc": "2026-10-18T22:00:00Z",
                "game_type_code": 2,
            }])
            games = pd.DataFrame([{
                "game_id": 2026020135, "official_full_game_winner": "HOME",
                "official_final_home_goals": 3, "official_final_away_goals": 2,
            }])
            skaters = pd.DataFrame([{
                "canonical_season": 2026, "slate_date": slate_date, "game_id": 2026020135,
                "player_id": 77, "official_final": True, "official_sog": 3,
                "official_goals": 1, "official_assists": 0, "participation_state": "PARTICIPATED",
                "outcome_source": "NHL_OFFICIAL_GAMECENTER",
                "outcome_source_timestamp_utc": "2026-10-19T01:00:00Z",
            }])
            goalies = pd.DataFrame([{
                "game_id": 2026020135, "goalie_id": 88, "team_id": 1,
                "official_saves": 24, "actual_start_flag": True,
                "goalie_participation_state": "STARTER", "starter_identity_method": "OFFICIAL",
            }])
            cert_validator = lambda path, day: {
                "package_manifest_sha256": "cert-hash", "status": "CERTIFIED_FINAL",
            }
            roster_validator = lambda *args, **kwargs: {"manifest_sha256": roster_manifest}
            with patch("backend.nhl.reconciliation_certificate.verify_reconciliation_certificate",
                       side_effect=cert_validator), patch(
                           "backend.nhl.daily_orchestration.verify_roster_observation_reuse",
                           side_effect=roster_validator), patch.object(
                               core, "_verify_cross_market_identities"), patch(
                                   "backend.nhl.cross_market_shadow.moneyline_challenger.load_model",
                                   return_value={"parameter_file_sha256": "modelsha"}):
                grades, index = core._resilient_cross_market_grades(
                    slate_date=slate_date, operational_root=root, schedule=schedule,
                    games=games, skaters=skaters, goalies=goalies)

            self.assertTrue({
                "resilient_moneyline_production_champion", "resilient_moneyline_shadow",
                "resilient_puck_line", "resilient_sog", "resilient_points", "resilient_saves",
            }.issubset(set(grades)))
            self.assertEqual(index["lanes"]["MONEYLINE"]["selected_atomic_prediction_count"], 2)
            self.assertEqual(index["lanes"]["MONEYLINE"]["discovered_source_package_count"], 1)
            self.assertEqual(index["lanes"]["PUCK_LINE"]["selected_atomic_prediction_count"], 1)
            self.assertEqual(index["lanes"]["PUCK_LINE"]["discovered_source_package_count"], 1)
            self.assertEqual(index["lanes"]["POINTS"]["selected_atomic_prediction_count"], 1)
            self.assertEqual(index["lanes"]["SAVES"]["selected_atomic_prediction_count"], 1)
            self.assertEqual(index["lanes"]["SOG"]["selected_atomic_prediction_count"], 1)
            self.assertEqual(index["lanes"]["POINTS"]["unlinked_source_package_count"], 1)
            self.assertTrue(any(
                item.get("lane") == "POINTS"
                and item.get("excluded_reason") == "RESILIENT_PROP_PARENT_CERTIFICATE_ROSTER_LINKAGE_MISSING"
                for item in index["proof_receipts"]
            ))
            self.assertEqual(len(grades["resilient_points"]), 3)
            self.assertEqual(len(grades["resilient_saves"]), 13)
            self.assertTrue(all(grades[name].evaluation_population.eq("RESILIENT_PREGAME").all()
                                for name in ("resilient_moneyline_production_champion",
                                             "resilient_moneyline_shadow", "resilient_puck_line",
                                             "resilient_points", "resilient_saves")))
            self.assertTrue(all(frame.resilient_parent_receipt_sha256.notna().all()
                                for frame in grades.values() if not frame.empty))

            points_run = source_prop_runs["POINTS"]
            marker_path = points_run / "RUN_COMPLETE.json"
            marker_path.write_text(json.dumps({"status": "COMPLETE", "run_id": "points1"}))
            (points_run / "SHA256SUMS").write_text("".join(
                f"{sha(path)}  {path.name}\n" for path in sorted(points_run.iterdir())
                if path.name != "SHA256SUMS"
            ))
            with self.assertRaisesRegex(RuntimeError, "TIMESTAMP_NOT_PREGAME"):
                core._verify_resilient_prop_package(
                    run=points_run, lane="POINTS", slate_date=slate_date,
                    schedule=schedule,
                )


class ResilientSogMultipleSnapshotTests(unittest.TestCase):
    """A slate may have up to four daily snapshots, each with its own roster."""

    def _run(self, package_roster: str | None, complete: bool = False, before_run=None):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "operational"
            slate_date = "2026-10-19"
            rosters = {}
            for name in ("midday", "late"):
                roster = root / f"roster/observation={name}"
                roster.mkdir(parents=True)
                (roster / "SHA256SUMS").write_text(f"{name} manifest\n")
                (roster / "observation_summary.json").write_text(
                    json.dumps({"season": 2026, "phase": "MIDDAY"}))
                rosters[name] = roster
            # The SOG package was built against the midday snapshot's roster.
            package = root / "sog_prediction_only/run_id=sog-midday"
            package.mkdir(parents=True)
            (package / "run_metadata.json").write_text(json.dumps({
                "slate_date": slate_date, "phase": "MIDDAY",
                "readiness_evidence": {
                    "prior_day_reconciliation_certificate": {"package_manifest_sha256": "cert-hash"},
                    "current_slate_roster_observation": {
                        "package_manifest_sha256": package_roster or sha(rosters["midday"] / "SHA256SUMS")},
                },
            }))
            if complete:
                (package / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
            (package / "SHA256SUMS").write_text("".join(
                f"{sha(path)}  {path.name}\n" for path in sorted(package.iterdir())))
            if before_run:
                before_run(root, package)
            certificate = {"status": "CERTIFIED_FINAL", "package_path": "/verified/cert",
                           "package_manifest_sha256": "cert-hash"}
            for name, outputs in (("midday", []), ("late", [{"path": str(package)}])):
                receipt = root / f"daily_runs/run_id={name}/parent_receipt.json"
                receipt.parent.mkdir(parents=True)
                receipt.write_text(json.dumps({
                    "slate_date": slate_date,
                    "prior_day_learning": {"certificate": certificate},
                    "roster_observation": {"path": str(rosters[name]),
                                           "manifest_sha256": sha(rosters[name] / "SHA256SUMS")},
                    "lanes": {"cold_start_sog_reference": {"outputs": outputs}},
                }))
            schedule = pd.DataFrame([{"game_id": 2026020139,
                                      "scheduled_start_time_utc": "2026-10-19T23:00:00Z",
                                      "game_type_code": 2}])
            empty = pd.DataFrame()
            with patch("backend.nhl.reconciliation_certificate.verify_reconciliation_certificate",
                       return_value={"package_manifest_sha256": "cert-hash", "status": "CERTIFIED_FINAL"}), \
                    patch("backend.nhl.daily_orchestration.verify_roster_observation_reuse",
                          side_effect=self._roster_validator):
                return core._resilient_cross_market_grades(
                    slate_date=slate_date, operational_root=root, schedule=schedule,
                    games=empty, skaters=empty, goalies=empty)

    @staticmethod
    def _roster_validator(path, **kwargs):
        # Like the real validator: the late snapshot followed a puck drop, so
        # only the per-game rule accepts it.
        if "late" in str(path) and kwargs.get("require_strictly_prestart", True):
            raise RuntimeError("ROSTER_REUSE_VALIDATION_FAILED:strictly_prestart")
        return {"manifest_sha256": sha(Path(path) / "SHA256SUMS")}

    def test_package_bound_to_an_earlier_verified_snapshot_is_accepted(self):
        _, index = self._run(package_roster=None)
        # It passes the readiness check and stops only at the (absent)
        # completion marker; no conflict is raised.
        self.assertTrue(any(
            item.get("excluded_reason") == "RESILIENT_SOG_SOURCE_COMPLETION_MARKER_MISSING"
            for item in index["proof_receipts"]))

    def test_captured_status_is_found_in_the_observer_status_directory(self):
        # Without it the package is excluded as unproven; with it the
        # package proceeds to its prediction artifact (absent in this
        # fixture), which shows the receipt was found.
        def add_status(root, package):
            status = root / "sog_prediction_only/status/2026-10-19/prediction_captured.json"
            status.parent.mkdir(parents=True)
            status.write_text(json.dumps({
                "status": "CAPTURED", "run_dir": str(package),
                "durable_publication_timestamp_utc": "2026-10-19T16:06:00Z"}))
        _, index = self._run(package_roster=None, complete=True)
        self.assertTrue(any(
            item.get("excluded_reason") == "RESILIENT_SOG_SOURCE_RECEIPT_OR_DURABLE_PUBLICATION_UNPROVEN"
            for item in index["proof_receipts"]))
        with self.assertRaisesRegex(RuntimeError, "RESILIENT_SOG_PREDICTION_ARTIFACT_MISSING"):
            self._run(package_roster=None, complete=True, before_run=add_status)

    def test_package_bound_to_an_unverified_roster_still_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "RESILIENT_SOG_SOURCE_READINESS_EVIDENCE_CONFLICT"):
            self._run(package_roster="f" * 64)


class PerRowPregameTests(unittest.TestCase):
    """P2.24 I/G: a package keeps its rows for games that had not started."""

    def test_rows_for_a_started_game_are_dropped_and_the_rest_kept(self):
        schedule = pd.DataFrame({"game_id": [1, 2], "scheduled_start_time_utc": [
            "2026-10-19T23:00:00Z", "2026-10-20T01:00:00Z"]})
        frame = pd.DataFrame({"game_id": [1, 2, 2], "prediction_timestamp_utc": [
            "2026-10-19T23:05:00Z", "2026-10-19T23:05:00Z", "2026-10-19T23:06:00Z"]})
        kept, dropped = core._drop_rows_after_their_game_started(
            frame, schedule, "prediction_timestamp_utc", None)
        self.assertEqual(list(kept.game_id), [2, 2])
        self.assertEqual(dropped, 1)

    def test_publication_time_is_judged_per_game(self):
        schedule = pd.DataFrame({"game_id": [1, 2], "scheduled_start_time_utc": [
            "2026-10-19T23:00:00Z", "2026-10-20T01:00:00Z"]})
        frame = pd.DataFrame({"game_id": [1, 2], "prediction_timestamp_utc": [
            "2026-10-19T22:50:00Z", "2026-10-19T22:50:00Z"]})
        kept, dropped = core._drop_rows_after_their_game_started(
            frame, schedule, "prediction_timestamp_utc", pd.Timestamp("2026-10-19T23:10:00Z"))
        self.assertEqual(list(kept.game_id), [2])
        self.assertEqual(dropped, 1)

    def test_unknown_game_or_timestamp_is_kept_for_the_identity_checks(self):
        schedule = pd.DataFrame({"game_id": [1], "scheduled_start_time_utc": ["2026-10-19T23:00:00Z"]})
        frame = pd.DataFrame({"game_id": [1, 9], "prediction_timestamp_utc": [None, "2026-10-19T22:00:00Z"]})
        kept, dropped = core._drop_rows_after_their_game_started(
            frame, schedule, "prediction_timestamp_utc", None)
        self.assertEqual(len(kept), 2)
        self.assertEqual(dropped, 0)


if __name__ == "__main__":
    unittest.main()
