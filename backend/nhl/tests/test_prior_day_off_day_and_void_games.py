"""P2.24 C and D: off-day prior certificates and void (PPD/SUSP/CNCL) games.

C: after a zero-game prior day the daily receipt and observers carry a
VERIFIED_EMPTY_SCHEDULE certificate with no package. Resilient grading must
verify it against the retained empty schedule and keep every lane.

D: a postponed, suspended or cancelled game is void. Its predictions are
never graded; the played games of the slate grade normally.
"""
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
from backend.nhl.reconciliation_certificate import (
    certificate_identity, verified_empty_schedule)
from backend.nhl.sog_cold_start.core import digest as sog_digest

SLATE = "2026-12-27"
PRIOR = "2026-12-26"
GAMES = {2026020500: ("2026-12-28T00:00:00Z", 77, 88),
         2026020501: ("2026-12-28T01:00:00Z", 78, 89)}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(run: Path) -> None:
    (run / "SHA256SUMS").write_text("".join(
        f"{sha(path)}  {path.name}\n" for path in sorted(run.iterdir())
        if path.name != "SHA256SUMS"))


def write_empty_prior_schedule(root: Path) -> dict:
    day = root / "slates" / PRIOR
    day.mkdir(parents=True)
    raw = day / "raw_schedule_response.json"
    raw.write_text(json.dumps({"gameWeek": [{"date": PRIOR, "games": []}]}))
    (day / "slate_health.json").write_text(json.dumps({
        "slate_date": PRIOR, "completion_status": "VALID_EMPTY_SLATE",
        "downstream_ready": True, "normalized_game_count": 0,
        "source": f"https://api-web.nhle.com/v1/schedule/{PRIOR}",
        "raw_source_hash": sha(raw)}))
    return verified_empty_schedule(PRIOR, slate_root=root / "slates")


def build_slate(root: Path, certificate: dict) -> None:
    """Retain one capture per lane for every game in GAMES, bound to certificate."""
    roster = root / "roster/observation=one"
    roster.mkdir(parents=True)
    (roster / "SHA256SUMS").write_text("fixture manifest\n")
    (roster / "observation_summary.json").write_text(
        json.dumps({"season": 2026, "phase": "MIDDAY"}))
    roster_manifest = sha(roster / "SHA256SUMS")
    readiness = {"prior_day_reconciliation_certificate": certificate,
                 "current_slate_roster_observation": {"package_manifest_sha256": roster_manifest}}
    contract = core._sha(core.SOG_CONTRACT_PATH)
    spine = pd.DataFrame([{"game_id": game_id, "scheduled_start_time_utc": start}
                          for game_id, (start, _, _) in GAMES.items()])
    common = {"slate_date": SLATE, "canonical_season": 2026, "game_type_code": 2,
              "prediction_timestamp_utc": "2026-12-27T18:40:00Z",
              "input_cutoff_timestamp_utc": "2026-12-27T18:30:00Z"}
    lanes = {"POINTS": [], "SAVES": [], "SOG": []}
    for game_id, (start, player, goalie) in GAMES.items():
        lanes["POINTS"] += [{
            **common, "run_id": "points1", "game_id": game_id, "player_id": player,
            "line": line, "prob_over": .4, "model_version": "POINTS_V1",
            "probability_construction": "COUNT_DISTRIBUTION", "phase": "MIDDAY",
            "feature_cutoff_timestamp_utc": "2026-12-27T18:30:00Z",
            "scheduled_start_time_utc": start, "prediction_eligible": True,
            "market_qualified": False, "price": None,
            "prediction_identity": prediction_only_digest({
                "run_id": "points1", "game_id": game_id, "player_id": player,
                "line": float(line), "model_version": "POINTS_V1",
                "probability_construction": "COUNT_DISTRIBUTION"})}
            for line in (.5, 1.5, 2.5)]
        lanes["SAVES"] += [{
            **common, "run_id": "saves1", "game_id": game_id, "goalie_id": goalie,
            "line": line, "prob_over": .4, "model_version": "SAVES_V1",
            "prediction_semantics": "CONDITIONAL_STARTER", "phase": "MIDDAY",
            "scheduled_start_time_utc": start, "expected_saves": 25.0,
            "market_qualified": False, "price": None,
            "prediction_identity": prediction_only_digest({
                "run_id": "saves1", "game_id": game_id, "goalie_id": goalie,
                "line": float(line), "model_version": "SAVES_V1"})}
            for line in (18.5 + n for n in range(13))]
        lanes["SOG"].append({
            "canonical_season": 2026, "slate_date": SLATE, "game_id": game_id,
            "player_id": player, "line": 2.5, "phase": "MIDDAY",
            "prediction_timestamp_utc": "2026-12-27T18:40:00Z",
            "feature_cutoff_utc": "2026-12-27T18:30:00Z",
            "input_cutoff_utc": "2026-12-27T18:30:00Z",
            "scheduled_start_time_utc": start, "game_type_code": 2,
            "contract_arm": "A_BASE", "contract_sha256": contract,
            "selected_side": "OVER", "prob_over": .6,
            "prediction_identity": sog_digest({
                "contract": contract, "arm": "A_BASE", "phase": "MIDDAY",
                "slate_date": SLATE, "game_id": game_id, "player_id": player,
                "line": 2.5})})
    links = {}
    for lane, rows in lanes.items():
        run = root / f"{lane.lower()}_prediction_only/season=2026/slate_date={SLATE}/phase=MIDDAY/run_id={lane.lower()}1"
        run.mkdir(parents=True)
        name = "immutable_conditional_predictions.csv" if lane == "SAVES" else "immutable_predictions.csv"
        pd.DataFrame(rows).to_csv(run / name, index=False)
        spine.to_csv(run / "canonical_game_spine.csv", index=False)
        (run / "run_metadata.json").write_text(json.dumps({
            "run_id": f"{lane.lower()}1", "lane": lane, "slate_date": SLATE,
            "phase": "MIDDAY", "readiness_evidence": readiness}))
        (run / "RUN_COMPLETE.json").write_text(json.dumps({
            "status": "COMPLETE", "completed_at_utc": "2026-12-27T18:42:00Z"}))
        write_manifest(run)
        status = root / f"{lane.lower()}_prediction_only/status/{SLATE}/capture.json"
        status.parent.mkdir(parents=True)
        status.write_text(json.dumps({
            "status": "CAPTURED", "slate_date": SLATE, "phase": "MIDDAY",
            "run_dir": str(run), "prior_day_reconciliation_certificate": certificate,
            "durable_publication_timestamp_utc": "2026-12-27T18:42:01Z"}))
        links[lane] = {"status": "CAPTURED", "receipt_path": str(status)}

    source = root / f"cross_market_shadow/season=2026/slate_date={SLATE}/run_type=REFRESH/state=state1"
    source.mkdir(parents=True)
    base = [{"game_id": game_id, "slate_date": SLATE, "scheduled_start_time_utc": start,
             "prediction_creation_time_utc": "2026-12-27T18:40:00Z",
             "home_team": "AAA", "away_team": "BBB"} for game_id, (start, _, _) in GAMES.items()]
    pd.DataFrame([{**row, "challenger_favored_team": "AAA",
                   "challenger_home_win_probability": .6, "challenger_away_win_probability": .4,
                   "model_version": "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3",
                   "model_parameter_sha256": "modelsha", "operational_role": "PRODUCTION_CHAMPION"}
                  for row in base]).to_csv(source / "moneyline_production_predictions.csv", index=False)
    pd.DataFrame([{**row, "model_favored_team": "BBB"} for row in base]).to_csv(
        source / "moneyline_schedule_logit_v2_shadow_predictions.csv", index=False)
    pd.DataFrame([{**row, "away_by_2_plus_probability": .2, "one_goal_game_probability": .5,
                   "home_by_2_plus_probability": .3} for row in base]).to_csv(
        source / "puck_line_v1_immutable_predictions.csv", index=False)
    (source / "daily_execution_status.json").write_text(json.dumps({
        "slate_date": SLATE, "run_type": "REFRESH", "substantive_state_sha256": "state1",
        "moneyline_production_model": "ML_V3", "moneyline_shadow_model": "ML_V2",
        "moneyline_shot_finishing_challenger_v3_feature_policy": "ML_FEATURE_V3",
        "puck_line_v2_shot_prior_feature_policy": "PL_FEATURE_V1"}))
    write_manifest(source)
    receipt = root / "daily_runs/run_id=run1/parent_receipt.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({
        "slate_date": SLATE, "feature_cutoff_utc": "2026-12-27T18:35:00Z",
        "ended_at_utc": "2026-12-27T18:45:00Z",
        "prior_day_learning": {"certificate": certificate},
        "roster_observation": {"path": str(roster), "manifest_sha256": roster_manifest},
        "lanes": {"moneyline": {"ended_at_utc": "2026-12-27T18:44:00Z", "outputs": [
            {"path": str(source / name), "sha256": sha(source / name)}
            for name in ("moneyline_production_predictions.csv",
                         "moneyline_schedule_logit_v2_shadow_predictions.csv")]}},
    }))
    parent = root / f"cross_market_shadow/orchestration_status/{SLATE}/capture.json"
    parent.parent.mkdir(parents=True)
    parent.write_text(json.dumps({
        "slate_date": SLATE, "player_prop_observers": links,
        "prior_day_reconciliation_certificate": certificate,
        "current_slate_roster_observation": {
            "status": "VERIFIED_CURRENT_SLATE_ROSTER", "package_path": str(roster),
            "package_manifest_sha256": roster_manifest}}))


def outcomes(game_ids):
    schedule = pd.DataFrame([{"game_id": game_id, "scheduled_start_time_utc": GAMES[game_id][0],
                              "game_type_code": 2} for game_id in GAMES])
    games = pd.DataFrame([{"game_id": game_id, "official_full_game_winner": "HOME",
                           "official_final_home_goals": 3, "official_final_away_goals": 2}
                          for game_id in game_ids])
    skaters = pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": game_id,
        "player_id": GAMES[game_id][1], "official_final": True, "official_sog": 3,
        "official_goals": 1, "official_assists": 0, "participation_state": "PARTICIPATED",
        "outcome_source": "NHL_OFFICIAL_GAMECENTER",
        "outcome_source_timestamp_utc": "2026-12-28T04:00:00Z"} for game_id in game_ids])
    goalies = pd.DataFrame([{
        "game_id": game_id, "goalie_id": GAMES[game_id][2], "team_id": 1,
        "official_saves": 24, "actual_start_flag": True,
        "goalie_participation_state": "STARTER", "starter_identity_method": "OFFICIAL"}
        for game_id in game_ids])
    return schedule, games, skaters, goalies


def grade(root: Path, game_ids, **kwargs):
    schedule, games, skaters, goalies = outcomes(game_ids)
    roster_validator = lambda path, **_: {"manifest_sha256": sha(Path(path) / "SHA256SUMS")}
    with patch("backend.nhl.daily_orchestration.verify_roster_observation_reuse",
               side_effect=roster_validator), \
            patch.object(core, "_verify_cross_market_identities"), \
            patch("backend.nhl.cross_market_shadow.moneyline_challenger.load_model",
                  return_value={"parameter_file_sha256": "modelsha"}):
        return core._resilient_cross_market_grades(
            slate_date=SLATE, operational_root=root, schedule=schedule,
            games=games, skaters=skaters, goalies=goalies, **kwargs)


LANES = ("MONEYLINE", "PUCK_LINE", "SOG", "POINTS", "SAVES")


class OffDayPriorCertificateTests(unittest.TestCase):
    """P2.24 C: a zero-game prior day (first real one: the Christmas break)."""

    def test_empty_schedule_certificate_has_a_stable_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            certificate = write_empty_prior_schedule(Path(temp))
            self.assertEqual(certificate["status"], "VERIFIED_EMPTY_SCHEDULE")
            self.assertEqual(certificate_identity(certificate), certificate["schedule_sha256"])
            self.assertEqual(certificate_identity(
                {"status": "CERTIFIED_FINAL", "package_manifest_sha256": "abc"}), "abc")
            self.assertIsNone(certificate_identity({"status": "VERIFIED_EMPTY_SCHEDULE"}))

    def test_slate_after_an_off_day_grades_every_resilient_lane(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "operational"
            certificate = write_empty_prior_schedule(root)
            build_slate(root, certificate)
            grades, index = grade(root, list(GAMES))
        for lane in LANES:
            self.assertEqual(index["lanes"][lane]["coverage_status"], "FULL", lane)
        selected = {row["certificate_sha256"] for lane in LANES
                    for row in index["lanes"][lane]["selected"]}
        self.assertEqual(selected, {certificate["schedule_sha256"]})
        self.assertEqual(len(grades["resilient_moneyline_production_champion"]), 2)
        self.assertEqual(len(grades["resilient_points"]), 6)
        self.assertEqual(len(grades["resilient_saves"]), 26)
        self.assertEqual(len(grades["resilient_sog"]), 2)

    def test_changed_empty_schedule_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "operational"
            certificate = write_empty_prior_schedule(root)
            build_slate(root, {**certificate, "schedule_sha256": "0" * 64})
            _, index = grade(root, list(GAMES))
        for lane in ("MONEYLINE", "PUCK_LINE"):
            self.assertEqual(index["lanes"][lane]["coverage_status"], "EMPTY", lane)


if __name__ == "__main__":
    unittest.main()
