import numpy as np
import pandas as pd
import pytest
import hashlib
import json
import tempfile
from pathlib import Path

from backend.nhl.scripts.grade_nhl_points_hgb_shadow import grade
from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, score
from backend.nhl.scripts.export_nhl_points_hgb_features import build_features, normalize_history, normalize_slate
from backend.nhl.points_hgb_shadow import capture_from_files, grade_prior_hgb_capture
from backend.nhl.daily_capture import verify_package
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.points_hgb_promotion import evaluate_promotion, selected_production_authority


class FixedCountModel:
    def predict(self, x):
        assert list(x.columns) == FEATURES
        return np.array([0.5, 1.5])


def test_frozen_feature_order_scoring_and_poisson_coherence():
    frame = pd.DataFrame({"game_id": [1, 2], "player_id": [11, 22], **{c: [0.0, 1.0] for c in FEATURES}})
    result = score(frame, FixedCountModel())
    assert list(result.columns[-4:]) == ["expected_points", "prob_over_0_5", "prob_over_1_5", "prob_over_2_5"]
    assert np.all(result.prob_over_0_5 >= result.prob_over_1_5)
    assert np.all(result.prob_over_1_5 >= result.prob_over_2_5)


def test_shadow_scorer_fails_closed_on_missing_frozen_feature():
    frame = pd.DataFrame({"game_id": [1], "player_id": [11], **{c: [1.0] for c in FEATURES[:-1]}})
    with pytest.raises(ValueError, match="MISSING_FROZEN_HGB_FEATURES"):
        score(frame, FixedCountModel())


def test_grader_uses_official_goals_plus_assists_and_reports_metrics():
    predictions = pd.DataFrame({"game_id": [1, 2], "player_id": [11, 22], "expected_points": [0.5, 1.5],
        "prob_over_0_5": [0.4, 0.8], "prob_over_1_5": [0.1, 0.4], "prob_over_2_5": [0.01, 0.1]})
    outcomes = pd.DataFrame({"game_id": [1, 2], "player_id": [11, 22], "goals": [0, 1], "assists": [0, 1]})
    report = grade(predictions, outcomes)
    assert report["n"] == 2
    assert report["mean_observed"] == 1
    assert report["crossing_count"] == 0
    assert report["average_threshold_log_loss"] > 0
    assert set(report["thresholds"]) == {"prob_over_0_5", "prob_over_1_5", "prob_over_2_5"}
    assert all("lift" in values for values in report["thresholds"].values())


def test_grader_rejects_population_without_settled_participants():
    predictions = pd.DataFrame({"game_id": [1], "player_id": [11], "expected_points": [1.0],
        "prob_over_0_5": [0.6], "prob_over_1_5": [0.3], "prob_over_2_5": [0.1]})
    outcomes = pd.DataFrame(columns=["game_id", "player_id", "goals", "assists"])
    with pytest.raises(ValueError, match="NO_SETTLED_PARTICIPANTS"):
        grade(predictions, outcomes)


def test_grader_classifies_nonparticipants_and_unresolved_rows():
    predictions = pd.DataFrame({"game_id": [1, 2, 3], "player_id": [11, 22, 33], "expected_points": [0.5, 1.0, 1.2],
        "prob_over_0_5": [0.4, 0.6, 0.7], "prob_over_1_5": [0.1, 0.3, 0.4], "prob_over_2_5": [0.01, 0.1, 0.2]})
    outcomes = pd.DataFrame({"game_id": [1, 2, 3], "player_id": [11, 22, 33], "official_points": [0, None, None],
        "participation_state": ["PARTICIPATED", "SCRATCHED", "UNRESOLVED"], "official_final": [True, True, True]})
    report = grade(predictions, outcomes)
    assert report["participated_graded_count"] == 1
    assert report["nonparticipant_count"] == 1
    assert report["unresolved_count"] == 1
    assert report["identity_reconciled_count"] == 3


def test_operational_feature_builder_enforces_120_day_and_strict_prior_membership():
    outcomes = pd.DataFrame([
        {"canonical_season":2026,"game_date":"2026-05-01","game_id":2025020001,"player_id":11,"team_id":1,"goals":1,"assists":0},
        {"canonical_season":2026,"game_date":"2026-09-30","game_id":2026020001,"player_id":11,"team_id":1,"goals":1,"assists":1},
        {"canonical_season":2026,"game_date":"2026-09-30","game_id":2026020001,"player_id":22,"team_id":1,"goals":0,"assists":0},
        # A same-day realized row in the source must not leak into the target.
        {"canonical_season":2026,"game_date":"2026-10-09","game_id":2026020066,"player_id":11,"team_id":1,"goals":3,"assists":2},
    ])
    logs = pd.DataFrame([
        {"game_id":2025020001,"player_id":11,"team_id":1,"is_home":True,"toi_minutes":20,"pp_toi_minutes":2,"shots_on_goal":9,"shot_attempts":12},
        {"game_id":2026020001,"player_id":11,"team_id":1,"is_home":True,"toi_minutes":30,"pp_toi_minutes":4,"shots_on_goal":3,"shot_attempts":6},
        {"game_id":2026020001,"player_id":22,"team_id":1,"is_home":False,"toi_minutes":25,"pp_toi_minutes":1,"shots_on_goal":2,"shot_attempts":4},
        {"game_id":2026020066,"player_id":11,"team_id":1,"is_home":True,"toi_minutes":99,"pp_toi_minutes":99,"shots_on_goal":99,"shot_attempts":99},
    ])
    slate = pd.DataFrame([{"game_id":2026020066,"player_id":11,"game_date":"2026-10-09",
        "game_start_utc":"2026-10-09T23:00:00Z","is_home":1,"home_team_id":1,"away_team_id":2}])
    features = build_features(normalize_history(logs, outcomes), normalize_slate(slate, 2026)).iloc[0]
    assert features.player_points_last10 == 2
    assert features.current_season_points_prior == 3
    assert features.current_season_games_prior == 2
    assert features.mean_toi_last10 == 30
    assert features.d10_sog_per60 == 6
    assert features.attempts_d10_per60 == 12
    assert features.team_d10_sf_per_game == 5
    assert features.last10_team_sog_share == 0.6


def test_routine_capture_uses_exporter_retains_phoenix_control_and_replays(tmp_path):
    logs = pd.DataFrame([{"game_id":2026020001,"player_id":11,"team_id":1,"is_home":True,
        "toi_minutes":20,"pp_toi_minutes":2,"shots_on_goal":4,"shot_attempts":8}])
    outcomes = pd.DataFrame([{"canonical_season":2026,"game_date":"2026-10-08","game_id":2026020001,
        "player_id":11,"team_id":1,"official_goals":1,"official_assists":0,"official_final":True}])
    slate = pd.DataFrame([{"game_id":2026020066,"player_id":11,"game_date":"2026-10-09",
        "game_start_utc":"2026-10-10T01:00:00Z","is_home":True,"home_team_id":1,"away_team_id":2}])
    phoenix = pd.DataFrame([{"game_id":2026020066,"player_id":11,"line":line,"prob_over":prob}
                            for line,prob in ((0.5,0.7),(1.5,0.4),(2.5,0.1))])
    logs_path, outcomes_path, slate_path, phoenix_path = [tmp_path / n for n in
        ("logs.csv","outcomes.csv","slate.csv","phoenix.csv")]
    logs.to_csv(logs_path,index=False); outcomes.to_csv(outcomes_path,index=False)
    slate.to_csv(slate_path,index=False); phoenix.to_csv(phoenix_path,index=False)
    digest = hashlib.sha256(phoenix_path.read_bytes()).hexdigest()
    result = capture_from_files(logs_path=logs_path,outcomes_paths=[outcomes_path],slate_path=slate_path,
        phoenix_predictions_path=phoenix_path,phoenix_prediction_sha256=digest,
        phoenix_model_identity_sha256="phoenix-model",phoenix_feature_contract_sha256="phoenix-contract",
        phoenix_feature_cutoff_utc="2026-10-09T14:00:00Z",slate_date="2026-10-09",season=2026,
        capture_phase="EARLY",parent_run_id="routine-test",feature_cutoff_utc="2026-10-09T15:00:00Z",
        output_root=tmp_path/"operational")
    capture = Path(result["capture_path"])
    verify_package(capture)
    receipt=json.loads((capture/"receipt.json").read_text())
    assert receipt["status"] == "COMPLETE" and receipt["blocking"] is False
    assert receipt["deterministic_replay"] == "DETERMINISTIC_REPLAY_PASS"
    assert receipt["coherence_crossing_count"] == 0
    assert receipt["phoenix_control"]["prediction_sha256"] == digest
    assert (capture/"model_identity.json").is_file()
    assert (capture/"feature_contract_identity.json").is_file()
    reconciliation=tmp_path/"reconciliation"
    reconciliation.mkdir()
    pd.DataFrame([{"game_id":2026020066,"player_id":11,"official_points":1,
        "official_final":True,"participation_state":"PARTICIPATED",
        "slate_date":"2026-10-09"}]).to_csv(
            reconciliation/"canonical_skater_outcomes.csv",index=False)
    pd.DataFrame([{"game_id":2026020066,"game_type_code":2,"official_final":True}]).to_csv(
        reconciliation/"canonical_game_outcomes.csv",index=False)
    for name in ("graded_moneyline.csv","graded_puck_line.csv"):
        pd.DataFrame([{"game_id":2026020066,"grading_status":"REGULAR_SEASON_GRADED"}]).to_csv(
            reconciliation/name,index=False)
    (reconciliation/"summary.json").write_text(json.dumps({"status":"COMPLETE","slate_date":"2026-10-09"}))
    (reconciliation/"RUN_COMPLETE.json").write_text(json.dumps({"status":"COMPLETE"}))
    files=sorted(x for x in reconciliation.iterdir() if x.is_file())
    (reconciliation/"SHA256SUMS").write_text("".join(
        f"{hashlib.sha256(x.read_bytes()).hexdigest()}  {x.name}\n" for x in files))
    grade_result=grade_prior_hgb_capture(slate_date="2026-10-09",reconciliation_package=reconciliation,
                                         shadow_root=tmp_path/"operational")
    assert grade_result["status"] == "COMPLETE"
    assert grade_result["hgb_metrics"]["participated_graded_count"] == 1
    assert grade_result["phoenix_same_capture_threshold_metrics"]["prob_over_0_5"]["n"] == 1


def test_points_hgb_daily_lane_is_nonblocking_and_authority_defaults_to_phoenix(tmp_path):
    recorder = DailyRunRecorder(run_id="test",command=[],phase="EARLY")
    lane=recorder.lane("points_hgb_shadow")
    assert lane.blocking is False
    authority=selected_production_authority()
    assert authority["production_authority"] == "phoenix_v2"
    gate=tmp_path/"gates.json"
    gate.write_text(json.dumps({"gates":{k:{"status":"PASS"} for k in "ABCDEFGHIJ"}}))
    result=evaluate_promotion(gate_path=gate,integration_evidence_path=tmp_path/"missing.json",
                              shadow_root=tmp_path/"no_grades")
    assert result["classification"] == "NOT_READY_FOR_HGB_PRODUCTION_PROMOTION"


def test_hgb_shadow_failure_does_not_block_phoenix_points_lane():
    recorder = DailyRunRecorder(run_id="test",command=[],phase="EARLY")
    recorder.finish_lane("points",status="COMPLETE")
    recorder.start_lane("points_hgb_shadow")
    recorder.fail_lane("points_hgb_shadow",RuntimeError("fixture failure"),blocking=False)
    assert recorder.lane("points").status == "COMPLETE"
    assert recorder.lane("points_hgb_shadow").status == "FAILED_NONBLOCKING"
    assert recorder.lane("points_hgb_shadow").blocking is False
    assert recorder.classification() == "READY_WITH_BOUNDED_LANE_WARNING"
