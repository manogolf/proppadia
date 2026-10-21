"""Points HGB grading picks each game's latest pregame capture."""
import hashlib
import json
from pathlib import Path

from datetime import datetime, timedelta

import pandas as pd
import pytest

from backend.nhl import points_hgb_shadow
from backend.nhl.points_hgb_shadow import capture_from_files, grade_prior_hgb_capture

SLATE = "2026-10-09"
EARLY_GAME, LATE_GAME = 2026020066, 2026020068
PLAYER = {EARLY_GAME: 11, LATE_GAME: 12}
TEAMS = {EARLY_GAME: (1, 2), LATE_GAME: (3, 4)}
LATER = "2099-01-01T00:00:00+00:00"


@pytest.fixture(autouse=True)
def verified_proofs(monkeypatch):
    """Certificate and roster re-verification are covered by their own tests."""
    monkeypatch.setattr(points_hgb_shadow, "daily_receipt_source_proofs",
                        lambda receipt: {"certificate_sha256": "certificate",
                                         "roster_source_sha256": "roster"})


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    logs = pd.DataFrame([{"game_id": 2026020001, "player_id": PLAYER[game], "team_id": TEAMS[game][0],
                          "is_home": True, "home_team_id": TEAMS[game][0], "away_team_id": TEAMS[game][1],
                          "toi_minutes": 20, "pp_toi_minutes": 2, "shots_on_goal": 4, "shot_attempts": 8}
                         for game in (EARLY_GAME, LATE_GAME)])
    outcomes = pd.DataFrame([{"canonical_season": 2026, "game_date": "2026-10-08", "game_id": 2026020001,
                              "player_id": PLAYER[game], "team_id": TEAMS[game][0], "official_goals": 1,
                              "official_assists": 0, "official_final": True, "official_sog": 4,
                              "outcome_source_timestamp_utc": "2026-10-09T03:00:00Z"}
                             for game in (EARLY_GAME, LATE_GAME)])
    logs_path, outcomes_path = tmp_path / "logs.csv", tmp_path / "outcomes.csv"
    logs.to_csv(logs_path, index=False)
    outcomes.to_csv(outcomes_path, index=False)
    return logs_path, outcomes_path


def _capture(tmp_path: Path, run_id: str, eligible: list[int]) -> str:
    """Retain one HGB capture for ``eligible`` games and its daily receipt.

    Returns the capture time, which is also the lane's publication time.
    """
    logs_path, outcomes_path = _inputs(tmp_path)
    slate_path, phoenix_path = tmp_path / f"slate_{run_id}.csv", tmp_path / f"phoenix_{run_id}.csv"
    pd.DataFrame([{"game_id": game, "player_id": PLAYER[game], "game_date": SLATE,
                   "game_start_utc": "2026-10-10T01:00:00Z", "is_home": True,
                   "home_team_id": TEAMS[game][0], "away_team_id": TEAMS[game][1]}
                  for game in eligible]).to_csv(slate_path, index=False)
    pd.DataFrame([{"game_id": game, "player_id": PLAYER[game], "line": line, "prob_over": prob}
                  for game in eligible for line, prob in ((0.5, 0.7), (1.5, 0.4), (2.5, 0.1))]
                 ).to_csv(phoenix_path, index=False)
    result = capture_from_files(
        logs_path=logs_path, outcomes_paths=[outcomes_path], slate_path=slate_path,
        phoenix_predictions_path=phoenix_path, phoenix_prediction_sha256=_sha(phoenix_path),
        phoenix_model_identity_sha256="phoenix-model", phoenix_feature_contract_sha256="phoenix-contract",
        phoenix_feature_cutoff_utc="2026-10-09T14:00:00Z", slate_date=SLATE, season=2026,
        capture_phase="EARLY", parent_run_id=run_id, feature_cutoff_utc="2026-10-09T15:00:00Z",
        output_root=tmp_path / "operational", canonical_game_ids=[EARLY_GAME, LATE_GAME],
        excluded_started_game_ids=sorted({EARLY_GAME, LATE_GAME} - set(eligible)))
    capture = Path(result["capture_path"])
    receipt = json.loads((capture / "receipt.json").read_text())
    run_dir = tmp_path / "daily_runs" / f"run_id={run_id}"
    run_dir.mkdir(parents=True)
    (run_dir / "parent_receipt.json").write_text(json.dumps({
        "slate_date": SLATE, "parent_daily_run_id": run_id, "lanes": {"points_hgb_shadow": {
            "status": "COMPLETE", "ended_at_utc": receipt["capture_timestamp_utc"], "outputs": [{
                "capture_path": str(capture.resolve()), "manifest_sha256": _sha(capture / "SHA256SUMS"),
                "prediction_sha256": receipt["prediction_sha256"], "feature_sha256": receipt["feature_sha256"],
                "model_identity": receipt["model_identity"],
                "deterministic_replay": "DETERMINISTIC_REPLAY_PASS", "coherence_crossing_count": 0}]}}}))
    (run_dir / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
    (run_dir / "SHA256SUMS").write_text("".join(
        f"{_sha(run_dir / name)}  {name}\n" for name in ("RUN_COMPLETE.json", "parent_receipt.json")))
    return receipt["capture_timestamp_utc"]


def _reconciliation(tmp_path: Path, starts: dict[int, str] | None = None) -> Path:
    package = tmp_path / "reconciliation"
    package.mkdir()
    games = (EARLY_GAME, LATE_GAME)
    pd.DataFrame([{"game_id": game, "player_id": PLAYER[game], "official_points": 1,
                   "official_final": True, "participation_state": "PARTICIPATED", "slate_date": SLATE}
                  for game in games]).to_csv(package / "canonical_skater_outcomes.csv", index=False)
    starts = starts or {game: LATER for game in games}
    pd.DataFrame([{"game_id": game, "slate_date": SLATE, "scheduled_start_time_utc": starts[game]}
                  for game in games]).to_csv(
        package / "canonical_admitted_slate.csv", index=False)
    pd.DataFrame([{"game_id": game, "slate_date": SLATE, "game_type_code": 2, "official_final": True,
                   "official_final_home_goals": 3, "official_final_away_goals": 2,
                   "outcome_source": "NHL_OFFICIAL_GAMECENTER",
                   "outcome_source_timestamp_utc": "2026-10-10T03:00:00Z"} for game in games]).to_csv(
        package / "canonical_game_outcomes.csv", index=False)
    for name in ("graded_moneyline.csv", "graded_puck_line.csv"):
        pd.DataFrame([{"game_id": game, "grading_status": "REGULAR_SEASON_GRADED"} for game in games]
                     ).to_csv(package / name, index=False)
    (package / "summary.json").write_text(json.dumps({"status": "COMPLETE", "slate_date": SLATE}))
    (package / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE"}))
    files = sorted(x for x in package.iterdir() if x.is_file() and x.name != "RUN_COMPLETE.json")
    (package / "SHA256SUMS").write_text("".join(f"{_sha(x)}  {x.name}\n" for x in files))
    return package


def _grade(tmp_path: Path, starts: dict[int, str] | None = None) -> dict:
    return grade_prior_hgb_capture(slate_date=SLATE, reconciliation_package=_reconciliation(tmp_path, starts),
                                   shadow_root=tmp_path / "operational",
                                   daily_run_root=tmp_path / "daily_runs")


def test_a_game_a_late_capture_excluded_keeps_the_earlier_capture(tmp_path):
    _capture(tmp_path, "midday", [EARLY_GAME, LATE_GAME])
    _capture(tmp_path, "late", [LATE_GAME])  # after the early game's start
    result = _grade(tmp_path)
    assert result["status"] == "COMPLETE"
    assert result["source_selection"] == "NHL_RESILIENT_PREGAME_FINALIZATION_V1"
    assert [(item["parent_daily_run_id"], item["game_ids"]) for item in result["source_captures"]] == [
        ("midday", [EARLY_GAME]), ("late", [LATE_GAME])]
    assert result["hgb_metrics"]["participated_graded_count"] == 2
    assert result["selected_side_comparison"]["common_player_game_identities"] == 2
    assert result["phoenix_same_capture_threshold_metrics"]["prob_over_0_5"]["n"] == 2


def test_a_single_contributing_capture_is_graded_as_before(tmp_path):
    _capture(tmp_path, "late", [LATE_GAME])
    result = _grade(tmp_path)
    assert result["status"] == "COMPLETE"
    assert "source_captures" not in result
    assert result["hgb_metrics"]["participated_graded_count"] == 1


def test_a_capture_after_a_games_start_never_supplies_that_game(tmp_path):
    midday = _capture(tmp_path, "midday", [EARLY_GAME, LATE_GAME])
    late = _capture(tmp_path, "late", [EARLY_GAME, LATE_GAME])
    # The early game started between the two captures.
    first, second = (datetime.fromisoformat(value) for value in (midday, late))
    started = (first + (second - first) / 2).isoformat()
    result = _grade(tmp_path, {EARLY_GAME: started, LATE_GAME: LATER})
    assert [(item["parent_daily_run_id"], item["game_ids"]) for item in result["source_captures"]] == [
        ("midday", [EARLY_GAME]), ("late", [LATE_GAME])]
