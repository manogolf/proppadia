"""Production SOG grading picks each game's latest pregame daily run."""
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.nhl import performance_summary
from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.performance_summary import build_production_sog_reference

SLATE = "2026-10-20"
EARLY_GAME, LATE_GAME = 2026020141, 2026020148
GAMES = [EARLY_GAME, LATE_GAME]
STARTS = {EARLY_GAME: "2026-10-20T23:00:00+00:00", LATE_GAME: "2026-10-21T02:00:00+00:00"}
UNVERIFIED: set[str] = set()


@pytest.fixture(autouse=True)
def verified_proofs(monkeypatch):
    """Certificate and roster re-verification are covered by their own tests."""
    def proofs(receipt):
        if receipt["parent_daily_run_id"] in UNVERIFIED:
            raise RuntimeError("DAILY_RECEIPT_PRIOR_CERTIFICATE_UNVERIFIED")
        return {"certificate_sha256": "certificate", "roster_source_sha256": "roster"}
    UNVERIFIED.clear()
    monkeypatch.setattr(performance_summary, "daily_receipt_source_proofs", proofs)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _daily_run(root: Path, run_id: str, ended_at: str, eligible: list[int],
               p_over: float) -> None:
    """Write a READY daily run whose baseline SOG lane predicted ``eligible``."""
    run_dir = root / "daily_runs" / f"run_id={run_id}"
    run_dir.mkdir(parents=True)
    predictions_path = root / "predictions" / run_id / "sog_predictions_wide_calibrated.csv"
    predictions_path.parent.mkdir(parents=True)
    pd.DataFrame([{"game_id": game, "player_id": player, "expected_sog": 2.0,
                   "p_over_1_5": p_over, "p_over_2_5": p_over, "p_over_3_5": p_over}
                  for game in eligible for player in (game * 10 + 1, game * 10 + 2)]
                 ).to_csv(predictions_path, index=False)
    prediction_sha = _sha(predictions_path)
    game_hash = canonical_game_set_hash(GAMES)
    evidence = {"model_family": "poisson_baseline", "model_version": "baseline_v1",
                "scoring_run_id": run_id, "fitted_model_identity_sha256": "model",
                "prediction_artifact_path": str(predictions_path.resolve()),
                "prediction_artifact_sha256": prediction_sha,
                "component_artifacts": [{"role": "scorer", "sha256": "scorer",
                    "canonical_artifact_path": "backend/nhl/scripts/score_sog_poisson_baseline.py"}]}
    receipt = {"slate_date": SLATE, "parent_daily_run_id": run_id, "ended_at_utc": ended_at,
               "canonical_season": 2026, "canonical_game_ids": GAMES,
               "canonical_game_set_hash": game_hash, "eligible_pregame_game_ids": eligible,
               "started_excluded_game_ids": sorted(set(GAMES) - set(eligible)),
               "resolved_phase": "EARLY",
               "lanes": {"legacy_sog": {"status": "COMPLETE", "ended_at_utc": ended_at, "outputs": [{
                   "path": str(predictions_path.resolve()), "sha256": prediction_sha,
                   "canonical_game_set_hash": game_hash,
                   "natural_identity_count": 2 * len(eligible),
                   "fitted_model_evidence": evidence}]}}}
    (run_dir / "parent_receipt.json").write_text(json.dumps(receipt))
    (run_dir / "RUN_COMPLETE.json").write_text(json.dumps(
        {"parent_daily_run_id": run_id, "final_classification": "READY"}))
    (run_dir / "SHA256SUMS").write_text("".join(
        f"{_sha(run_dir / name)}  {name}\n" for name in ("RUN_COMPLETE.json", "parent_receipt.json")))


def _outcomes(root: Path) -> Path:
    package = root / "reconciliation"
    package.mkdir()
    pd.DataFrame([{"canonical_season": 2026, "slate_date": SLATE, "game_id": game,
                   "player_id": game * 10 + offset, "official_final": True, "official_sog": 0,
                   "participation_state": "PARTICIPATED", "outcome_source": "NHL_OFFICIAL",
                   "outcome_source_timestamp_utc": "2026-10-21T05:00:00Z"}
                  for game in GAMES for offset in (1, 2)]
                 ).to_csv(package / "canonical_skater_outcomes.csv", index=False)
    pd.DataFrame([{"game_id": game, "scheduled_start_time_utc": STARTS[game]} for game in GAMES]
                 ).to_csv(package / "canonical_admitted_slate.csv", index=False)
    return package


def test_a_game_a_late_run_excluded_keeps_the_earlier_runs_prediction(tmp_path):
    # Midday predicted both games; the late run came after the early game's start.
    _daily_run(tmp_path, "midday", "2026-10-20T19:00:00Z", GAMES, p_over=0.2)
    _daily_run(tmp_path, "late", "2026-10-20T23:10:00Z", [LATE_GAME], p_over=0.8)
    display, provenance = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes(tmp_path), daily_run_root=tmp_path / "daily_runs")
    identity = display["model_identity"]
    assert identity["eligible_pregame_game_ids"] == GAMES
    assert identity["source_selection"] == "NHL_RESILIENT_PREGAME_FINALIZATION_V1"
    assert [(run["parent_daily_run_id"], run["game_ids"]) for run in identity["source_daily_runs"]] == [
        ("midday", [EARLY_GAME]), ("late", [LATE_GAME])]
    # Every player had 0 shots: midday's UNDER picks win, the late run's OVER picks lose.
    assert (display["overall"]["wins"], display["overall"]["losses"]) == (6, 6)
    assert display["unscored"]["eligible_games_without_predictions"] == []
    assert "sog_reference_prediction_artifact_sha256:midday" in provenance


def test_the_late_run_wins_for_a_game_both_runs_predicted(tmp_path):
    _daily_run(tmp_path, "midday", "2026-10-20T19:00:00Z", GAMES, p_over=0.8)
    _daily_run(tmp_path, "late", "2026-10-20T22:00:00Z", GAMES, p_over=0.2)
    display, provenance = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes(tmp_path), daily_run_root=tmp_path / "daily_runs")
    assert display["model_identity"]["parent_daily_run_id"] == "late"
    assert "source_daily_runs" not in display["model_identity"]
    assert (display["overall"]["wins"], display["overall"]["losses"]) == (12, 0)
    assert not any(":" in key for key in provenance)


def test_a_single_run_slate_is_graded_as_before(tmp_path):
    _daily_run(tmp_path, "late", "2026-10-20T23:10:00Z", [LATE_GAME], p_over=0.2)
    display, _ = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes(tmp_path), daily_run_root=tmp_path / "daily_runs")
    assert display["model_identity"]["eligible_pregame_game_ids"] == [LATE_GAME]
    assert "source_daily_runs" not in display["model_identity"]
    assert display["overall"]["settled"] == 6


def test_a_run_after_a_games_start_never_supplies_that_game(tmp_path):
    # The late run listed both games, but ran after the early game's start.
    _daily_run(tmp_path, "midday", "2026-10-20T19:00:00Z", GAMES, p_over=0.2)
    _daily_run(tmp_path, "late", "2026-10-20T23:10:00Z", GAMES, p_over=0.8)
    display, _ = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes(tmp_path), daily_run_root=tmp_path / "daily_runs")
    assert [(run["parent_daily_run_id"], run["game_ids"])
            for run in display["model_identity"]["source_daily_runs"]] == [
        ("midday", [EARLY_GAME]), ("late", [LATE_GAME])]


def test_a_run_whose_proofs_do_not_verify_contributes_nothing(tmp_path):
    _daily_run(tmp_path, "midday", "2026-10-20T19:00:00Z", GAMES, p_over=0.2)
    _daily_run(tmp_path, "late", "2026-10-20T22:00:00Z", GAMES, p_over=0.8)
    UNVERIFIED.add("late")
    display, _ = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes(tmp_path), daily_run_root=tmp_path / "daily_runs")
    assert display["model_identity"]["parent_daily_run_id"] == "midday"
    assert (display["overall"]["wins"], display["overall"]["losses"]) == (12, 0)


def _outcomes_with_absence(root: Path, *, both_teams_listed: bool) -> Path:
    # Player <game>2 did not dress: he is missing from the official box score.
    package = _outcomes(root)
    rows = [{"canonical_season": 2026, "slate_date": SLATE, "game_id": game,
             "player_id": player, "team_id": team, "official_final": True, "official_sog": 0,
             "participation_state": "PARTICIPATED", "outcome_source": "NHL_OFFICIAL",
             "outcome_source_timestamp_utc": "2026-10-21T05:00:00Z"}
            for game in GAMES for player, team in ((game * 10 + 1, 1), (game * 100, 2))
            if both_teams_listed or team == 1]
    pd.DataFrame(rows).to_csv(package / "canonical_skater_outcomes.csv", index=False)
    return package


def test_a_predicted_skater_absent_from_a_complete_box_score_did_not_play(tmp_path):
    # Same rule as reconciliation (P2.21): not graded and not unresolved.
    _daily_run(tmp_path, "midday", "2026-10-20T19:00:00Z", GAMES, p_over=0.2)
    display, _ = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes_with_absence(tmp_path, both_teams_listed=True),
        daily_run_root=tmp_path / "daily_runs")
    overall = display["overall"]
    assert (overall["wins"], overall["losses"], overall["settled"]) == (6, 0, 6)
    assert overall["unresolved"] == 0
    assert overall["not_gradeable"] == 6  # two absent skaters x three lines


def test_an_absence_without_a_complete_box_score_stays_unresolved(tmp_path):
    _daily_run(tmp_path, "midday", "2026-10-20T19:00:00Z", GAMES, p_over=0.2)
    display, _ = build_production_sog_reference(
        slate_date=SLATE, package=_outcomes_with_absence(tmp_path, both_teams_listed=False),
        daily_run_root=tmp_path / "daily_runs")
    assert display["overall"]["unresolved"] == 6
    assert display["overall"]["not_gradeable"] == 0
