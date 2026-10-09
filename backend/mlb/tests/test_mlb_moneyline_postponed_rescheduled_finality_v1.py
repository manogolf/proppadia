from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from backend.mlb.public_game_predictions.finality_v1 import (
    NONFINAL,
    NONPLAYABLE_TERMINAL,
    PLAYABLE_TERMINAL,
    STATUS_CONFLICT,
    UNKNOWN_STATUS,
    classify_playable_terminal,
    dependency_blocked_current_games,
    reconcile_schedule_by_game_pk,
)
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import score_schedule_payload
from backend.mlb.public_game_predictions.state_v1 import reconstruct_state
from backend.mlb.scripts import run_mlb_public_game_moneyline_daily_v1 as runner


def status(abstract: str, detailed: str, coded: str, code: str | None = None) -> dict:
    return {
        "abstractGameState": abstract,
        "detailedState": detailed,
        "codedGameState": coded,
        "statusCode": coded if code is None else code,
    }


def schedule_game(
    game_pk: int,
    state: dict,
    *,
    start: str = "2026-09-23T17:35:00Z",
    official_date: str = "2026-09-23",
    away: int = 141,
    home: int = 110,
    game_number: int = 1,
    double_header: str = "N",
    **relations,
) -> dict:
    return {
        "gamePk": game_pk,
        "gameDate": start,
        "officialDate": official_date,
        "gameNumber": game_number,
        "doubleHeader": double_header,
        "status": state,
        "teams": {
            "away": {"team": {"id": away, "name": f"Away {away}"}},
            "home": {"team": {"id": home, "name": f"Home {home}"}},
        },
        **relations,
    }


def payload(*games: dict) -> dict:
    return {"dates": [{"date": "2026-09-23", "games": list(games)}]}


def final_feed(game_pk: int = 824785, *, away: int = 141, home: int = 110) -> dict:
    return {
        "gamePk": game_pk,
        "gameData": {
            "status": status("Final", "Final", "F"),
            "datetime": {"officialDate": "2026-09-23", "dateTime": "2026-09-23T17:35:00Z"},
            "game": {"gameNumber": 1},
            "teams": {"away": {"id": away}, "home": {"id": home}},
        },
        "liveData": {
            "linescore": {"teams": {"away": {"runs": 2}, "home": {"runs": 4}}},
            "plays": {"allPlays": [{"about": {"endTime": "2026-09-23T20:31:00Z"}}]},
        },
    }


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        (status("Final", "Final", "F"), PLAYABLE_TERMINAL),
        (status("Final", "Game Over", "O"), PLAYABLE_TERMINAL),
        (status("Final", "Postponed", "D", "DR"), NONPLAYABLE_TERMINAL),
        (status("Final", "Cancelled", "C"), NONPLAYABLE_TERMINAL),
        (status("Final", "Suspended", "D"), NONPLAYABLE_TERMINAL),
        (status("Preview", "Delayed", "D"), NONPLAYABLE_TERMINAL),
        (status("Preview", "Scheduled", "S"), NONFINAL),
        (status("Preview", "Pre-Game", "P"), NONFINAL),
        (status("Live", "In Progress", "I"), NONFINAL),
        (status("Final", "Final", "O"), STATUS_CONFLICT),
        ({"abstractGameState": "Final"}, UNKNOWN_STATUS),
    ],
)
def test_complete_status_tuple_fails_closed(fields, expected):
    assert classify_playable_terminal(fields).classification == expected


def test_scores_never_establish_finality():
    value = status("Preview", "Scheduled", "S")
    value["homeScore"] = 12
    value["awayScore"] = 2
    assert classify_playable_terminal(value).classification == NONFINAL


def test_824785_sequence_selects_only_eventual_makeup_final(monkeypatch, tmp_path):
    original = schedule_game(
        824785, status("Final", "Postponed", "D", "DR"),
        start="2026-09-22T22:35:00Z", official_date="2026-09-23",
        rescheduleDate="2026-09-23T17:35:00Z",
    )
    for current_status in (
        status("Preview", "Scheduled", "S"),
        status("Preview", "Pre-Game", "P"),
        status("Live", "In Progress", "I"),
    ):
        makeup = schedule_game(
            824785, current_status, double_header="S",
            rescheduledFrom="2026-09-22T22:35:00Z", description="Makeup of 9/22 PPD",
        )
        decision = reconcile_schedule_by_game_pk(payload(original, makeup))[0]
        assert decision.decision == "REJECTED_NONPLAYABLE"
        assert decision.selected is None

    makeup_final = schedule_game(
        824785, status("Final", "Final", "F"), double_header="S",
        rescheduledFrom="2026-09-22T22:35:00Z", description="Makeup of 9/22 PPD",
    )
    calls = []
    raw = json.dumps(final_feed(), sort_keys=True).encode()
    monkeypatch.setattr(runner, "_fetch_game_feed", lambda game_pk: (calls.append(game_pk) or final_feed(), raw))
    collection = runner.collect_official_finals(
        payload(original, makeup_final), retained_source_dir=tmp_path / "feeds",
    )
    assert calls == [824785]
    assert len(collection.finals) == 1
    assert collection.finals[0].game_pk == 824785
    assert collection.decisions[0]["appearances"][0]["status"]["classification"] == NONPLAYABLE_TERMINAL
    assert collection.decisions[0]["final_decision"] == "ADMITTED_PLAYABLE_FINAL"


def test_makeup_pregame_remains_current_slate_prediction_eligible():
    makeup = schedule_game(
        824785, status("Preview", "Scheduled", "S"), double_header="S",
        rescheduledFrom="2026-09-22T22:35:00Z",
    )
    history = reconcile_schedule_by_game_pk(payload(makeup))[0]
    assert history.decision == "REJECTED_NONPLAYABLE" and not history.dependency_team_ids
    state_snapshot = reconstruct_state(
        [], prediction_cutoff_utc="2026-09-23T12:00:00Z",
        state_generated_at_utc="2026-09-23T12:00:00Z",
    )
    rows = score_schedule_payload(
        payload(makeup), prediction_timestamp_utc="2026-09-23T12:00:00Z",
        source_schedule_hash="a" * 64, team_state_snapshot=state_snapshot,
    )
    assert rows[0]["admission_status"] == "ADMITTED_SHADOW"


def test_changed_official_date_requires_authoritative_reschedule_relationship():
    abandoned = schedule_game(
        824785, status("Final", "Postponed", "D", "DR"),
        start="2026-09-22T22:35:00Z", official_date="2026-09-22",
    )
    later_final = schedule_game(824785, status("Final", "Final", "F"))
    decision = reconcile_schedule_by_game_pk(payload(abandoned, later_final))[0]
    assert decision.decision == "QUARANTINED"
    assert decision.reason == "CONFLICTING_REPEATED_SCHEDULE_APPEARANCES"


@pytest.mark.parametrize(
    "game",
    [
        schedule_game(1, status("Final", "Postponed", "D", "DR"), rescheduleDate=""),
        schedule_game(2, status("Final", "Cancelled", "C")),
    ],
)
def test_abandoned_nonplayed_games_never_attach_outcomes_or_block_state(game):
    decision = reconcile_schedule_by_game_pk(payload(game))[0]
    assert decision.decision == "REJECTED_NONPLAYABLE"
    assert decision.selected is None and not decision.dependency_team_ids


def test_suspended_game_is_quarantined_as_strict_prior_dependency():
    game = schedule_game(3, status("Final", "Suspended", "D"))
    decision = reconcile_schedule_by_game_pk(payload(game))[0]
    assert decision.decision == "QUARANTINED"
    assert decision.dependency_team_ids == (110, 141)


def test_suspended_game_becomes_eligible_only_after_related_resume_is_final(monkeypatch, tmp_path):
    suspended = schedule_game(
        4, status("Final", "Suspended", "D"),
        start="2026-09-22T22:35:00Z", official_date="2026-09-22",
        resumeDate="2026-09-23T17:35:00Z",
    )
    resumed = schedule_game(
        4, status("Final", "Final", "F"), resumedFrom="2026-09-22T22:35:00Z",
    )
    feed = final_feed(4)
    raw = json.dumps(feed, sort_keys=True).encode()
    monkeypatch.setattr(runner, "_fetch_game_feed", lambda _game_pk: (feed, raw))
    collection = runner.collect_official_finals(
        payload(suspended, resumed), retained_source_dir=tmp_path / "feeds",
    )
    assert len(collection.finals) == 1
    assert collection.decisions[0]["final_decision"] == "ADMITTED_PLAYABLE_FINAL"


def test_doubleheader_game_two_is_blocked_when_game_one_feed_is_inconsistent(monkeypatch, tmp_path):
    first = schedule_game(10, status("Final", "Final", "F"), game_number=1, double_header="S")
    second = schedule_game(
        11, status("Preview", "Scheduled", "S"), game_number=2, double_header="S",
        start="2026-09-24T01:00:00Z",
    )
    nonfinal = final_feed(10)
    nonfinal["gameData"]["status"] = status("Live", "In Progress", "I")
    raw = json.dumps(nonfinal, sort_keys=True).encode()
    monkeypatch.setattr(runner, "_fetch_game_feed", lambda _game_pk: (nonfinal, raw))
    collection = runner.collect_official_finals(payload(first), retained_source_dir=tmp_path / "feeds")
    assert collection.quarantined_count == 1
    blocked = dependency_blocked_current_games(payload(first, second), collection.dependency_team_ids)
    assert blocked == {10, 11}


def test_unknown_identity_forces_broader_fail_closed_dependency():
    unknown = schedule_game(12, {"abstractGameState": "Final"})
    unknown["teams"] = {}
    decision = reconcile_schedule_by_game_pk(payload(unknown))[0]
    assert decision.decision == "QUARANTINED"
    collection = runner.FinalCollection((), (decision.as_dict() | {"final_decision": "QUARANTINED"},), (), False)
    assert not collection.dependency_isolation_proven


def test_missing_game_pk_is_retained_as_global_fail_closed_quarantine():
    missing = schedule_game(12, status("Final", "Final", "F"))
    missing.pop("gamePk")
    decision = reconcile_schedule_by_game_pk(payload(missing))[0]
    assert decision.game_pk < 0
    assert decision.as_dict()["game_pk"] is None
    assert decision.decision == "QUARANTINED"
    assert decision.reason == "EXACT_GAME_PK_MISSING"


def test_independent_current_game_continues():
    dependent = schedule_game(20, status("Preview", "Scheduled", "S"), away=141, home=110)
    independent = schedule_game(21, status("Preview", "Scheduled", "S"), away=144, home=147)
    assert dependency_blocked_current_games(payload(dependent, independent), {110, 141}) == {20}


def test_one_inconsistent_feed_does_not_suppress_independent_final(monkeypatch, tmp_path):
    affected = schedule_game(70, status("Final", "Final", "F"), away=141, home=110)
    independent = schedule_game(71, status("Final", "Final", "F"), away=144, home=147)
    bad_feed = final_feed(70, away=141, home=110)
    bad_feed["gameData"]["status"] = status("Live", "In Progress", "I")
    good_feed = final_feed(71, away=144, home=147)
    feeds = {70: bad_feed, 71: good_feed}
    calls = []

    def fetch(game_pk):
        calls.append(game_pk)
        feed = feeds[game_pk]
        return feed, json.dumps(feed, sort_keys=True).encode()

    monkeypatch.setattr(runner, "_fetch_game_feed", fetch)
    collection = runner.collect_official_finals(
        payload(affected, independent), retained_source_dir=tmp_path / "feeds",
    )
    assert calls == [70, 71]
    assert [row.game_pk for row in collection.finals] == [71]
    assert collection.quarantined_count == 1
    assert collection.dependency_team_ids == (110, 141)


def test_post_start_rejection_is_preserved():
    started = schedule_game(30, status("Live", "In Progress", "I"), start="2026-09-23T11:00:00Z")
    state_snapshot = reconstruct_state(
        [], prediction_cutoff_utc="2026-09-23T12:00:00Z",
        state_generated_at_utc="2026-09-23T12:00:00Z",
    )
    row = score_schedule_payload(
        payload(started), prediction_timestamp_utc="2026-09-23T12:00:00Z",
        source_schedule_hash="a" * 64, team_state_snapshot=state_snapshot,
    )[0]
    assert row["admission_status"] == "REJECTED_FAIL_CLOSED"
    assert row["failure_reason"] == "PREGAME_CUTOFF_FAILED"


def test_repeated_final_appearance_fetches_once(monkeypatch, tmp_path):
    final = schedule_game(40, status("Final", "Final", "F"))
    calls = []
    feed = final_feed(40)
    raw = json.dumps(feed, sort_keys=True).encode()
    monkeypatch.setattr(runner, "_fetch_game_feed", lambda game_pk: (calls.append(game_pk) or feed, raw))
    result = runner.collect_official_finals(payload(final, dict(final)), retained_source_dir=tmp_path / "feeds")
    assert calls == [40]
    assert len(result.finals) == 1


def test_no_quarantine_preserves_prediction_rows_byte_for_byte():
    fixture = Path("backend/mlb/tests/fixtures/public_game_predictions_v1/august6_schedule.json")
    schedule = json.loads(fixture.read_text())
    state_snapshot = reconstruct_state(
        [], prediction_cutoff_utc="2026-08-06T00:00:00Z",
        state_generated_at_utc="2026-08-06T00:00:00Z",
    )
    original = score_schedule_payload(
        schedule, prediction_timestamp_utc="2026-08-06T00:00:00Z",
        source_schedule_hash="a" * 64, team_state_snapshot=state_snapshot,
    )
    corrected = runner._apply_dependency_blocks(original, set(), block_all=False)
    assert corrected == original


def test_history_raw_and_selection_receipt_are_deterministic(tmp_path):
    raw = json.dumps(payload(schedule_game(50, status("Final", "Postponed", "D", "DR"))), sort_keys=True).encode()
    query = runner._schedule_query("2026-08-05", "2026-09-23")
    source = runner._retain_history_schedule(
        raw, retrieved_at_utc="2026-09-23T23:34:00Z", query=query, root=tmp_path,
    )
    decision = reconcile_schedule_by_game_pk(json.loads(raw))[0]
    collection = runner.FinalCollection(
        (), (decision.as_dict() | {"final_decision": decision.decision, "final_reason": decision.reason},), (), True,
    )
    first = runner._write_selection_receipt(source, collection, root=tmp_path)
    second = runner._write_selection_receipt(source, collection, root=tmp_path)
    assert first["path"] == second["path"]
    assert first["sha256"] == second["sha256"]
    assert Path(source["source_path"]).read_bytes() == raw


def test_agreement_barrier_requires_valid_published_artifact():
    hook = Path("bin/mlb_public_game_moneyline_daily_hook.sh").read_text()
    assert '"${barrier_status}" != VALID_*' in hook
    assert "lifecycle_rc=3" in hook
    assert "retained_attempt=" in hook


def test_main_reports_atomic_publication_counts_without_external_requests(monkeypatch, tmp_path):
    current = payload(schedule_game(
        60, status("Preview", "Scheduled", "S"), start="2099-09-23T17:35:00Z",
    ))
    responses = [(current, json.dumps(current).encode()), ({"dates": []}, b'{"dates":[]}')]
    monkeypatch.setattr(runner, "_fetch_schedule", lambda *_args: responses.pop(0))
    monkeypatch.setattr(runner, "append_official_finals", lambda rows: 0)
    monkeypatch.setattr(runner, "load_official_finals_before", lambda *_args: [])
    monkeypatch.setattr(runner, "fetch_ungraded_final_predictions", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(runner, "append_state_snapshot", lambda _row: True)
    inserted = []
    monkeypatch.setattr(runner, "append_prediction_rows", lambda rows, **_kwargs: inserted.extend(rows) or len(rows))
    monkeypatch.setattr(runner, "designated_snapshot_exists", lambda *_args: False)
    output = tmp_path / "result.json"
    monkeypatch.setattr(sys, "argv", [
        "runner", "--mlb-date", "2026-09-23", "--write-durable",
        "--retained-source-dir", str(tmp_path / "feeds"),
        "--retained-history-dir", str(tmp_path / "history"),
        "--output-json", str(output),
    ])
    assert runner.main() == 0
    result = json.loads(output.read_text())
    assert result["publication_counts"] == {
        "admitted": 1, "quarantined": 0, "dependency_blocked": 0, "post_start": 0,
    }
    assert result["agreement_barrier_status"] == "VALID_IMMUTABLE_MONEYLINE_ARTIFACT"
    assert len(inserted) == 1
    assert responses == []
