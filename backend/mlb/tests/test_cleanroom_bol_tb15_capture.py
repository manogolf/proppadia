import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import backend.mlb.scripts.cleanroom_v1.run_cleanroom_bol_tb15_capture as capture

from backend.mlb.scripts.cleanroom_v1.run_cleanroom_bol_tb15_capture import (
    assert_one_event_per_game,
    atomic_publish,
    certify_identity_pilot,
    ensure_paths_absent,
    enforce_current_pacific_date,
    exact_provider_event_ids,
    generate_run_tag,
    require_credentials,
    select_new_raw_run,
    validate_snapshot,
)
from backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 import (
    american_profit,
    baseline,
)


REQUIRED = (
    "bol_tb15_market_sides.csv",
    "bol_tb15_two_sided_markets.csv",
    "lineup_snapshot.csv",
    "identity_audit.csv",
)


def make_snapshot(root: Path, payload: Path, *, rejects=0, sha=None) -> Path:
    root.mkdir()
    (root / "run_manifest.json").write_text(
        json.dumps({"identity_rejects": rejects}) + "\n"
    )
    for name in REQUIRED:
        (root / name).write_text("header\n")
    with (root / "source_hash_manifest.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["raw_payload_path", "sha256"], lineterminator="\n"
        )
        writer.writeheader()
        writer.writerow({
            "raw_payload_path": str(payload),
            "sha256": sha or hashlib.sha256(payload.read_bytes()).hexdigest(),
        })
    return root


def test_unique_run_tag_generation():
    one = generate_run_tag(datetime(2026, 7, 29, 15, 0, 0, tzinfo=timezone.utc))
    two = generate_run_tag(datetime(2026, 7, 29, 15, 0, 1, tzinfo=timezone.utc))
    assert one == "cleanroom_20260729T150000Z"
    assert one != two


def test_current_pacific_date_guard_accepts_current_date():
    at = datetime(2026, 8, 1, 0, 12, tzinfo=timezone.utc)
    enforce_current_pacific_date(date(2026, 7, 31), False, at)


@pytest.mark.parametrize("requested", [date(2026, 7, 30), date(2026, 8, 1)])
def test_current_pacific_date_guard_rejects_prior_and_future(requested):
    at = datetime(2026, 8, 1, 0, 12, tzinfo=timezone.utc)
    with pytest.raises(SystemExit):
        enforce_current_pacific_date(requested, False, at)


def test_explicit_noncurrent_replay_bypasses_only_date_guard():
    at = datetime(2026, 8, 1, 0, 12, tzinfo=timezone.utc)
    enforce_current_pacific_date(date(2026, 7, 30), True, at)


def test_provider_event_date_binding_rejects_different_slate():
    schedule = {"dates": [{"games": [{
        "officialDate": "2026-07-30", "gameDate": "2026-07-30T20:00:00Z",
        "teams": {
            "away": {"team": {"name": "Away"}},
            "home": {"team": {"name": "Home"}},
        },
    }]}]}
    events = [{
        "id": "current-event", "commence_time": "2026-07-31T20:00:00Z",
        "away_team": "Away", "home_team": "Home",
    }]
    assert exact_provider_event_ids(schedule, events, date(2026, 7, 30)) == set()


def test_make_capture_target_does_not_enable_replay_flag():
    makefile = Path("Makefile").read_text()
    target = makefile.split("mlb-cleanroom-bol-tb15-capture:", 1)[1].split("\n\n", 1)[0]
    assert "--allow-noncurrent-date" not in target


def test_source_date_mismatch_preserves_diagnostics_and_records_zero_row_failure(
    tmp_path, monkeypatch
):
    schedule = {"dates": [{"games": [{
        "officialDate": "2026-07-30", "gameDate": "2026-07-30T20:00:00Z",
        "teams": {
            "away": {"team": {"name": "Away"}},
            "home": {"team": {"name": "Home"}},
        },
    }]}]}
    events = [{
        "id": "wrong-day", "commence_time": "2026-07-31T20:00:00Z",
        "away_team": "Away", "home_team": "Home",
    }]

    class Response:
        def __init__(self, payload):
            self.payload = payload
            self.content = json.dumps(payload).encode()

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    responses = iter((Response(schedule), Response(events)))
    monkeypatch.setattr(capture.requests, "get", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(capture, "RAW_ROOT", tmp_path)
    monkeypatch.setenv("ODDS_API_KEY", "test-key")
    recorded = {}

    def record(slate, raw_dir, aggregate_sha, summary):
        recorded.update({"slate": slate, "raw_dir": raw_dir, "summary": summary})
        return "failed-run-id"

    monkeypatch.setattr(capture, "record_failed_source_date_ingestion", record)
    with pytest.raises(RuntimeError, match="SOURCE_DATE_MISMATCH"):
        capture.preflight_source_dates(date(2026, 7, 30), "cleanroom_test")
    assert (recorded["raw_dir"] / "official_schedule.json").exists()
    assert (recorded["raw_dir"] / "provider_events.json").exists()
    assert "exact_provider_events=0" in recorded["summary"]


def test_isolated_raw_run_selection(tmp_path):
    expected = tmp_path / "cleanroom_20260729T150000Z"
    expected.mkdir()
    (tmp_path / "older").mkdir()
    assert select_new_raw_run(tmp_path, expected.name) == expected


def test_run_tag_collision_failure(tmp_path):
    collision = tmp_path / "snapshot"
    collision.mkdir()
    with pytest.raises(RuntimeError, match="collision"):
        ensure_paths_absent([collision])


def test_missing_credentials():
    with pytest.raises(RuntimeError, match="SUPABASE_DB_URL"):
        require_credentials({})
    with pytest.raises(RuntimeError, match="ODDS_API_KEY"):
        require_credentials({"SUPABASE_DB_URL": "present"})


def test_identity_rejection(tmp_path):
    payload = tmp_path / "raw.json"
    payload.write_text("{}")
    snapshot = make_snapshot(tmp_path / "snapshot", payload, rejects=1)
    with pytest.raises(RuntimeError, match="identity rejects"):
        validate_snapshot(snapshot)


def test_missing_source_payload(tmp_path):
    missing = tmp_path / "missing.json"
    snapshot = make_snapshot(tmp_path / "snapshot", missing, sha="0" * 64)
    with pytest.raises(RuntimeError, match="missing source payload"):
        validate_snapshot(snapshot)


def test_sha_mismatch(tmp_path):
    payload = tmp_path / "raw.json"
    payload.write_text("{}")
    snapshot = make_snapshot(tmp_path / "snapshot", payload, sha="0" * 64)
    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        validate_snapshot(snapshot)


def test_failed_snapshot_does_not_refresh_board(tmp_path):
    board = tmp_path / "2026-07-29"
    board.mkdir()
    current = board / "bol_tb15_cleanroom_market_board_2026-07-29.md"
    current.write_text("old\n")
    payload = tmp_path / "raw.json"
    payload.write_text("{}")
    snapshot = make_snapshot(tmp_path / "snapshot", payload, rejects=1)
    with pytest.raises(RuntimeError):
        validate_snapshot(snapshot)
    assert current.read_text() == "old\n"


def test_failed_identity_pilot_publishes_neither_board_nor_run_index(tmp_path):
    board = tmp_path / "board.md"
    index = tmp_path / "run_index.csv"
    board.write_text("certified-board\n")
    index.write_text("certified-index\n")
    with pytest.raises(RuntimeError, match="identity certification failed"):
        certify_identity_pilot({
            "certifiable": False, "event_binding_failures": 188,
            "ambiguous": 0, "unmatched": 0,
        })
    assert board.read_text() == "certified-board\n"
    assert index.read_text() == "certified-index\n"


def test_successful_snapshot_refreshes_board(tmp_path):
    board = tmp_path / "2026-07-29"
    staged = tmp_path / "staged"
    staged.mkdir()
    for name, content in (
        ("bol_tb15_cleanroom_market_board_2026-07-29.csv", "new csv\n"),
        ("bol_tb15_cleanroom_market_board_2026-07-29.md", "new md\n"),
        ("population_manifest.json", "{}\n"),
    ):
        (staged / name).write_text(content)
    staged_index = tmp_path / "staged_index.csv"
    staged_index.write_text("run_tag\nnew\n")
    run_index = tmp_path / "evidence/index.csv"
    atomic_publish(staged, board, staged_index, run_index)
    assert (board / "bol_tb15_cleanroom_market_board_2026-07-29.md").read_text() == "new md\n"
    assert run_index.read_text() == "run_tag\nnew\n"


def test_doubleheader_event_separation():
    valid = [
        {"provider_event_id": "event-1", "game_pk": "100", "decision": "EXACT_UNIQUE_MATCH"},
        {"provider_event_id": "event-2", "game_pk": "101", "decision": "EXACT_UNIQUE_MATCH"},
    ]
    assert_one_event_per_game(valid)
    invalid = [
        {"provider_event_id": "event-1", "game_pk": "100", "decision": "EXACT_UNIQUE_MATCH"},
        {"provider_event_id": "event-1", "game_pk": "101", "decision": "EXACT_UNIQUE_MATCH"},
    ]
    with pytest.raises(RuntimeError, match="reused"):
        assert_one_event_per_game(invalid)


def test_flat_five_american_profit():
    assert american_profit(5, 150) == 7.5
    assert american_profit(5, -200) == 2.5


def test_neutral_baseline_excludes_voids():
    rows = [
        {
            "settlement_status": "SETTLED", "outcome": "OVER_WIN",
            "final_pregame_over_odds": "150",
        },
        {
            "settlement_status": "SETTLED", "outcome": "OVER_LOSS",
            "final_pregame_over_odds": "100",
        },
        {
            "settlement_status": "VOID", "outcome": "NO_ACTION",
            "final_pregame_over_odds": "200",
        },
    ]
    result = baseline(rows, "Over")
    assert result["wagers"] == 2
    assert result["wins"] == 1
    assert result["losses"] == 1
    assert result["total_stake"] == 10
    assert result["net_dollars"] == 2.5
