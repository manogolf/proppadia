import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.mlb.scripts.cleanroom_v1.run_cleanroom_bol_tb15_capture import (
    assert_one_event_per_game,
    atomic_publish,
    ensure_paths_absent,
    generate_run_tag,
    require_credentials,
    select_new_raw_run,
    validate_snapshot,
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
