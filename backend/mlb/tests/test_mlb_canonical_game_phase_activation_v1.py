from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.mlb.scripts.build_mlb_canonical_game_phase_backfill_v1 import (
    build_proposal,
    discover_inputs,
    write_outputs,
)
from backend.mlb.season_transition.canonical_phase_v1 import (
    CanonicalGamePhaseIndex,
    canonical_phase_record,
    cleanroom_game_insert_sql,
)
from backend.mlb.season_transition.contract_v1 import PhaseContractError


ROOT = Path(__file__).resolve().parents[3]


def game(game_pk: int, game_type: str | None, **extra: object) -> dict:
    row = {
        "gamePk": game_pk,
        "season": "2026",
        "officialDate": "2026-11-15",
        "gameDate": "2026-11-15T01:00:00Z",
        "seriesDescription": "source round exactly",
    }
    if game_type is not None:
        row["gameType"] = game_type
    row.update(extra)
    return row


@pytest.mark.parametrize(
    ("raw_type", "round_name"),
    [
        ("F", "WILD_CARD"),
        ("D", "DIVISION_SERIES"),
        ("L", "LEAGUE_CHAMPIONSHIP_SERIES"),
        ("W", "WORLD_SERIES"),
        ("P", "PLAYOFFS_UNSPECIFIED"),
        ("C", "CHAMPIONSHIP_UNSPECIFIED"),
    ],
)
def test_all_postseason_rounds(raw_type: str, round_name: str) -> None:
    record = canonical_phase_record(game(1, raw_type), source_sha256="a" * 64)
    assert record["source_game_type"] == raw_type
    assert record["season_phase"] == "POSTSEASON"
    assert record["postseason_round"] == round_name
    assert record["source_round"] == "source round exactly"


def test_regular_after_nominal_close_remains_regular() -> None:
    record = canonical_phase_record(game(2, "R"), source_sha256="b" * 64)
    assert record["season_phase"] == "REGULAR_SEASON"
    assert record["postseason_round"] is None


def test_postponed_rescheduled_relationships_are_exact() -> None:
    record = canonical_phase_record(
        game(
            3,
            "R",
            rescheduledFrom="2026-09-20T20:10:00Z",
            rescheduledFromDate="2026-09-20",
            rescheduleDate="2026-10-03T20:10:00Z",
            rescheduleGameDate="2026-10-03",
        ),
        source_sha256="c" * 64,
    )
    assert record["season_phase"] == "REGULAR_SEASON"
    assert record["schedule_relationships"] == {
        "rescheduledFrom": "2026-09-20T20:10:00Z",
        "rescheduledFromDate": "2026-09-20",
        "rescheduleDate": "2026-10-03T20:10:00Z",
        "rescheduleGameDate": "2026-10-03",
    }


def test_suspended_resumed_relationships_are_exact() -> None:
    record = canonical_phase_record(
        game(
            4,
            "R",
            resumedFrom=998,
            resumedFromDate="2026-09-18",
            resumeDate="2026-10-04T20:10:00Z",
            resumeGameDate="2026-10-04",
        ),
        source_sha256="d" * 64,
    )
    assert record["season_phase"] == "REGULAR_SEASON"
    assert record["schedule_relationships"]["resumedFrom"] == 998
    assert set(record["schedule_relationships"]) == {
        "resumedFrom",
        "resumedFromDate",
        "resumeDate",
        "resumeGameDate",
    }


@pytest.mark.parametrize("raw_type", ["A", "N"])
def test_special_games_are_preserved_but_excluded(raw_type: str) -> None:
    record = canonical_phase_record(game(5, raw_type), source_sha256="e" * 64)
    assert record["source_game_type"] == raw_type
    assert record["season_phase"] is None
    index = CanonicalGamePhaseIndex()
    index.add(record)
    with pytest.raises(PhaseContractError, match="EXCLUDED_SPECIAL"):
        index.phase_for_game_pk(5)


@pytest.mark.parametrize("raw_type", [None, "", "r", "X", " R"])
def test_missing_or_nonexact_unknown_type_fails_closed(raw_type: str | None) -> None:
    with pytest.raises(PhaseContractError):
        canonical_phase_record(game(6, raw_type), source_sha256="f" * 64)


def test_conflicting_schedule_and_feed_types_fail_closed() -> None:
    row = game(7, "R", gameData={"game": {"pk": 7, "season": 2026, "type": "W"}})
    with pytest.raises(PhaseContractError, match="CONFLICTING_AUTHORITATIVE_GAME_TYPES"):
        canonical_phase_record(row, source_sha256="1" * 64)


def test_duplicate_game_pk_conflict_fails_closed() -> None:
    index = CanonicalGamePhaseIndex()
    index.add(canonical_phase_record(game(8, "R"), source_sha256="2" * 64))
    with pytest.raises(PhaseContractError, match="DUPLICATE_GAME_PK_PHASE_CONFLICT"):
        index.add(canonical_phase_record(game(8, "W"), source_sha256="3" * 64))


def test_exact_game_pk_join_has_no_date_or_team_fallback() -> None:
    index = CanonicalGamePhaseIndex()
    index.add(canonical_phase_record(game(9, "R"), source_sha256="4" * 64))
    assert index.phase_for_game_pk(9).season_phase == "REGULAR_SEASON"
    with pytest.raises(PhaseContractError, match="CANONICAL_GAME_PHASE_MISSING"):
        index.phase_for_game_pk(10)


def test_cleanroom_insert_is_named_and_partial_schema_fails() -> None:
    legacy_columns = {
        "game_pk", "slate_date", "official_game_date", "home_team_mlb_id",
        "away_team_mlb_id", "scheduled_start_utc", "game_status", "source",
        "source_observed_at_utc", "ingested_at_utc", "source_payload_sha256",
    }
    sql = cleanroom_game_insert_sql(legacy_columns)
    assert "INSERT INTO mlb_cleanroom_v1.games (" in sql
    assert "INSERT INTO mlb_cleanroom_v1.games VALUES" not in sql
    with pytest.raises(RuntimeError, match="CANONICAL_PHASE_SCHEMA_PARTIAL"):
        cleanroom_game_insert_sql(legacy_columns | {"source_game_type"})


def test_no_game_producer_uses_positional_insert() -> None:
    producers = (
        ROOT / "backend/mlb/scripts/cleanroom_v1/run_cleanroom_source_cycle.py",
        ROOT / "backend/mlb/scripts/cleanroom_v1/admit_exact_roster_bridge.py",
    )
    for path in producers:
        assert "INSERT INTO mlb_cleanroom_v1.games VALUES" not in path.read_text()
    assert "INSERT INTO mlb.game_info VALUES" not in (
        ROOT / "backend/mlb/scripts/insert_mlb_stat_derived.py"
    ).read_text()


def test_zero_date_based_phase_reconstruction() -> None:
    files = (
        ROOT / "backend/mlb/season_transition/contract_v1.py",
        ROOT / "backend/mlb/season_transition/canonical_phase_v1.py",
        ROOT / "backend/mlb/scripts/build_mlb_canonical_game_phase_backfill_v1.py",
    )
    source = "\n".join(path.read_text() for path in files)
    forbidden = ("month ==", "month in", "officialDate[:4]", "gameDate[:4]", "default.*R")
    assert not any(token in source for token in forbidden)


def test_prepared_migration_persists_both_tables_and_join_is_exact() -> None:
    migration = (
        ROOT / "backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql"
    ).read_text()
    for table in ("mlb.game_info", "mlb_cleanroom_v1.games"):
        assert f"ALTER TABLE {table}" in migration
    for column in (
        "source_season", "source_game_type", "season_phase", "postseason_round",
        "season_name", "source_round", "schedule_relationships",
    ):
        assert migration.count(f"ADD COLUMN IF NOT EXISTS {column}") == 2
    view = migration.split("CREATE OR REPLACE VIEW mlb.canonical_game_phase_v1 AS", 1)[1]
    assert "game_id AS game_pk" in view
    assert "GROUP BY c.game_pk" in view
    assert "game_date" not in view
    assert "official_game_date" not in view


def test_offline_builder_is_deterministic_and_source_hashed(tmp_path: Path) -> None:
    payload = {"dates": [{"date": "2026-11-15", "games": [game(11, "R"), game(12, "W")]}]}
    source = tmp_path / "schedule.json"
    raw = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
    source.write_bytes(raw)
    paths = discover_inputs([source])
    first = build_proposal(paths, repo_root=ROOT)
    second = build_proposal(paths, repo_root=ROOT)
    assert first == second
    records, sources, summary = first
    assert len(records) == 2
    assert sources[0]["source_sha256"] == hashlib.sha256(raw).hexdigest()
    assert summary["database_writes"] == 0
    assert summary["network_requests"] == 0
    output = tmp_path / "proposal"
    write_outputs(output, records, sources, summary)
    for line in (output / "sha256_manifest.txt").read_text().splitlines():
        digest, name = line.split("  ", 1)
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest


def test_propagation_audit_distinguishes_current_and_post_activation() -> None:
    path = ROOT / "docs/contracts/mlb_2026_regular_season_close_and_postseason_data_plan_v1/end_to_end_propagation_audit.csv"
    text = path.read_text()
    assert "classification_after_canonical_activation" in text.splitlines()[0]
    hits = next(line for line in text.splitlines() if line.startswith("Full-board Hits,"))
    assert "RAW_AND_NORMALIZED_TYPE_PRESERVED" not in hits.split(",")[2]
