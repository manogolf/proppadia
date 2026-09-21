#!/usr/bin/env python3
"""Deterministic, offline validation for canonical MLB phase activation V1."""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

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


CONTRACT_NAME = "MLB_2026_CANONICAL_GAME_PHASE_ACTIVATION_V1"
ROOT = Path(__file__).resolve().parents[3]


def _game(game_pk: int, game_type: str | None, **extra: object) -> dict:
    game = {
        "gamePk": game_pk,
        "season": "2026",
        "officialDate": "2026-11-15",
        "gameDate": "2026-11-15T01:00:00Z",
        "seriesDescription": "raw round description",
    }
    if game_type is not None:
        game["gameType"] = game_type
    game.update(extra)
    return game


def _must_reject(game: dict, expected: str) -> None:
    try:
        canonical_phase_record(game, source_sha256="f" * 64)
    except PhaseContractError as exc:
        if expected not in str(exc):
            raise AssertionError(f"expected {expected}, received {exc}") from exc
    else:
        raise AssertionError(f"expected rejection: {expected}")


def validate() -> dict:
    checks: list[str] = []
    expected_rounds = {
        "F": "WILD_CARD",
        "D": "DIVISION_SERIES",
        "L": "LEAGUE_CHAMPIONSHIP_SERIES",
        "W": "WORLD_SERIES",
        "P": "PLAYOFFS_UNSPECIFIED",
        "C": "CHAMPIONSHIP_UNSPECIFIED",
    }
    for offset, (raw_type, round_name) in enumerate(expected_rounds.items(), 1):
        row = canonical_phase_record(_game(offset, raw_type), source_sha256="a" * 64)
        assert row["source_game_type"] == raw_type
        assert row["season_phase"] == "POSTSEASON"
        assert row["postseason_round"] == round_name
    checks.append("all_postseason_rounds")

    late_regular = canonical_phase_record(_game(20, "R"), source_sha256="b" * 64)
    assert late_regular["season_phase"] == "REGULAR_SEASON"
    checks.append("regular_after_nominal_close")

    relationships = {
        "rescheduledFrom": "2026-09-20T20:10:00Z",
        "rescheduledFromDate": "2026-09-20",
        "rescheduleDate": "2026-10-03T20:10:00Z",
        "rescheduleGameDate": "2026-10-03",
        "resumedFrom": 19,
        "resumedFromDate": "2026-09-21",
        "resumeDate": "2026-10-04T20:10:00Z",
        "resumeGameDate": "2026-10-04",
    }
    relationship_row = canonical_phase_record(
        _game(21, "R", **relationships), source_sha256="c" * 64
    )
    assert relationship_row["schedule_relationships"] == relationships
    checks.extend(("postponed_rescheduled", "suspended_resumed"))

    for raw_type in ("A", "N"):
        special = canonical_phase_record(_game(22, raw_type), source_sha256="d" * 64)
        assert special["season_phase"] is None
    checks.append("all_star_and_special_exclusion")

    _must_reject(_game(30, None), "AUTHORITATIVE_GAME_TYPE_MISSING")
    _must_reject(_game(31, "X"), "UNKNOWN_STATSAPI_GAME_TYPE")
    _must_reject(_game(32, "r"), "UNKNOWN_STATSAPI_GAME_TYPE")
    _must_reject(
        _game(33, "R", gameData={"game": {"pk": 33, "season": 2026, "type": "W"}}),
        "CONFLICTING_AUTHORITATIVE_GAME_TYPES",
    )
    checks.append("missing_unknown_conflicting_types")

    index = CanonicalGamePhaseIndex()
    index.add(canonical_phase_record(_game(40, "R"), source_sha256="1" * 64))
    try:
        index.add(canonical_phase_record(_game(40, "W"), source_sha256="2" * 64))
    except PhaseContractError as exc:
        assert "DUPLICATE_GAME_PK_PHASE_CONFLICT" in str(exc)
    else:
        raise AssertionError("duplicate gamePk conflict was accepted")
    assert index.phase_for_game_pk(40).season_phase == "REGULAR_SEASON"
    try:
        index.phase_for_game_pk(41)
    except PhaseContractError as exc:
        assert "CANONICAL_GAME_PHASE_MISSING" in str(exc)
    else:
        raise AssertionError("non-exact gamePk lookup was accepted")
    checks.extend(("duplicate_game_pk_conflict", "exact_game_pk_join"))

    producer_paths = (
        ROOT / "backend/mlb/scripts/cleanroom_v1/run_cleanroom_source_cycle.py",
        ROOT / "backend/mlb/scripts/cleanroom_v1/admit_exact_roster_bridge.py",
    )
    assert all(
        "INSERT INTO mlb_cleanroom_v1.games VALUES" not in path.read_text()
        for path in producer_paths
    )
    assert "INSERT INTO mlb.game_info VALUES" not in (
        ROOT / "backend/mlb/scripts/insert_mlb_stat_derived.py"
    ).read_text()
    base_columns = {
        "game_pk", "slate_date", "official_game_date", "home_team_mlb_id",
        "away_team_mlb_id", "scheduled_start_utc", "game_status", "source",
        "source_observed_at_utc", "ingested_at_utc", "source_payload_sha256",
    }
    assert "INSERT INTO mlb_cleanroom_v1.games (" in cleanroom_game_insert_sql(base_columns)
    checks.append("positional_insert_regression")

    phase_sources = (
        ROOT / "backend/mlb/season_transition/contract_v1.py",
        ROOT / "backend/mlb/season_transition/canonical_phase_v1.py",
        ROOT / "backend/mlb/scripts/build_mlb_canonical_game_phase_backfill_v1.py",
    )
    text = "\n".join(path.read_text() for path in phase_sources)
    assert not any(token in text for token in ("month ==", "month in", "officialDate[:4]", "gameDate[:4]"))
    checks.append("zero_date_based_reconstruction")

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
    assert "game_id AS game_pk" in view and "GROUP BY c.game_pk" in view
    assert "game_date" not in view and "official_game_date" not in view
    checks.append("prepared_schema_and_exact_join_view")

    with tempfile.TemporaryDirectory(prefix="mlb_phase_v1_") as temp_name:
        temp = Path(temp_name)
        raw = (json.dumps({"dates": [{"games": [_game(50, "R"), _game(51, "W")]}]}, separators=(",", ":")) + "\n").encode()
        source_path = temp / "schedule.json"
        source_path.write_bytes(raw)
        first = build_proposal(discover_inputs([source_path]), repo_root=ROOT)
        second = build_proposal(discover_inputs([source_path]), repo_root=ROOT)
        assert first == second
        records, sources, summary = first
        assert sources[0]["source_sha256"] == hashlib.sha256(raw).hexdigest()
        assert summary["database_writes"] == 0 and summary["network_requests"] == 0
        output = temp / "proposal"
        write_outputs(output, records, sources, summary)
        for line in (output / "sha256_manifest.txt").read_text().splitlines():
            digest, name = line.split("  ", 1)
            assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest
    checks.append("offline_source_hashed_proposal")

    audit = (
        ROOT
        / "docs/contracts/mlb_2026_regular_season_close_and_postseason_data_plan_v1"
        / "end_to_end_propagation_audit.csv"
    ).read_text()
    assert "classification_after_canonical_activation" in audit.splitlines()[0]
    hits = next(line for line in audit.splitlines() if line.startswith("Full-board Hits,"))
    assert hits.split(",")[2] != "RAW_AND_NORMALIZED_TYPE_PRESERVED"
    checks.append("two_state_propagation_audit")

    return {
        "contract_name": CONTRACT_NAME,
        "status": "PASS",
        "network_requests": 0,
        "database_writes": 0,
        "checks": checks,
        "check_count": len(checks),
    }


def main() -> int:
    print(json.dumps(validate(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
