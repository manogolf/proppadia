from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from backend.mlb.shared.ubo5_tb15_run_snapshot_spine import FIELDS, freeze_complete_run


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def sources(tmp_path: Path, tag: str, probability: float, hybrid: str = "CONFIRM"):
    odds = tmp_path / f"odds_{tag}.json"
    odds.write_text(json.dumps({"captured_at_utc": f"2099-07-26T1{0 if tag == 'one' else 1}:00:00Z"}))
    wide = tmp_path / f"wide_{tag}.csv"
    write_csv(wide, [{
        "game_id": 1, "player_id": 10, "team": "A", "opponent": "B",
        "game_time": "2099-07-26T20:00:00Z",
    }])
    route = tmp_path / f"route_{tag}.csv"
    write_csv(route, [{
        "game_pk": 1, "batter_mlb_id": 10, "team": "A", "opponent": "B",
        "scheduled_start_utc": "2099-07-26T20:00:00Z", "batting_order_position": 2,
        "ubo5_probability_over": probability, "model_source": "UBO5_TB15",
        "feature_vector_sha256": "feature",
    }])
    audit = tmp_path / f"audit_{tag}.csv"
    write_csv(audit, [{
        "game_pk": 1, "batter_mlb_id": 10, "player_name": "Player",
        "game": "A @ B", "lineup_status": "LINEUP_CONFIRMED",
        "BetOnline_over_price": 160, "BetOnline_under_price": -210,
        "no_vig_over_probability": .4,
        "full_1_to_9_classification": "ROBUST_CONFIRM" if hybrid == "CONFIRM" else "ORDER_SENSITIVE_WAIT",
        "hybrid_display_status": hybrid, "unscored_reason": "",
    }])
    return odds, wide, route, audit


def freeze(tmp_path: Path, tag: str, probability: float, hybrid: str = "CONFIRM"):
    odds, wide, route, audit = sources(tmp_path, tag, probability, hybrid)
    return freeze_complete_run(
        repository_root=tmp_path, output_root=tmp_path / "out",
        date="2099-07-26", run_tag=tag, market_snapshot_path=odds,
        identity_source_path=wide, route_ledger_path=route,
        prelineup_audit_path=audit,
    )


def test_every_run_is_immutable_and_populations_have_distinct_rules(tmp_path: Path) -> None:
    first = freeze(tmp_path, "one", .45)
    second = freeze(tmp_path, "two", .35, "LIKELY CONFIRM IF STARTING")
    assert first["counts"]["broad_ever_positive"] == 1
    assert second["counts"] == {
        "all_run_observations": 2,
        "all_identity_rejected_attempts": 0,
        "all_attempted_rows_including_identity_rejects": 2,
        "all_attempted_evaluated_identities": 1,
        "broad_ever_positive": 1,
        "final_pregame_positive": 0,
        "prelineup_hard_confirm": 1,
        "prelineup_likely_confirm": 1,
    }
    assert len(list((tmp_path / "out/2099-07-26/run_snapshots").glob("*.csv"))) == 2


def test_run_tag_collision_fails_instead_of_overwriting(tmp_path: Path) -> None:
    freeze(tmp_path, "one", .45)
    with pytest.raises(RuntimeError, match="IMMUTABLE_COMPLETE_RUN_SNAPSHOT_CONFLICT"):
        freeze(tmp_path, "one", .46)


def test_snapshot_has_stable_complete_schema(tmp_path: Path) -> None:
    freeze(tmp_path, "one", .45)
    path = tmp_path / "out/2099-07-26/run_snapshots/ubo5_tb15_run_snapshot_one.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == FIELDS
        assert len(list(reader)) == 1
