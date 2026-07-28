from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from backend.mlb.shared.ubo5_tb15_consensus_selection import freeze


def row(run_tag: str, player_id: int = 10) -> dict:
    return {
        "slate_date": "2026-07-26", "run_tag": run_tag,
        "snapshot_timestamp_utc": "2026-07-26T18:00:00+00:00",
        "selection_timestamp_utc": "2026-07-26T18:10:00+00:00",
        "game_pk": 1, "batter_mlb_id": player_id, "player_name": f"Player {player_id}",
        "team": "A", "opponent": "B", "game": "A @ B",
        "scheduled_start_utc": "2026-07-26T19:00:00+00:00", "batting_order": 2,
        "prop_type": "total_bases", "line": 1.5, "side": "OVER",
        "ubo5_probability_over": .4, "counterfactual_incumbent_probability": .42,
        "betonline_over_price": 160, "betonline_under_price": -210,
        "no_vig_over_probability": .36, "ubo5_over_edge_pp": 4,
        "incumbent_over_edge_pp": 6, "consensus_positive_flag": True,
        "ubo5_artifact_hash": "u", "counterfactual_incumbent_artifact_hash": "i",
        "counterfactual_lineage_status": "CERTIFIED_SAME_RUN_INDEPENDENT",
        "feature_vector_sha256": "f", "market_snapshot_path": "market.json",
        "route_ledger_path": "route.csv",
    }


def test_immutable_runs_and_daily_first_appearance_dedup(tmp_path: Path) -> None:
    first = freeze(tmp_path, "2026-07-26", "run_one", [row("run_one")])
    second_row = row("run_two")
    second_row["selection_timestamp_utc"] = "2026-07-26T18:20:00+00:00"
    second = freeze(tmp_path, "2026-07-26", "run_two", [second_row])
    assert first["selection_count"] == second["selection_count"] == 1
    population = second["population"][0]
    assert population["first_consensus_run_tag"] == "run_one"
    assert population["last_consensus_run_tag"] == "run_two"
    assert population["observation_count"] == 2
    assert len(list((tmp_path / "2026-07-26/consensus_selections").glob("*.csv"))) == 2


def test_same_run_cannot_be_rewritten(tmp_path: Path) -> None:
    freeze(tmp_path, "2026-07-26", "run_one", [row("run_one")])
    changed = row("run_one")
    changed["player_name"] = "Changed"
    with pytest.raises(RuntimeError, match="immutable consensus selection conflict"):
        freeze(tmp_path, "2026-07-26", "run_one", [changed])


def test_invalid_or_post_start_row_is_not_selected(tmp_path: Path) -> None:
    invalid = row("run_one")
    invalid["selection_timestamp_utc"] = invalid["scheduled_start_utc"]
    manifest = freeze(tmp_path, "2026-07-26", "run_one", [invalid])
    assert manifest["selection_count"] == 0
    assert not (tmp_path / "2026-07-26/consensus_selections").exists()
