"""Immutable UBO-5 + incumbent consensus selection ledgers."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    "slate_date", "run_tag", "snapshot_timestamp_utc", "selection_timestamp_utc",
    "game_pk", "batter_mlb_id", "player_name", "team", "opponent", "game",
    "scheduled_start_utc", "batting_order", "prop_type", "line", "side",
    "ubo5_probability_over", "counterfactual_incumbent_probability",
    "betonline_over_price", "betonline_under_price", "no_vig_over_probability",
    "ubo5_over_edge_pp", "incumbent_over_edge_pp", "consensus_positive_flag",
    "ubo5_artifact_hash", "counterfactual_incumbent_artifact_hash",
    "counterfactual_lineage_status", "feature_vector_sha256",
    "market_snapshot_path", "route_ledger_path",
]


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truth(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _time(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _identity(row: dict) -> tuple:
    return (
        str(row["slate_date"]), int(float(row["game_pk"])),
        int(float(row["batter_mlb_id"])), "total_bases", 1.5,
    )


def certified(row: dict) -> bool:
    order = _num(row.get("batting_order"))
    ubo_edge = _num(row.get("ubo5_over_edge_pp"))
    incumbent_edge = _num(row.get("incumbent_over_edge_pp"))
    try:
        unstarted = _time(row["selection_timestamp_utc"]) < _time(row["scheduled_start_utc"])
    except (KeyError, ValueError):
        unstarted = False
    return bool(
        str(row.get("side")).upper() == "OVER"
        and str(row.get("prop_type")) == "total_bases"
        and _num(row.get("line")) == 1.5
        and _truth(row.get("consensus_positive_flag"))
        and str(row.get("counterfactual_lineage_status")) == "CERTIFIED_SAME_RUN_INDEPENDENT"
        and order is not None and 1 <= order <= 9
        and ubo_edge is not None and ubo_edge > 0
        and incumbent_edge is not None and incumbent_edge > 0
        and unstarted
    )


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def rebuild_manifest(output_root: Path, date: str, status: str = "") -> dict:
    day = output_root / date
    observations: dict[tuple, list[dict]] = {}
    for path in sorted((day / "consensus_selections").glob("ubo5_tb15_consensus_selection_*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                observations.setdefault(_identity(row), []).append(row)
    population = []
    for _, rows in sorted(observations.items()):
        rows.sort(key=lambda r: (r["selection_timestamp_utc"], r["run_tag"]))
        first, last = rows[0], rows[-1]
        population.append({
            **{key: first.get(key, "") for key in FIELDS},
            "first_consensus_run_tag": first["run_tag"],
            "first_consensus_timestamp": first["selection_timestamp_utc"],
            "last_consensus_run_tag": last["run_tag"],
            "last_pregame_consensus_status": "CONSENSUS_POSITIVE",
            "observation_count": len(rows),
        })
    payload = {
        "slate_date": date,
        "population_label": "Certified UBO-5 + Incumbent consensus-board record",
        "governing_rule": "FIRST_CERTIFIED_CONSENSUS_POSITIVE_APPEARANCE_PER_EXACT_PLAYER_GAME_LINE",
        "population_status": status or ("CERTIFIED" if population else "NO_CERTIFIED_SELECTIONS"),
        "selection_count": len(population),
        "population": population,
    }
    path = day / f"ubo5_tb15_consensus_population_manifest_{date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


def freeze(output_root: Path, date: str, run_tag: str, rows: list[dict]) -> dict:
    selected = [{field: row.get(field, "") for field in FIELDS} for row in rows if certified(row)]
    if not selected:
        return rebuild_manifest(output_root, date)
    selected.sort(key=lambda r: (r["game_pk"], r["batter_mlb_id"]))
    folder = output_root / date / "consensus_selections"
    folder.mkdir(parents=True, exist_ok=True)
    csv_path = folder / f"ubo5_tb15_consensus_selection_{run_tag}.csv"
    json_path = folder / f"ubo5_tb15_consensus_selection_{run_tag}.json"
    canonical = json.dumps(selected, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    if csv_path.exists() or json_path.exists():
        prior = json.loads(json_path.read_text(encoding="utf-8"))
        if prior.get("selection_sha256") != digest:
            raise RuntimeError(f"immutable consensus selection conflict: {run_tag}")
    else:
        _write_csv(csv_path, selected)
        json_path.write_text(json.dumps({
            "slate_date": date, "run_tag": run_tag,
            "population_label": "Certified UBO-5 + Incumbent consensus-board run observation",
            "selection_sha256": digest, "selection_count": len(selected), "rows": selected,
        }, indent=2) + "\n", encoding="utf-8")
    return rebuild_manifest(output_root, date)
