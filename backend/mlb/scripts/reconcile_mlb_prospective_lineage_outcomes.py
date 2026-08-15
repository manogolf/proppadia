#!/usr/bin/env python3
"""Attach canonical completed-player outcomes to immutable prospective lineage rows."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.mlb.scripts.build_mlb_reconcile_rows import _load_actual_values

ROOT = Path(__file__).resolve().parents[3]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _strict_pregame(row: dict[str, str]) -> bool:
    try:
        prediction = datetime.fromisoformat(row["prediction_timestamp"].replace("Z", "+00:00"))
        start = datetime.fromisoformat(row["scheduled_game_start"].replace("Z", "+00:00"))
        return prediction < start
    except Exception:
        return False


def freeze_predictions(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        source = list(csv.DictReader(handle))
    frozen: dict[tuple[int, int, str, float], dict[str, Any]] = {}
    for row in source:
        if row.get("lineage_status") != "LINEAGE_CERTIFIED" or not _strict_pregame(row):
            continue
        identity = json.loads(row["canonical_row_identity"])
        key = (
            int(identity["game_id"]),
            int(identity["player_id"]),
            str(identity["prop_type"]).strip().lower(),
            float(identity["line"]),
        )
        prior = frozen.get(key)
        order = (row["prediction_timestamp"], row.get("bookmaker_key") or "")
        if prior is None or order < prior["_order"]:
            frozen[key] = {"_order": order, "identity": identity, "prediction": row}
    return [frozen[key] for key in sorted(frozen)]


def reconcile_rows(
    frozen: list[dict[str, Any]],
    actual_by_key: dict[tuple[int, int, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in frozen:
        identity, prediction = item["identity"], item["prediction"]
        game_id = int(identity["game_id"])
        player_id = int(identity["player_id"])
        prop_type = str(identity["prop_type"]).strip().lower()
        line = float(identity["line"])
        actual = actual_by_key.get((game_id, player_id, prop_type), {})
        value = actual.get("actual_value")
        distinct = int(actual.get("distinct_actual_values") or 0)
        if value is None:
            status = "UNRESOLVED_NO_OFFICIAL_APPEARANCE_OR_ELIGIBLE_OUTCOME"
            side_result = ""
        elif distinct > 1:
            status = "UNRESOLVED_CANONICAL_OUTCOME_CONFLICT"
            side_result = ""
            value = None
        else:
            status = "CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT"
            if abs(float(value) - line) < 1e-12:
                side_result = "push"
            else:
                winning_side = "over" if float(value) > line else "under"
                side_result = "win" if prediction["selected_side"] == winning_side else "loss"
        rows.append({
            "canonical_identity": f"{game_id}:{player_id}:{prop_type}:{line:g}",
            "game_date": identity["game_date"],
            "game_id": game_id,
            "player_id": player_id,
            "prop_type": prop_type,
            "line": line,
            "selected_side": prediction["selected_side"],
            "prediction_timestamp": prediction["prediction_timestamp"],
            "scheduled_game_start": prediction["scheduled_game_start"],
            "model_semantic_name": prediction["model_semantic_name"],
            "model_artifact_sha256": prediction["model_artifact_sha256"],
            "model_probability_over": prediction["model_probability_over"],
            "model_selected_side_probability": prediction["model_selected_side_probability"],
            "prediction_lineage_status": prediction["lineage_status"],
            "actual_value": "" if value is None else float(value),
            "selected_side_outcome": side_result,
            "outcome_status": status,
            "actual_sample_rows": int(actual.get("sample_rows") or 0),
            "actual_distinct_values": distinct,
            "outcome_contract": "MLB_API_CANONICAL_ACTUAL_WITH_PLAYER_STATS_FALLBACK",
        })
    return rows


def require_complete(date: str, path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"COMPLETENESS_ARTIFACT_MISSING date={date} path={path}")
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or any(row.get("classification") != "COMPLETE_EXACT" for row in rows):
        failed = [f"{r.get('game_pk')}:{r.get('classification')}" for r in rows if r.get("classification") != "COMPLETE_EXACT"]
        raise RuntimeError(f"COMPLETENESS_NOT_EXACT date={date} games={','.join(failed)}")


def write_immutable_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    fields = list(rows[0]) if rows else []
    from io import StringIO
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    data = buffer.getvalue().encode()
    if path.exists() and path.read_bytes() != data:
        raise RuntimeError(f"IMMUTABLE_OUTCOME_SIDECAR_CONFLICT path={path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--out-root", default="artifacts/analysis/mlb/prospective_lineage_outcomes")
    args = parser.parse_args()
    date = str(args.date)
    ledger = ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"
    completeness = ROOT / f"artifacts/analysis/mlb/player_stats_completeness/{date}/player_stats_date_completeness_{date}.csv"
    if not ledger.exists():
        raise RuntimeError(f"PREDICTION_LEDGER_MISSING date={date}")
    require_complete(date, completeness)
    frozen = freeze_predictions(ledger)
    actual = _load_actual_values(from_date=date, to_date=date)
    rows = reconcile_rows(frozen, actual)
    if len(rows) != len({row["canonical_identity"] for row in rows}):
        raise RuntimeError("DUPLICATE_CANONICAL_OUTCOME_IDENTITIES")
    out_dir = ROOT / args.out_root / date
    out_csv = out_dir / "canonical_outcome_reconciliation.csv"
    digest = write_immutable_csv(out_csv, rows)
    by_prop: dict[str, dict[str, int]] = {}
    by_prop_line: dict[str, dict[str, int]] = {}
    for row in rows:
        bucket = by_prop.setdefault(row["prop_type"], {"predictions": 0, "resolved": 0, "unresolved": 0})
        lane = f"{row['prop_type']}:{float(row['line']):g}"
        lane_bucket = by_prop_line.setdefault(lane, {"predictions": 0, "resolved": 0, "unresolved": 0})
        resolved = row["outcome_status"].startswith("CANONICAL_RESOLVED")
        bucket["predictions"] += 1
        bucket["resolved" if resolved else "unresolved"] += 1
        lane_bucket["predictions"] += 1
        lane_bucket["resolved" if resolved else "unresolved"] += 1
    summary = {
        "date": date,
        "decision": "CANONICAL_PROSPECTIVE_OUTCOME_RECONCILIATION_COMPLETE",
        "prediction_ledger": str(ledger.relative_to(ROOT)),
        "prediction_ledger_sha256": sha256(ledger),
        "completeness_sha256": sha256(completeness),
        "frozen_identities": len(rows),
        "resolved": sum(r["outcome_status"].startswith("CANONICAL_RESOLVED") for r in rows),
        "unresolved": sum(not r["outcome_status"].startswith("CANONICAL_RESOLVED") for r in rows),
        "duplicate_identities": 0,
        "by_prop": by_prop,
        "by_prop_line": by_prop_line,
        "outcome_csv": str(out_csv.relative_to(ROOT)),
        "outcome_csv_sha256": digest,
    }
    summary_path = out_dir / "canonical_outcome_reconciliation_summary.json"
    encoded = (json.dumps(summary, indent=2, sort_keys=True) + "\n").encode()
    if summary_path.exists() and summary_path.read_bytes() != encoded:
        raise RuntimeError(f"IMMUTABLE_OUTCOME_SUMMARY_CONFLICT path={summary_path}")
    if not summary_path.exists():
        summary_path.write_bytes(encoded)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
