#!/usr/bin/env python3
"""Attach canonical completed-player outcomes to immutable prospective lineage rows."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from typing import Any, Mapping

from backend.mlb.scripts.build_mlb_reconcile_rows import _load_actual_values

ROOT = Path(__file__).resolve().parents[3]

OUTCOME_FIELDS = (
    "canonical_identity",
    "game_date",
    "game_id",
    "player_id",
    "prop_type",
    "line",
    "selected_side",
    "prediction_timestamp",
    "scheduled_game_start",
    "model_semantic_name",
    "model_artifact_sha256",
    "model_probability_over",
    "model_selected_side_probability",
    "prediction_lineage_status",
    "actual_value",
    "selected_side_outcome",
    "outcome_status",
    "actual_sample_rows",
    "actual_distinct_values",
    "outcome_contract",
)
INTEGER_OUTCOME_FIELDS = {"game_id", "player_id", "actual_sample_rows", "actual_distinct_values"}
DECIMAL_OUTCOME_FIELDS = {
    "line",
    "model_probability_over",
    "model_selected_side_probability",
    "actual_value",
}
NULLABLE_OUTCOME_FIELDS = {"actual_value", "selected_side_outcome"}
INCIDENTAL_OUTCOME_FIELDS = {
    "generated_at",
    "generated_at_utc",
    "reconciliation_timestamp",
    "reconciliation_timestamp_utc",
    "write_timestamp",
    "write_timestamp_utc",
}
SUMMARY_SEMANTIC_FIELDS = (
    "date",
    "prediction_ledger",
    "frozen_identities",
    "resolved",
    "unresolved",
    "duplicate_identities",
    "by_prop",
    "by_prop_line",
    "outcome_csv",
    "outcome_csv_sha256",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalized_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def _normalized_decimal(value: Any, *, field: str) -> str | None:
    text = _normalized_text(value)
    if text is None:
        return None
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"INVALID_CANONICAL_OUTCOME_NUMERIC field={field} value={value!r}") from exc
    if not number.is_finite():
        raise RuntimeError(f"INVALID_CANONICAL_OUTCOME_NUMERIC field={field} value={value!r}")
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def _canonical_outcome_row(row: Mapping[str, Any]) -> dict[str, str | None]:
    keys = set(row)
    missing = set(OUTCOME_FIELDS) - keys
    unexpected = keys - set(OUTCOME_FIELDS) - INCIDENTAL_OUTCOME_FIELDS
    if missing or unexpected:
        raise RuntimeError(
            "CANONICAL_OUTCOME_SCHEMA_MISMATCH "
            f"missing={','.join(sorted(missing)) or 'NONE'} "
            f"unexpected={','.join(sorted(str(key) for key in unexpected)) or 'NONE'}"
        )
    normalized: dict[str, str | None] = {}
    for field in OUTCOME_FIELDS:
        if field in INTEGER_OUTCOME_FIELDS:
            number = _normalized_decimal(row[field], field=field)
            if number is None or Decimal(number) != Decimal(number).to_integral_value():
                raise RuntimeError(f"INVALID_CANONICAL_OUTCOME_INTEGER field={field} value={row[field]!r}")
            normalized[field] = str(int(Decimal(number)))
        elif field in DECIMAL_OUTCOME_FIELDS:
            normalized[field] = _normalized_decimal(row[field], field=field)
        else:
            normalized[field] = _normalized_text(row[field])
        if normalized[field] is None and field not in NULLABLE_OUTCOME_FIELDS:
            raise RuntimeError(f"MISSING_CANONICAL_OUTCOME_VALUE field={field}")

    expected_identity = ":".join(
        (
            str(normalized["game_id"]),
            str(normalized["player_id"]),
            str(normalized["prop_type"]),
            str(normalized["line"]),
        )
    )
    if normalized["canonical_identity"] != expected_identity:
        raise RuntimeError(
            "CANONICAL_OUTCOME_IDENTITY_MISMATCH "
            f"recorded={normalized['canonical_identity']} expected={expected_identity}"
        )
    return normalized


def canonical_outcome_set(rows: list[Mapping[str, Any]]) -> list[dict[str, str | None]]:
    canonical = [_canonical_outcome_row(row) for row in rows]
    identities = [str(row["canonical_identity"]) for row in canonical]
    if len(identities) != len(set(identities)):
        raise RuntimeError("DUPLICATE_CANONICAL_OUTCOME_IDENTITIES")
    return sorted(canonical, key=lambda row: str(row["canonical_identity"]))


def canonical_outcome_set_sha256(rows: list[Mapping[str, Any]]) -> str:
    encoded = json.dumps(
        canonical_outcome_set(rows),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_outcome_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise RuntimeError(f"CANONICAL_OUTCOME_SCHEMA_MISMATCH path={path} missing=HEADER")
        rows = list(reader)
    if any(None in row for row in rows):
        raise RuntimeError(f"CANONICAL_OUTCOME_SCHEMA_MISMATCH path={path} unexpected=EXTRA_CSV_VALUES")
    return rows


def _encoded_outcome_csv(rows: list[Mapping[str, Any]]) -> bytes:
    ordered = sorted(rows, key=lambda row: str(_canonical_outcome_row(row)["canonical_identity"]))
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(OUTCOME_FIELDS), lineterminator="\n")
    writer.writeheader()
    writer.writerows({field: row.get(field) for field in OUTCOME_FIELDS} for row in ordered)
    return buffer.getvalue().encode()


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
    proposed_semantic_sha = canonical_outcome_set_sha256(rows)
    if path.exists():
        existing_semantic_sha = canonical_outcome_set_sha256(_read_outcome_csv(path))
        if existing_semantic_sha != proposed_semantic_sha:
            raise RuntimeError(
                "IMMUTABLE_OUTCOME_SIDECAR_CONFLICT "
                f"path={path} existing_semantic_sha256={existing_semantic_sha} "
                f"proposed_semantic_sha256={proposed_semantic_sha}"
            )
        return sha256(path)
    data = _encoded_outcome_csv(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def write_immutable_summary(path: Path, summary: dict[str, Any]) -> str:
    encoded = (json.dumps(summary, indent=2, sort_keys=True) + "\n").encode()
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"IMMUTABLE_OUTCOME_SUMMARY_INVALID path={path}") from exc
        if not isinstance(existing, dict):
            raise RuntimeError(f"IMMUTABLE_OUTCOME_SUMMARY_INVALID path={path}")
        missing = [field for field in SUMMARY_SEMANTIC_FIELDS if field not in existing]
        if missing:
            raise RuntimeError(
                f"IMMUTABLE_OUTCOME_SUMMARY_INVALID path={path} missing={','.join(missing)}"
            )
        changed = [field for field in SUMMARY_SEMANTIC_FIELDS if existing[field] != summary[field]]
        if changed:
            raise RuntimeError(
                "IMMUTABLE_OUTCOME_SUMMARY_CONFLICT "
                f"path={path} semantic_fields={','.join(changed)}"
            )
        return "IMMUTABLE_OUTCOME_SUMMARY_ALREADY_CURRENT"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    return "CANONICAL_PROSPECTIVE_OUTCOME_RECONCILIATION_COMPLETE"


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
    outcome_set_sha = canonical_outcome_set_sha256(rows)
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
    decision = write_immutable_summary(summary_path, summary)
    result = dict(summary)
    result["decision"] = decision
    result["canonical_outcome_set_sha256"] = outcome_set_sha
    result["write_action"] = "NO_OP" if decision == "IMMUTABLE_OUTCOME_SUMMARY_ALREADY_CURRENT" else "CREATED"
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
