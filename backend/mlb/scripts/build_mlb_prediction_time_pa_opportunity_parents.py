"""Build run-bound MLB prediction-time PA opportunity parent artifacts.

Research-only. Reads an explicit run-bound population and explicit local PA
source records. Writes parent/missing ledgers only; no DB, network, model, or
production behavior changes.

Implementation marker:
PA_PARENT_GENERATOR_CONTRACT_VERSION =
    pa_opp_v1_strict_prior_player_game_rolling_avg_2026_07_16
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _date(value: str) -> datetime.date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _f(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


CONTRACT_VERSION = "pa_opp_v1_strict_prior_player_game_rolling_avg_2026_07_16"
FORMULA_VERSION = "v1_prior_player_game_avg_plus_trend_band"


def _player_game_key(date_value: str, row: dict[str, Any]) -> str:
    return "|".join([date_value, str(row.get("game_id") or ""), str(row.get("player_id") or "")])


def _source_identity(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("game_date") or row.get("date") or "")[:10],
            str(row.get("game_id") or ""),
            str(row.get("player_id") or ""),
        ]
    )


def _load_sources(source_manifest: Path) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    source_rows: list[dict[str, str]] = []
    source_inventory: list[dict[str, Any]] = []
    for row in _rows(source_manifest):
        path = ROOT / row.get("source_path", "") if not str(row.get("source_path", "")).startswith("/") else Path(row["source_path"])
        accepted = path.exists() and str(row.get("source_role") or "") in {"official_pa_records", "local_pa_history"}
        data = _rows(path) if accepted else []
        source_inventory.append(
            {
                "source_path": _rel(path),
                "source_role": row.get("source_role"),
                "exists": path.exists(),
                "accepted": accepted,
                "rows": len(data),
                "sha256": _sha256(path) if path.exists() else "",
                "notes": row.get("notes", ""),
            }
        )
        if accepted:
            for item in data:
                item["_source_path"] = _rel(path)
                item["_source_sha256"] = _sha256(path)
                source_rows.append(item)
    return source_rows, source_inventory


def _build(args: argparse.Namespace, generated_at: str) -> dict[str, Any]:
    date_value = args.date
    cutoff_date = _date(args.prediction_cutoff)
    run_population = _rows(Path(args.run_bound_population))
    source_rows, source_inventory = _load_sources(Path(args.source_manifest))
    source_identity_counts: dict[str, int] = defaultdict(int)
    for row in source_rows:
        identity = _source_identity(row)
        if identity.strip("|"):
            source_identity_counts[identity] += 1
    duplicate_source_identities = {identity for identity, count in source_identity_counts.items() if count > 1}
    history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in source_rows:
        game_date = str(row.get("game_date") or row.get("date") or "")[:10]
        player_id = str(row.get("player_id") or "")
        if not game_date or not player_id:
            continue
        if _source_identity(row) in duplicate_source_identities:
            continue
        if _date(game_date) >= cutoff_date:
            continue
        history[player_id].append(row)
    for player_rows in history.values():
        player_rows.sort(
            key=lambda r: (
                str(r.get("game_date") or r.get("date") or ""),
                str(r.get("game_id") or ""),
                str(r.get("player_id") or ""),
            )
        )

    population_by_key: dict[str, dict[str, str]] = {}
    duplicates: list[dict[str, Any]] = []
    for row in run_population:
        key = _player_game_key(date_value, row)
        if key in population_by_key:
            duplicates.append({"player_game_key": key, "reason": "duplicate_population_identity"})
            continue
        population_by_key[key] = row
    for identity in sorted(duplicate_source_identities):
        duplicates.append({"player_game_key": identity, "reason": "duplicate_source_identity_fail_closed"})

    parent_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []
    insufficient_rows: list[dict[str, Any]] = []
    direct_rows: list[dict[str, Any]] = []
    cutoff_rows: list[dict[str, Any]] = []

    for key, row in sorted(population_by_key.items()):
        player_id = str(row.get("player_id") or "")
        player_history = history.get(player_id, [])
        latest_date = str(player_history[-1].get("game_date") or player_history[-1].get("date") or "")[:10] if player_history else ""
        if not player_history:
            missing_rows.append(
                {
                    "date": date_value,
                    "run_tag": args.run_tag,
                    "player_game_key": key,
                    "game_id": row.get("game_id"),
                    "player_id": player_id,
                    "player_name": row.get("player_name"),
                    "reason": "missing_direct_pa_history",
                }
            )
            continue
        direct_rows.append(
            {
                "date": date_value,
                "run_tag": args.run_tag,
                "player_game_key": key,
                "game_id": row.get("game_id"),
                "player_id": player_id,
                "player_name": row.get("player_name"),
                "latest_included_source_date": latest_date,
                "direct_source_rows": len(player_history),
                "source_path": player_history[-1].get("_source_path", ""),
            }
        )
        sums: dict[int, float] = {}
        numerators: dict[int, float] = {}
        complete = True
        for window in [7, 15, 30]:
            selected = player_history[-window:]
            values = [_f(r.get("plate_appearances")) for r in selected]
            known = [v for v in values if v is not None]
            numerators[window] = sum(known)
            sums[window] = numerators[window] / window if len(known) == window else 0.0
            if len(selected) < window or len(known) < window:
                complete = False
        status = "PASS_PRIOR_DATE" if complete else "INSUFFICIENT_HISTORY"
        if not complete:
            insufficient_rows.append(
                {
                    "date": date_value,
                    "run_tag": args.run_tag,
                    "player_game_key": key,
                    "player_id": player_id,
                    "player_name": row.get("player_name"),
                    "history_rows_available": len(player_history),
                    "latest_included_source_date": latest_date,
                    "reason": "insufficient_prior_player_game_history_for_frozen_windows",
                }
            )
            continue
        parent_rows.append(
            {
                "slate_date": date_value,
                "game_date": row.get("game_date") or date_value,
                "game_id": row.get("game_id"),
                "player_id": player_id,
                "player_name": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "run_tag": args.run_tag,
                "prior_d7_plate_appearances": sums[7],
                "prior_d15_plate_appearances": sums[15],
                "prior_d30_plate_appearances": sums[30],
                "pa_opp_v1_d7_pa_pg": sums[7],
                "pa_opp_v1_d15_pa_pg": sums[15],
                "pa_opp_v1_d30_pa_pg": sums[30],
                "pa_opp_v1_d7_vs_d15_delta": sums[7] - sums[15],
                "pa_opp_v1_d7_vs_d30_delta": sums[7] - sums[30],
                "pa_opp_v1_d15_vs_d30_delta": sums[15] - sums[30],
                "pa_opp_v1_d7_to_d30_ratio": sums[7] / sums[30] if sums[30] else "",
                "pa_opp_v1_d15_opportunity_band": "low_lt3_8" if sums[15] < 3.8 else ("medium_3_8_to_lt4_3" if sums[15] < 4.3 else "high_ge4_3"),
                "pa_opp_v1_trend_label": "short_window_up" if sums[7] - sums[30] >= 0.35 else ("short_window_down" if sums[7] - sums[30] <= -0.35 else "stable"),
                "pa_context_latest_date": latest_date,
                "pa_opp_v1_cutoff_status": status,
                "pa_missing_flag": 0,
                "pa_source_regime": "official_pa_records_research_source",
                "pa_semantics_status": "STRICT_PRIOR_PLAYER_GAME_ROLLING_HISTORY",
                "pa_opp_v1_complete_prior_pa": True,
                "pa_opp_v1_context_age_days": (cutoff_date - _date(latest_date)).days if latest_date else "",
                "pa_opp_v1_feature_version": CONTRACT_VERSION,
                "pa_opp_v1_formula_version": FORMULA_VERSION,
                "source_manifest_path": _rel(Path(args.source_manifest)),
                "generated_at_utc": generated_at,
            }
        )

    return {
        "parent_rows": parent_rows,
        "direct_rows": direct_rows,
        "missing_rows": missing_rows,
        "insufficient_rows": insufficient_rows,
        "duplicates": duplicates,
        "cutoff_rows": cutoff_rows,
        "source_inventory": source_inventory,
        "summary": {
            "date": date_value,
            "run_tag": args.run_tag,
            "prediction_cutoff": args.prediction_cutoff,
            "run_population_rows": len(run_population),
            "unique_player_games": len(population_by_key),
            "parent_rows": len(parent_rows),
            "direct_rows": len(direct_rows),
            "missing_rows": len(missing_rows),
            "insufficient_history_rows": len(insufficient_rows),
            "duplicate_rows": len(duplicates),
            "cutoff_violations": len(cutoff_rows),
            "contract_version": CONTRACT_VERSION,
            "formula_version": FORMULA_VERSION,
            "rolling_counting_basis": "strict_prior_player_game_rows",
            "generated_at_utc": generated_at,
            "db_writes": 0,
            "network_calls": 0,
            "production_behavior_changed": False,
        },
    }


def _digest(payload: dict[str, Any]) -> str:
    stable = {k: payload[k] for k in payload if k != "summary"}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--prediction-cutoff", required=True)
    parser.add_argument("--run-bound-population", required=True)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    payload = _build(args, generated_at)
    if _digest(payload) != _digest(_build(args, generated_at)):
        raise RuntimeError("deterministic parent generation failed")
    out = Path(args.output_root)
    date_value = args.date
    run_tag = args.run_tag
    parent_path = out / f"run_bound_pa_parent_artifact_{date_value}_{run_tag}.csv"
    direct_path = out / f"direct_parent_ledger_{date_value}_{run_tag}.csv"
    missing_path = out / f"missing_parent_ledger_{date_value}_{run_tag}.csv"
    insufficient_path = out / f"insufficient_history_ledger_{date_value}_{run_tag}.csv"
    duplicate_path = out / f"duplicate_parent_ledger_{date_value}_{run_tag}.csv"
    source_path = out / f"parent_source_inventory_{date_value}_{run_tag}.csv"
    summary_path = out / f"parent_generation_summary_{date_value}_{run_tag}.json"
    parent_fields = [
        "slate_date", "game_date", "game_id", "player_id", "player_name", "team", "opponent", "run_tag",
        "prior_d7_plate_appearances", "prior_d15_plate_appearances", "prior_d30_plate_appearances",
        "pa_opp_v1_d7_pa_pg", "pa_opp_v1_d15_pa_pg", "pa_opp_v1_d30_pa_pg",
        "pa_opp_v1_d7_vs_d15_delta", "pa_opp_v1_d7_vs_d30_delta", "pa_opp_v1_d15_vs_d30_delta",
        "pa_opp_v1_d7_to_d30_ratio", "pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label",
        "pa_context_latest_date", "pa_opp_v1_cutoff_status", "pa_missing_flag", "pa_source_regime",
        "pa_semantics_status", "pa_opp_v1_complete_prior_pa", "pa_opp_v1_context_age_days",
        "pa_opp_v1_feature_version", "pa_opp_v1_formula_version", "source_manifest_path", "generated_at_utc",
    ]
    _write_csv(parent_path, payload["parent_rows"], parent_fields)
    _write_csv(direct_path, payload["direct_rows"], ["date", "run_tag", "player_game_key", "game_id", "player_id", "player_name", "latest_included_source_date", "direct_source_rows", "source_path"])
    _write_csv(missing_path, payload["missing_rows"], ["date", "run_tag", "player_game_key", "game_id", "player_id", "player_name", "reason"])
    _write_csv(insufficient_path, payload["insufficient_rows"], ["date", "run_tag", "player_game_key", "player_id", "player_name", "history_rows_available", "latest_included_source_date", "reason"])
    _write_csv(duplicate_path, payload["duplicates"], ["player_game_key", "reason"])
    _write_csv(source_path, payload["source_inventory"], ["source_path", "source_role", "exists", "accepted", "rows", "sha256", "notes"])
    payload["summary"]["payload_hash"] = _digest(payload)
    payload["summary"]["parent_artifact"] = _rel(parent_path)
    _write_json(summary_path, payload["summary"])
    print(json.dumps(payload["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
