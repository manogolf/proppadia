#!/usr/bin/env python3
"""Produce daily integrity and predeclared progress reports for the full-board shadow."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
DEFAULT_REPORT_ROOT = ROOT / "artifacts/analysis/mlb/hits05_full_board_shadow"
MIN_CLUSTERS = 20
TARGET_RESOLVED = 5000
MIN_UPPER_TAIL = 500
EPS = 1e-12


def _metrics(rows: list[dict[str, Any]], probability_key: str = "probability_over") -> dict[str, Any]:
    if not rows:
        return {"rows": 0, "brier": None, "log_loss": None, "ece_10": None}
    p = np.clip(np.array([float(row[probability_key]) for row in rows]), EPS, 1 - EPS)
    y = np.array([float(row["target"]) for row in rows])
    brier = float(np.mean((p - y) ** 2))
    log_loss = float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))
    bins = np.minimum((p * 10).astype(int), 9)
    ece = sum(float(np.sum(bins == value)) / len(rows) * abs(float(np.mean(p[bins == value])) - float(np.mean(y[bins == value]))) for value in sorted(set(bins)))
    return {"rows": len(rows), "brier": brier, "log_loss": log_loss, "ece_10": float(ece), "observed_rate": float(np.mean(y)), "mean_probability": float(np.mean(p))}


def _ordering(rows: list[dict[str, Any]], bands: int) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (float(row["probability_over"]), row["canonical_identity"]))
    output = []
    for band in range(bands):
        members = [row for index, row in enumerate(ordered) if min(bands - 1, (index * bands) // max(1, len(ordered))) == band]
        output.append({"band": band + 1, **_metrics(members)})
    return output


def _cluster_interval(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_date[row["slate_date"]].append(row)
    dates = sorted(by_date)
    if len(dates) < 2:
        return {"clusters": len(dates), "hitter_baseline_brier_improvement_mean": None, "cluster_bootstrap_95_interval": None}
    values = np.array([
        _metrics(by_date[day], "baseline_hitter_shrunk_probability")["brier"] - _metrics(by_date[day])["brier"]
        for day in dates
    ])
    rng = np.random.default_rng(20260823)
    boot = np.array([float(np.mean(rng.choice(values, size=len(values), replace=True))) for _ in range(10000)])
    return {
        "clusters": len(dates),
        "hitter_baseline_brier_improvement_mean": float(np.mean(values)),
        "cluster_bootstrap_95_interval": [float(np.quantile(boot, 0.025)), float(np.quantile(boot, 0.975))],
        "bootstrap_seed": 20260823,
        "bootstrap_draws": 10000,
    }


def _calibration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if len(rows) < 200 or len({row["target"] for row in rows}) < 2:
        return {"supported": False, "intercept": None, "slope": None}
    p = np.clip(np.array([float(row["probability_over"]) for row in rows]), 1e-6, 1 - 1e-6)
    y = np.array([float(row["target"]) for row in rows])
    # Logistic calibration via bounded Newton iterations; evaluation only.
    x = np.column_stack([np.ones(len(p)), np.log(p / (1 - p))])
    beta = np.array([0.0, 1.0])
    for _ in range(50):
        fitted = 1 / (1 + np.exp(-np.clip(x @ beta, -30, 30)))
        weights = np.clip(fitted * (1 - fitted), 1e-8, None)
        hessian = x.T @ (weights[:, None] * x)
        step = np.linalg.pinv(hessian) @ (x.T @ (y - fitted))
        beta += step
        if float(np.max(np.abs(step))) < 1e-10:
            break
    return {"supported": True, "intercept": float(beta[0]), "slope": float(beta[1])}


def build_report(ledger_path: Path) -> dict[str, Any]:
    connection = ledger.connect_ledger(ledger_path)
    rows = [dict(row) for row in connection.execute(
        """SELECT p.canonical_identity,p.slate_date,p.game_id,p.player_id,p.probability_over,
                  p.baseline_population_probability,p.baseline_hitter_shrunk_probability,
                  o.actual_hits,o.appearance_status,o.outcome_status
           FROM hits05_full_board_predictions p
           LEFT JOIN hits05_full_board_outcomes o USING(canonical_identity)
           WHERE json_extract(p.prediction_payload_json,'$.evidence_mode')='PROSPECTIVE'
           ORDER BY p.slate_date,p.game_id,p.player_id"""
    ).fetchall()]
    market_by_identity: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in connection.execute("SELECT canonical_identity,market_payload_json FROM hits05_full_board_market_observations"):
        market_by_identity[row[0]].append(json.loads(row[1]))
    market_snapshot_summary = []
    for row in rows:
        observations = market_by_identity[row["canonical_identity"]]
        row["market_observed"] = bool(observations)
        row["betonline_le30"] = any(
            str(obs.get("bookmaker_key", "")).lower() in {"betonline", "betonlineag", "betonline.ag"}
            and 0 <= float(obs.get("minutes_before_start", math.inf)) <= 30 for obs in observations
        )
        row["target"] = 1 if row["actual_hits"] is not None and float(row["actual_hits"]) >= 1 else 0
        ordered_observations = sorted(observations, key=lambda item: (item["observation_timestamp_utc"], item["bookmaker_key"]))
        within_30 = [item for item in ordered_observations if 0 <= float(item.get("minutes_before_start", math.inf)) <= 30]
        market_snapshot_summary.append({
            "canonical_identity": row["canonical_identity"],
            "hits05_market_later_appeared": bool(ordered_observations),
            "first_proppadia_observed_pregame_line": ordered_observations[0] if ordered_observations else None,
            "nearest_le30_minute_snapshot": min(within_30, key=lambda item: float(item["minutes_before_start"])) if within_30 else None,
            "latest_proppadia_observed_prestart_snapshot": ordered_observations[-1] if ordered_observations else None,
            "true_opening_or_closing_claim": False,
        })
    resolved = [row for row in rows if row["appearance_status"] == "APPEARANCE_RESOLVED"]
    no_appearance = [row for row in rows if row["appearance_status"] == "NO_APPEARANCE_UNRESOLVED"]
    populations = {
        "entire_technically_eligible_appearance_resolved": resolved,
        "market_observed_appearance_resolved": [row for row in resolved if row["market_observed"]],
        "market_unobserved_appearance_resolved": [row for row in resolved if not row["market_observed"]],
        "betonline_le30_matched_appearance_resolved": [row for row in resolved if row["betonline_le30"]],
    }
    evaluations = {}
    for name, population in populations.items():
        evaluations[name] = {
            "model": _metrics(population),
            "frozen_population_baseline": _metrics(population, "baseline_population_probability"),
            "hitter_shrunk_baseline": _metrics(population, "baseline_hitter_shrunk_probability"),
        }
    dates = sorted({row["slate_date"] for row in resolved})
    split = len(dates) // 2
    first_dates, second_dates = set(dates[:split]), set(dates[split:])
    quintiles = _ordering(resolved, 5)
    deciles = _ordering(resolved, 10)
    upper_tail = deciles[-1] if deciles else {"rows": 0}
    top_bottom = None
    if deciles and deciles[0].get("rows") and deciles[-1].get("rows"):
        top_bottom = float(deciles[-1]["observed_rate"] - deciles[0]["observed_rate"])
    cluster = _cluster_interval(resolved)
    horizon = len(dates) >= MIN_CLUSTERS and len(resolved) >= TARGET_RESOLVED and int(upper_tail.get("rows") or 0) >= MIN_UPPER_TAIL
    full = evaluations["entire_technically_eligible_appearance_resolved"]
    if not horizon:
        decision = "FULL_BOARD_EVIDENCE_INSUFFICIENT"
    else:
        m, b1, b2 = full["model"], full["frozen_population_baseline"], full["hitter_shrunk_baseline"]
        interval = cluster.get("cluster_bootstrap_95_interval")
        incremental = (
            m["brier"] < b1["brier"] and m["brier"] < b2["brier"]
            and m["log_loss"] < b1["log_loss"] and m["log_loss"] < b2["log_loss"]
            and interval is not None and interval[0] > 0
        )
        if incremental:
            decision = "FULL_BOARD_INCREMENTAL_INFORMATION_REPRODUCED"
        elif top_bottom is not None and top_bottom > 0:
            decision = "FULL_BOARD_ORDERING_ONLY"
        else:
            decision = "FULL_BOARD_NO_INCREMENTAL_INFORMATION"
    lodo = []
    for day in dates:
        sample = [row for row in resolved if row["slate_date"] != day]
        lodo.append({"left_out_date": day, "model_brier": _metrics(sample)["brier"], "hitter_baseline_brier": _metrics(sample, "baseline_hitter_shrunk_probability")["brier"]})
    sorted_resolved = sorted(resolved, key=lambda row: (float(row["probability_over"]), row["canonical_identity"]))
    for index, row in enumerate(sorted_resolved):
        row["evaluation_decile"] = min(10, (index * 10) // max(1, len(sorted_resolved)) + 1)
    leave_band_out = []
    for band in range(1, 11):
        sample = [row for row in sorted_resolved if row["evaluation_decile"] != band]
        leave_band_out.append({"left_out_decile": band, "model_brier": _metrics(sample)["brier"], "hitter_baseline_brier": _metrics(sample, "baseline_hitter_shrunk_probability")["brier"]})
    return {
        "experiment_id": ledger.EXPERIMENT_ID,
        "model_semantic_id": ledger.MODEL_ID,
        "model_artifact_sha256": ledger.MODEL_HASH,
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "evidence_horizon": {"minimum_game_date_clusters": MIN_CLUSTERS, "target_appearance_resolved_rows": TARGET_RESOLVED, "minimum_upper_decile_rows": MIN_UPPER_TAIL, "horizon_satisfied": horizon},
        "counts": {**ledger.counts(connection), "prospective_predictions": len(rows), "appearance_resolved": len(resolved), "no_appearance_unresolved": len(no_appearance), "qualifying_game_date_clusters": len(dates)},
        "population_evaluations": evaluations,
        "no_appearance_population": {
            "rows": len(no_appearance),
            "proper_score_status": "UNRESOLVED_EXCLUDED_FROM_PROPER_SCORES",
            "mean_model_probability": float(np.mean([float(row["probability_over"]) for row in no_appearance])) if no_appearance else None,
            "market_observed_rows": sum(bool(row["market_observed"]) for row in no_appearance),
        },
        "market_snapshot_classifications": market_snapshot_summary,
        "confidence_ordering": {"quintiles_low_to_high": quintiles, "deciles_low_to_high": deciles, "top_minus_bottom_observed_rate": top_bottom},
        "clustered_uncertainty": cluster,
        "temporal_stability": {"first_half_dates": sorted(first_dates), "first_half": _metrics([r for r in resolved if r["slate_date"] in first_dates]), "second_half_dates": sorted(second_dates), "second_half": _metrics([r for r in resolved if r["slate_date"] in second_dates])},
        "lodo": lodo,
        "leave_decile_out": leave_band_out,
        "calibration": _calibration(resolved),
        "decision_category": decision,
        "certification_status": "DEFERRED_SEPARATELY_GOVERNED",
        "public_readiness": "NOT_READY",
        "preserved_formal_review_declarations": ["HITS05_20_CLUSTER_FORWARD_EVIDENCE_WEAK", "HITS05_CERTIFICATION_STILL_DEFERRED", "HITS05_PUBLIC_PREDICTION_NOT_READY", "INCREMENTAL_INFORMATION_NOT_REPRODUCED"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--out", type=Path, default=DEFAULT_REPORT_ROOT / "progress_latest.json")
    args = parser.parse_args()
    report = build_report(args.ledger)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(args.out.relative_to(ROOT)), **report}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
