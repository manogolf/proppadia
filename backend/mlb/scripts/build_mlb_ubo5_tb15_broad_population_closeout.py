#!/usr/bin/env python3
"""Revisioned closeout for frozen broad UBO-5 run-spine populations."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.scripts.build_mlb_ubo5_tb15_daily_closeout import (
    load_certified_player_stats, load_outcomes, number, read_rows, write_rows,
)

ROOT = Path(__file__).resolve().parents[3]
CONFIG = {
    "ever_positive": {
        "manifest_key": "broad_ever_positive",
        "population_name": "BROAD_EVER_POSITIVE",
        "title": "UBO-5 Broad Ever-Positive Record",
        "selection_prefix": "first",
    },
    "final_pregame": {
        "manifest_key": "final_pregame_positive",
        "population_name": "BROAD_FINAL_PREGAME_POSITIVE",
        "title": "UBO-5 Broad Final-Pregame Positive Record",
        "selection_prefix": "final",
    },
}
CLOSEOUT_FIELDS = [
    "slate_date", "population_name", "population_rule_version",
    "population_manifest_path", "game_pk", "batter_mlb_id", "player_name",
    "game", "selection_run_tag", "selection_timestamp_utc", "batting_order",
    "strict_prior_pa", "feature_state", "ubo5_probability_over",
    "betonline_over_price", "betonline_under_price", "no_vig_over_probability",
    "ubo5_over_edge_pp", "plate_appearances", "at_bats", "singles", "doubles",
    "triples", "home_runs", "total_bases", "result", "outcome_status",
    "closeout_status",
]
RECORD_FIELDS = [
    "slate_date", "population_name", "coverage_status", "selection_count",
    "wins", "losses", "voids", "no_action", "unresolved", "win_rate",
    "average_odds", "units_at_one_unit_risk", "ROI",
    "average_ubo5_probability", "expected_wins", "actual_minus_expected_wins",
    "Brier_score", "log_loss", "average_edge_pp", "source_run_tags",
    "closeout_revision", "closeout_status", "current_revision_flag",
    "source_fingerprint", "generated_at_utc",
]


def ident(row: dict, date: str) -> tuple:
    return (date, int(float(row["game_pk"])), int(float(row["batter_mlb_id"])), "total_bases", 1.5)


def avg(values: list[float]) -> str:
    return "" if not values else f"{sum(values) / len(values):.10f}"


def risk_profit(odds: float) -> float:
    return odds / 100 if odds > 0 else 100 / abs(odds)


def render_closeout(path: Path, title: str, summary: dict, rows: list[dict]) -> None:
    lines = [
        f"# {title} Closeout — {summary['slate_date']}", "",
        f"Coverage: **{summary['coverage_status']}**  ",
        f"Status: **{summary['closeout_status']}**  ",
        f"Revision: **{summary['closeout_revision']}**  ",
        f"Record: **{summary['wins']}–{summary['losses']}**  ",
        f"ROI: **{summary['ROI'] or '—'}**", "",
        "| Player | Game | Run | Probability | Edge | Odds | TB | Result |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['player_name']} | {row['game']} | {row['selection_run_tag']} | "
            f"{float(row['ubo5_probability_over'])*100:.2f}% | "
            f"{float(row['ubo5_over_edge_pp']):+.2f} pp | {row['betonline_over_price']} | "
            f"{row['total_bases'] or '—'} | {row['result']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_record(path: Path, title: str, rows: list[dict]) -> None:
    lines = [
        f"# {title}", "",
        "| Slate | Coverage | Rev | Current | N | W | L | Unresolved | Win rate | Units | ROI | Status |",
        "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['slate_date']} | {row['coverage_status']} | {row['closeout_revision']} | "
            f"{row['current_revision_flag']} | {row['selection_count']} | {row['wins']} | "
            f"{row['losses']} | {row['unresolved']} | {row['win_rate'] or '—'} | "
            f"{row['units_at_one_unit_risk'] or '—'} | {row['ROI'] or '—'} | "
            f"{row['closeout_status']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--population", choices=CONFIG, required=True)
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    ap.add_argument("--reconcile-csv", default="")
    args = ap.parse_args()
    config = CONFIG[args.population]
    root = ROOT / args.output_root
    day = root / args.date
    manifest_path = day / f"ubo5_tb15_run_population_manifest_{args.date}.json"
    if not manifest_path.is_file():
        print(f"broad_closeout_skipped=POPULATION_NOT_CERTIFIABLE date={args.date}")
        return 0
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    population = manifest.get("populations", {}).get(config["manifest_key"], [])
    if not population:
        print(f"broad_closeout_skipped=NO_SELECTIONS population={args.population} date={args.date}")
        return 0
    coverage = {
        "CERTIFIED_COMPLETE_RUN_SNAPSHOTS": "FULLY_CERTIFIED_COMPLETE_RUN_SPINE",
        "PARTIAL_CERTIFIED_RUN_SNAPSHOTS": "PARTIAL_CERTIFIED_RUN_SPINE",
    }.get(manifest.get("spine_status"), "CERTIFIED_RETAINED_SNAPSHOT_POPULATION")
    reconcile_path = Path(args.reconcile_csv) if args.reconcile_csv else (
        ROOT / f"artifacts/analysis/mlb/execution_vs_model/{args.date}/reconcile_rows.csv"
    )
    reconciled = load_outcomes(reconcile_path, args.date)
    certified, certified_status = load_certified_player_stats(args.date)
    rows = []
    prefix = config["selection_prefix"]
    for selection in population:
        identity = ident(selection, args.date)
        market, official = reconciled.get(identity), certified.get(identity)
        outcome = official or market or {"value": None, "stats": {}, "conflict": False}
        if market and official and number(market.get("value")) is not None:
            outcome["conflict"] = bool(
                outcome.get("conflict")
                or number(market["value"]) != number(official.get("value"))
            )
        stats = outcome.get("stats") or {}
        total_bases = number(outcome.get("value"))
        if outcome.get("conflict"):
            result, outcome_status = "UNRESOLVED", "CONFLICTING_AUTHORITATIVE_OUTCOMES"
        elif total_bases is None:
            result, outcome_status = "UNRESOLVED", "AUTHORITATIVE_OUTCOME_PENDING"
        else:
            result, outcome_status = ("WIN" if total_bases >= 2 else "LOSS"), "RESOLVED"
        rows.append({
            "slate_date": args.date, "population_name": config["population_name"],
            "population_rule_version": "RUN_SNAPSHOT_SPINE_V1",
            "population_manifest_path": str(manifest_path.relative_to(ROOT)),
            "game_pk": selection["game_pk"], "batter_mlb_id": selection["batter_mlb_id"],
            "player_name": selection["player_name"], "game": selection["game"],
            "selection_run_tag": selection[f"{prefix}_run_tag"],
            "selection_timestamp_utc": selection[f"{prefix}_timestamp_utc"],
            "batting_order": selection.get("batting_order", ""),
            "strict_prior_pa": selection.get("strict_prior_pa", ""),
            "feature_state": selection.get("feature_state", ""),
            "ubo5_probability_over": selection[f"{prefix}_ubo5_probability_over"],
            "betonline_over_price": selection[f"{prefix}_betonline_over_price"],
            "betonline_under_price": selection[f"{prefix}_betonline_under_price"],
            "no_vig_over_probability": selection[f"{prefix}_no_vig_over_probability"],
            "ubo5_over_edge_pp": selection[f"{prefix}_ubo5_over_edge_pp"],
            "plate_appearances": stats.get("plate_appearances", ""),
            "at_bats": stats.get("at_bats", ""), "singles": stats.get("singles", ""),
            "doubles": stats.get("doubles", ""), "triples": stats.get("triples", ""),
            "home_runs": stats.get("home_runs", ""),
            "total_bases": "" if total_bases is None else f"{total_bases:g}",
            "result": result, "outcome_status": outcome_status,
        })
    rows.sort(key=lambda row: (row["game"], row["player_name"]))
    unresolved = sum(row["result"] == "UNRESOLVED" for row in rows)
    status = "FINAL" if unresolved == 0 else "PARTIAL_PENDING_OUTCOMES"
    for row in rows:
        row["closeout_status"] = status
    wins, losses = sum(r["result"] == "WIN" for r in rows), sum(r["result"] == "LOSS" for r in rows)
    resolved = [r for r in rows if r["result"] in {"WIN", "LOSS"}]
    probs = [float(r["ubo5_probability_over"]) for r in resolved]
    odds = [float(r["betonline_over_price"]) for r in resolved]
    edges = [float(r["ubo5_over_edge_pp"]) for r in resolved]
    outcomes = [1.0 if r["result"] == "WIN" else 0.0 for r in resolved]
    units = sum(risk_profit(o) if y == 1 else -1 for o, y in zip(odds, outcomes))
    brier = [(p - y) ** 2 for p, y in zip(probs, outcomes)]
    logs = [-(y * math.log(max(p, 1e-12)) + (1-y)*math.log(max(1-p, 1e-12))) for p, y in zip(probs, outcomes)]
    fingerprint = hashlib.sha256(json.dumps({
        "population": population, "rows": rows, "certified_status": certified_status,
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    current = day / f"ubo5_tb15_{args.population}_closeout_current.json"
    prior = json.loads(current.read_text()) if current.is_file() else {}
    if prior.get("closeout_status") == "FINAL" and status != "FINAL":
        print("broad_closeout_decision=PRESERVED_EXISTING_FINAL_SOURCE_DEGRADED")
        return 0
    unchanged = prior.get("source_fingerprint") == fingerprint
    revision = int(prior.get("closeout_revision", 0)) + (0 if unchanged else 1)
    generated = prior.get("generated_at_utc") if unchanged else datetime.now(timezone.utc).isoformat()
    tags = sorted({r["selection_run_tag"] for r in rows})
    summary = {
        "slate_date": args.date, "population_name": config["population_name"],
        "coverage_status": coverage, "selection_count": len(rows), "wins": wins,
        "losses": losses, "voids": 0, "no_action": 0, "unresolved": unresolved,
        "win_rate": "" if not resolved else f"{100*wins/len(resolved):.2f}%",
        "average_odds": avg(odds), "units_at_one_unit_risk": f"{units:.6f}" if resolved else "",
        "ROI": f"{100*units/len(resolved):.2f}%" if resolved else "",
        "average_ubo5_probability": avg(probs), "expected_wins": f"{sum(probs):.6f}" if probs else "",
        "actual_minus_expected_wins": f"{wins-sum(probs):.6f}" if probs else "",
        "Brier_score": avg(brier), "log_loss": avg(logs), "average_edge_pp": avg(edges),
        "source_run_tags": "|".join(tags), "closeout_revision": revision,
        "closeout_status": status, "current_revision_flag": "true",
        "source_fingerprint": fingerprint, "generated_at_utc": generated,
    }
    csv_path = day / f"ubo5_tb15_{args.population}_closeout_{args.date}.csv"
    md_path = day / f"ubo5_tb15_{args.population}_closeout_{args.date}.md"
    if not unchanged:
        write_rows(csv_path, rows, CLOSEOUT_FIELDS)
        render_closeout(md_path, config["title"], summary, rows)
        revision_dir = day / "broad_revisions" / args.population / f"revision_{revision:03d}"
        revision_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(csv_path, revision_dir / csv_path.name)
        shutil.copy2(md_path, revision_dir / md_path.name)
        current.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    record_dir = root / "broad_daily_record"
    record_csv = record_dir / f"ubo5_tb15_{args.population}_daily_record.csv"
    record_md = record_dir / f"ubo5_tb15_{args.population}_daily_record.md"
    records = read_rows(record_csv)
    if not unchanged:
        for record in records:
            if record["slate_date"] == args.date:
                record["current_revision_flag"] = "false"
        records.append({field: summary.get(field, "") for field in RECORD_FIELDS})
        records.sort(key=lambda row: (row["slate_date"], int(row["closeout_revision"])))
        write_rows(record_csv, records, RECORD_FIELDS)
        render_record(record_md, config["title"], records)
    print(json.dumps(summary, indent=2))
    print(f"rerun_unchanged={str(unchanged).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
