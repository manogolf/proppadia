#!/usr/bin/env python3
"""Grade the frozen certified UBO-5 + incumbent consensus-board population."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.scripts.build_mlb_ubo5_tb15_daily_closeout import (
    load_certified_player_stats, load_official_game_statuses, load_outcomes,
    number, read_rows, write_rows,
)
from backend.mlb.shared.ubo5_tb15_outcome_resolver import resolve_tb15_outcome

ROOT = Path(__file__).resolve().parents[3]
CLOSEOUT_FIELDS = [
    "slate_date", "game_pk", "batter_mlb_id", "player_name", "game",
    "selection_run_tag", "selection_timestamp_utc", "batting_order",
    "ubo5_probability_over", "counterfactual_incumbent_probability",
    "betonline_over_price", "betonline_under_price", "ubo5_over_edge_pp",
    "incumbent_over_edge_pp", "eventual_starting_status", "plate_appearances",
    "at_bats", "singles", "doubles", "triples", "home_runs", "total_bases",
    "result", "outcome_status", "closeout_status",
    "outcome_source", "outcome_source_path", "resolution_method",
    "resolution_reason_code",
]
RECORD_FIELDS = [
    "slate_date", "closeout_revision", "generated_at_utc", "selection_count",
    "wins", "losses", "voids", "no_action", "unresolved", "win_rate",
    "average_ubo5_probability", "average_incumbent_probability",
    "average_ubo5_edge_pp", "average_incumbent_edge_pp",
    "first_selection_run_tag", "source_run_tags", "closeout_status",
    "current_revision_flag", "source_fingerprint",
]


def identity(row: dict, date: str) -> tuple:
    return (date, int(float(row["game_pk"])), int(float(row["batter_mlb_id"])), "total_bases", 1.5)


def average(rows: list[dict], field: str) -> str:
    values = [number(row.get(field)) for row in rows]
    values = [value for value in values if value is not None]
    return "" if not values else f"{sum(values) / len(values):.10f}"


def render_closeout(path: Path, summary: dict, rows: list[dict]) -> None:
    lines = [
        f"# Certified UBO-5 + Incumbent Consensus-Board Closeout — {summary['slate_date']}",
        "", f"Status: **{summary['closeout_status']}**  ",
        f"Revision: **{summary['closeout_revision']}**  ",
        "Population: first certified consensus-positive appearance per exact player/game/line.",
        "", f"Record: **{summary['wins']}–{summary['losses']}**  ",
        f"Win rate: **{summary['win_rate'] or '—'}**", "",
        "| Player | Game | Selection run | Batting | Total bases | Result |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['player_name']} | {row['game']} | {row['selection_run_tag']} | "
            f"{row['batting_order']} | {row['total_bases'] or '—'} | {row['result']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_record(path: Path, rows: list[dict]) -> None:
    lines = [
        "# UBO-5 + Incumbent Consensus Record", "",
        "Certified consensus-board population only; separate from the broad intraday ever-positive record.",
        "", "| Slate | Rev | Current | Selections | W | L | Void | No action | Unresolved | Win rate | Status |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['slate_date']} | {row['closeout_revision']} | {row['current_revision_flag']} | "
            f"{row['selection_count']} | {row['wins']} | {row['losses']} | {row['voids']} | "
            f"{row['no_action']} | {row['unresolved']} | {row['win_rate'] or '—'} | {row['closeout_status']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    ap.add_argument("--reconcile-csv", default="")
    args = ap.parse_args()
    date, root = args.date, ROOT / args.output_root
    day = root / date
    manifest_path = day / f"ubo5_tb15_consensus_population_manifest_{date}.json"
    if not manifest_path.is_file():
        print(f"consensus_closeout_skipped=NO_CERTIFIED_POPULATION_MANIFEST date={date}")
        return 0
    population_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    population = population_manifest.get("population") or []
    if population_manifest.get("population_status") == "CONSENSUS_POPULATION_NOT_CERTIFIABLE":
        print(f"consensus_closeout_skipped=CONSENSUS_POPULATION_NOT_CERTIFIABLE date={date}")
        return 0
    if not population:
        print(f"consensus_closeout_skipped=NO_CERTIFIED_SELECTIONS date={date}")
        return 0
    reconcile_path = Path(args.reconcile_csv) if args.reconcile_csv else (
        ROOT / f"artifacts/analysis/mlb/execution_vs_model/{date}/reconcile_rows.csv"
    )
    reconciled = load_outcomes(reconcile_path, date)
    certified, certified_status = load_certified_player_stats(date)
    official_statuses, game_status_source = load_official_game_statuses(
        date, day / f"ubo5_tb15_official_game_status_{date}.json"
    )
    rows = []
    for selection in population:
        ident = identity(selection, date)
        market = reconciled.get(ident)
        stats_outcome = certified.get(ident)
        outcome = resolve_tb15_outcome(
            ident, reconcile_outcome=market, player_stats_outcome=stats_outcome,
            official_game_status=official_statuses.get(ident[1], ""),
            market_action=True,
            final_lineup_member=number(selection.get("batting_order")) is not None,
            reconcile_source_path=str(reconcile_path),
            game_status_source_path=game_status_source,
            source_revision="CONSENSUS_POPULATION_V1",
            player_stats_available=certified_status == "PASS",
        )
        stats = outcome.get("stats") or {}
        value = number(outcome.get("value"))
        pa = number(stats.get("plate_appearances"))
        appeared = pa is not None and pa > 0
        result = outcome["result"]
        status = "RESOLVED" if result in {"WIN", "LOSS"} else outcome["resolution_reason_code"]
        rows.append({
            "slate_date": date, "game_pk": selection["game_pk"],
            "batter_mlb_id": selection["batter_mlb_id"], "player_name": selection["player_name"],
            "game": selection["game"], "selection_run_tag": selection["first_consensus_run_tag"],
            "selection_timestamp_utc": selection["first_consensus_timestamp"],
            "batting_order": selection["batting_order"],
            "ubo5_probability_over": selection["ubo5_probability_over"],
            "counterfactual_incumbent_probability": selection["counterfactual_incumbent_probability"],
            "betonline_over_price": selection["betonline_over_price"],
            "betonline_under_price": selection["betonline_under_price"],
            "ubo5_over_edge_pp": selection["ubo5_over_edge_pp"],
            "incumbent_over_edge_pp": selection["incumbent_over_edge_pp"],
            "eventual_starting_status": "STARTED" if appeared or value is not None else "UNRESOLVED",
            "plate_appearances": stats.get("plate_appearances", ""),
            "at_bats": stats.get("at_bats", ""), "singles": stats.get("singles", ""),
            "doubles": stats.get("doubles", ""), "triples": stats.get("triples", ""),
            "home_runs": stats.get("home_runs", ""),
            "total_bases": "" if value is None else f"{value:g}",
            "result": result, "outcome_status": status,
            "outcome_source": outcome["outcome_source"],
            "outcome_source_path": outcome["outcome_source_path"],
            "resolution_method": outcome["resolution_method"],
            "resolution_reason_code": outcome["resolution_reason_code"],
        })
    rows.sort(key=lambda row: (row["game"], row["player_name"]))
    wins = sum(row["result"] == "WIN" for row in rows)
    losses = sum(row["result"] == "LOSS" for row in rows)
    voids = sum(row["result"] == "VOID" for row in rows)
    no_action = sum(row["result"] == "NO_ACTION" for row in rows)
    unresolved = sum(row["result"] in {"PENDING", "TECHNICAL_UNRESOLVED"} for row in rows)
    technical = sum(row["result"] == "TECHNICAL_UNRESOLVED" for row in rows)
    closeout_status = (
        "FINAL" if unresolved == 0
        else "TECHNICAL_UNRESOLVED" if technical
        else "PREPARED_PENDING_OFFICIAL_GAME_COMPLETION"
    )
    for row in rows:
        row["closeout_status"] = closeout_status
    source_tags = sorted({row["selection_run_tag"] for row in rows})
    fingerprint = hashlib.sha256(json.dumps({
        "population": population, "rows": rows, "reconcile_path": str(reconcile_path),
        "certified_status": certified_status,
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    current_path = day / "ubo5_tb15_consensus_closeout_current.json"
    prior = json.loads(current_path.read_text()) if current_path.is_file() else {}
    if prior.get("closeout_status") == "FINAL" and closeout_status != "FINAL":
        print("consensus_closeout_decision=PRESERVED_EXISTING_FINAL_SOURCE_DEGRADED")
        return 0
    unchanged = prior.get("source_fingerprint") == fingerprint
    revision = int(prior.get("closeout_revision", 0)) + (0 if unchanged else 1)
    generated = prior.get("generated_at_utc") if unchanged else datetime.now(timezone.utc).isoformat()
    summary = {
        "slate_date": date, "closeout_revision": revision, "generated_at_utc": generated,
        "selection_count": len(rows), "wins": wins, "losses": losses, "voids": voids,
        "no_action": no_action, "unresolved": unresolved,
        "win_rate": "" if wins + losses == 0 else f"{100 * wins / (wins + losses):.2f}%",
        "average_ubo5_probability": average(rows, "ubo5_probability_over"),
        "average_incumbent_probability": average(rows, "counterfactual_incumbent_probability"),
        "average_ubo5_edge_pp": average(rows, "ubo5_over_edge_pp"),
        "average_incumbent_edge_pp": average(rows, "incumbent_over_edge_pp"),
        "first_selection_run_tag": min(source_tags), "source_run_tags": "|".join(source_tags),
        "closeout_status": closeout_status, "current_revision_flag": "true",
        "source_fingerprint": fingerprint,
    }
    csv_path = day / f"ubo5_tb15_consensus_closeout_{date}.csv"
    md_path = day / f"ubo5_tb15_consensus_closeout_{date}.md"
    if not unchanged:
        write_rows(csv_path, rows, CLOSEOUT_FIELDS)
        render_closeout(md_path, summary, rows)
        revision_dir = day / "consensus_revisions" / f"revision_{revision:03d}"
        revision_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(csv_path, revision_dir / csv_path.name)
        shutil.copy2(md_path, revision_dir / md_path.name)
        current_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    record_dir = root / "consensus_daily_record"
    record_csv = record_dir / "ubo5_tb15_consensus_daily_record.csv"
    record_md = record_dir / "ubo5_tb15_consensus_daily_record.md"
    records = read_rows(record_csv)
    if not unchanged:
        for record in records:
            if record["slate_date"] == date:
                record["current_revision_flag"] = "false"
        records.append({field: summary.get(field, "") for field in RECORD_FIELDS})
        records.sort(key=lambda row: (row["slate_date"], int(row["closeout_revision"])))
        write_rows(record_csv, records, RECORD_FIELDS)
        render_record(record_md, records)
    latest = root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    for source, name in (
        (md_path, "ubo5_tb15_latest_consensus_closeout.md"),
        (csv_path, "ubo5_tb15_latest_consensus_closeout.csv"),
        (record_md, "ubo5_tb15_consensus_daily_record.md"),
        (record_csv, "ubo5_tb15_consensus_daily_record.csv"),
    ):
        shutil.copy2(source, latest / name)
    print(json.dumps(summary, indent=2))
    print(f"rerun_unchanged={str(unchanged).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
