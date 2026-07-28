#!/usr/bin/env python3
"""Revisioned outcome ledger for the complete frozen BetOnline TB1.5 universe."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.scripts.build_mlb_ubo5_tb15_daily_closeout import (
    load_certified_player_stats, load_official_game_statuses, load_outcomes,
    number, read_rows, write_rows,
)
from backend.mlb.shared.ubo5_tb15_outcome_resolver import (
    identity, resolution_counts, resolve_tb15_outcome,
)

ROOT = Path(__file__).resolve().parents[3]
FIELDS = [
    "slate_date", "game_pk", "batter_mlb_id", "prop_type", "line",
    "player_name", "game", "team", "opponent", "batting_order", "route_status",
    "first_run_tag", "final_run_tag", "total_bases", "result",
    "resolution_reason_code", "outcome_source", "outcome_source_path",
    "resolution_method", "source_revision", "resolved_timestamp_utc",
    "plate_appearances", "at_bats", "singles", "doubles", "triples",
    "home_runs", "closeout_status",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    ap.add_argument("--reconcile-csv", default="")
    args = ap.parse_args()
    root = ROOT / args.output_root
    day = root / args.date
    manifest_path = day / f"ubo5_tb15_run_population_manifest_{args.date}.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    population = manifest["populations"]["all_attempted_evaluated"]
    if len(population) != manifest["counts"]["all_attempted_evaluated_identities"]:
        raise RuntimeError("population member disappeared from certified manifest")
    reconcile_path = Path(args.reconcile_csv) if args.reconcile_csv else (
        ROOT / f"artifacts/analysis/mlb/execution_vs_model/{args.date}/reconcile_rows.csv"
    )
    reconciled = load_outcomes(reconcile_path, args.date)
    certified, certified_status = load_certified_player_stats(args.date)
    official, game_status_source = load_official_game_statuses(
        args.date, day / f"ubo5_tb15_official_game_status_{args.date}.json"
    )
    resolved = []
    for member in population:
        ident = identity(args.date, member)
        resolution = resolve_tb15_outcome(
            ident, reconcile_outcome=reconciled.get(ident),
            player_stats_outcome=certified.get(ident),
            official_game_status=official.get(ident[1], ""),
            market_action=True,
            final_lineup_member=number(member.get("batting_order")) is not None,
            reconcile_source_path=str(reconcile_path),
            game_status_source_path=game_status_source,
            source_revision=str(manifest.get("spine_status") or ""),
            player_stats_available=certified_status == "PASS",
        )
        stats = resolution.get("stats") or {}
        resolved.append({
            **{k: member.get(k, "") for k in [
                "slate_date", "game_pk", "batter_mlb_id", "prop_type", "line",
                "player_name", "game", "team", "opponent", "batting_order",
                "route_status", "first_run_tag", "final_run_tag",
            ]},
            "total_bases": "" if resolution["value"] is None else f"{resolution['value']:g}",
            "result": resolution["result"],
            "resolution_reason_code": resolution["resolution_reason_code"],
            "outcome_source": resolution["outcome_source"],
            "outcome_source_path": resolution["outcome_source_path"],
            "resolution_method": resolution["resolution_method"],
            "source_revision": resolution["source_revision"],
            "resolved_timestamp_utc": resolution["resolved_timestamp_utc"],
            **{k: stats.get(k, "") for k in [
                "plate_appearances", "at_bats", "singles", "doubles",
                "triples", "home_runs",
            ]},
        })
    counts = resolution_counts(resolved)
    technical = counts["technical_unresolved_rows"]
    pending = counts["pending_postponed_rows"]
    unexplained_pending = sum(
        r["result"] == "PENDING"
        and r["resolution_reason_code"] != "POSTPONED_GAME_PENDING"
        for r in resolved
    )
    if technical or unexplained_pending:
        status = "TECHNICAL_UNRESOLVED"
    elif pending:
        status = "PREPARED_PENDING_OFFICIAL_GAME_COMPLETION"
    else:
        status = "FINAL"
    for row in resolved:
        row["closeout_status"] = status
    stable_rows = [
        {k: v for k, v in row.items() if k != "resolved_timestamp_utc"}
        for row in resolved
    ]
    fingerprint = hashlib.sha256(json.dumps({
        "population_sha256": manifest.get("source_fingerprint", ""),
        "rows": stable_rows, "certified_status": certified_status,
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    current = day / f"ubo5_tb15_complete_outcome_audit_current.json"
    prior = json.loads(current.read_text()) if current.is_file() else {}
    unchanged = prior.get("source_fingerprint") == fingerprint
    revision = int(prior.get("revision", 0)) + (0 if unchanged else 1)
    generated = prior.get("generated_at_utc") if unchanged else datetime.now(timezone.utc).isoformat()
    summary = {
        "slate_date": args.date, "revision": revision,
        "generated_at_utc": generated, "closeout_status": status,
        **counts, "unexplained_pending_rows": unexplained_pending,
        "wins": sum(r["result"] == "WIN" for r in resolved),
        "losses": sum(r["result"] == "LOSS" for r in resolved),
        "source_fingerprint": fingerprint, "rerun_unchanged": unchanged,
    }
    csv_path = day / f"ubo5_tb15_complete_outcome_audit_{args.date}.csv"
    md_path = day / f"ubo5_tb15_complete_outcome_audit_{args.date}.md"
    if not unchanged:
        write_rows(csv_path, resolved, FIELDS)
        lines = [
            f"# UBO-5 TB 1.5 Complete Outcome Audit — {args.date}", "",
            f"Status: **{status}**  ", f"Revision: **{revision}**", "",
            *[f"- {k.replace('_', ' ').title()}: {v}" for k, v in summary.items()
              if k in {*counts, "wins", "losses", "unexplained_pending_rows"}],
        ]
        md_path.write_text("\n".join(lines) + "\n")
        revision_dir = day / "complete_outcome_revisions" / f"revision_{revision:03d}"
        revision_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(csv_path, revision_dir / csv_path.name)
        shutil.copy2(md_path, revision_dir / md_path.name)
        current.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 1 if technical or unexplained_pending else 0


if __name__ == "__main__":
    raise SystemExit(main())
