#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts.audit_mlb_canonical_identity import build_coverage, build_join_risk_report, write_reports


DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/identity")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _health_rows(coverage: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in coverage:
        if row.get("path") == "database table":
            continue
        rows_sampled = int(float(row.get("rows_sampled") or 0))
        player_cov = float(row.get("player_id_coverage_pct") or 0)
        game_cov = float(row.get("game_id_coverage_pct") or 0)
        event_cov = float(row.get("provider_event_id_coverage_pct") or 0)
        market_cov = float(row.get("canonical_market_key_coverage_pct") or 0)
        fallback_rows = int(float(row.get("fallback_identity_rows") or 0))
        ambiguous_rows = int(float(row.get("ambiguous_identity_rows") or 0))
        rows_using_ids = int(round(rows_sampled * min(player_cov, game_cov) / 100.0))
        unresolved_rows = ambiguous_rows
        status = "pass"
        if rows_sampled and rows_using_ids == 0 and event_cov == 0:
            status = "warn"
        if row.get("classification") == "E. unsafe/mixed identity source":
            status = "warn"
        if row.get("artifact") == "review boards":
            if player_cov < 95 or game_cov < 90 or market_cov < 90 or unresolved_rows > 0:
                status = "warn"
            if rows_sampled and unresolved_rows / rows_sampled > 0.05:
                status = "fail"
        rows.append(
            {
                "artifact": row.get("artifact", ""),
                "path": row.get("path", ""),
                "classification": row.get("classification", ""),
                "rows": rows_sampled,
                "rows_using_ids": rows_using_ids,
                "rows_using_fallback": fallback_rows,
                "rows_ambiguous": ambiguous_rows,
                "rows_unresolved": unresolved_rows,
                "player_id_coverage_pct": player_cov,
                "game_id_coverage_pct": game_cov,
                "canonical_team_coverage_pct": float(row.get("canonical_team_coverage_pct") or 0),
                "provider_event_id_coverage_pct": event_cov,
                "canonical_market_key_coverage_pct": market_cov,
                "identity_status": status,
            }
        )
    return rows


def _load_hits_environment_health(out_dir: Path) -> dict[str, Any] | None:
    path = out_dir / "hits_environment_identity_health.csv"
    if not path.exists():
        return None
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return None
    if not rows:
        return None
    row = rows[-1]

    def as_int(key: str) -> int:
        try:
            return int(float(row.get(key) or 0))
        except Exception:
            return 0

    def as_float(key: str) -> float:
        try:
            return float(row.get(key) or 0)
        except Exception:
            return 0.0

    rows_total = as_int("rows")
    unresolved = as_int("unresolved_identity_rows")
    ambiguous = as_int("ambiguous_identity_rows")
    status = str(row.get("status") or "warn")
    if rows_total and unresolved / rows_total > 0.05:
        status = "fail"
    return {
        "artifact": "hits environment",
        "path": "artifacts/analysis/mlb/identity/hits_environment_identity_health.csv",
        "classification": "B. ID-first artifact",
        "rows": rows_total,
        "rows_using_ids": min(as_int("player_id_rows"), as_int("game_id_rows")),
        "rows_using_fallback": as_int("fallback_identity_rows"),
        "rows_ambiguous": ambiguous,
        "rows_unresolved": unresolved,
        "player_id_coverage_pct": as_float("player_id_coverage_pct"),
        "game_id_coverage_pct": as_float("game_id_coverage_pct"),
        "canonical_team_coverage_pct": 100.0 if rows_total and as_int("blank_team_rows") == 0 else 0.0,
        "provider_event_id_coverage_pct": 0.0,
        "canonical_market_key_coverage_pct": as_float("market_key_coverage_pct"),
        "context_only_rows": as_int("context_only_rows"),
        "trusted_forecast_rows": as_int("trusted_forecast_rows"),
        "blank_team_rows": as_int("blank_team_rows"),
        "blank_opponent_rows": as_int("blank_opponent_rows"),
        "blank_starter_rows": as_int("blank_starter_rows"),
        "blank_game_id_rows": as_int("blank_game_id_rows"),
        "identity_status": status,
    }


def _write_health_report(out_dir: Path, health: list[dict[str, Any]], risks: list[dict[str, Any]]) -> None:
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    class_counts = Counter(str(row.get("classification", "")) for row in health)
    risk_counts = Counter(str(row.get("risk_classification", "")) for row in risks)
    total_rows = sum(int(row.get("rows") or 0) for row in health)
    rows_using_ids = sum(int(row.get("rows_using_ids") or 0) for row in health)
    rows_using_fallback = sum(int(row.get("rows_using_fallback") or 0) for row in health)
    rows_ambiguous = sum(int(row.get("rows_ambiguous") or 0) for row in health)
    rows_unresolved = sum(int(row.get("rows_unresolved") or 0) for row in health)
    warnings = [row for row in health if row.get("identity_status") != "pass"]
    payload = {
        "generated_utc": generated,
        "status": "warn" if warnings else "pass",
        "artifact_samples": len(health),
        "sampled_rows": total_rows,
        "rows_using_ids": rows_using_ids,
        "rows_using_fallback": rows_using_fallback,
        "rows_ambiguous": rows_ambiguous,
        "rows_unresolved": rows_unresolved,
        "classification_counts": dict(class_counts),
        "join_risk_counts": dict(risk_counts),
        "warning_artifacts": len(warnings),
    }
    (out_dir / "mlb_identity_health_summary.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_csv(out_dir / "mlb_identity_health_by_artifact.csv", health)

    lines = [
        "# MLB Identity Health",
        "",
        f"- Generated UTC: `{generated}`",
        f"- Status: `{payload['status']}`",
        "- Scope: diagnostic/reporting only; no production joins changed.",
        "",
        "## Summary",
        "",
        f"- Artifact samples: `{len(health)}`",
        f"- Sampled rows: `{total_rows}`",
        f"- Rows using canonical player+game IDs: `{rows_using_ids}`",
        f"- Rows using fallback identity: `{rows_using_fallback}`",
        f"- Ambiguous rows: `{rows_ambiguous}`",
        f"- Unresolved rows: `{rows_unresolved}`",
        f"- Warning artifacts: `{len(warnings)}`",
        "",
        "## Priority Caller Health",
        "",
        "| caller | rows | player ID % | game ID % | market key % | fallback | unresolved | status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in health:
        if str(row.get("artifact") or "") in {"hits environment", "review boards", "expanded universe", "reconcile", "upload prep", "prediction outputs"}:
            lines.append(
                f"| {row.get('artifact')} | `{row.get('rows')}` | `{row.get('player_id_coverage_pct')}` | `{row.get('game_id_coverage_pct')}` | `{row.get('canonical_market_key_coverage_pct')}` | `{row.get('rows_using_fallback')}` | `{row.get('rows_unresolved')}` | `{row.get('identity_status')}` |"
            )
    lines.extend(
        [
            "",
            "## Classification Counts",
            "",
            "| classification | artifacts |",
            "|---|---:|",
        ]
    )
    for key, value in sorted(class_counts.items()):
        lines.append(f"| `{key}` | `{value}` |")
    lines.extend(["", "## Join Risk Counts", "", "| risk | findings |", "|---|---:|"])
    for key, value in sorted(risk_counts.items()):
        lines.append(f"| `{key}` | `{value}` |")
    lines.extend(
        [
            "",
            "## Warning Artifacts",
            "",
        "| artifact | rows | player ID % | game ID % | event ID % | classification | path |",
            "|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in warnings[:80]:
        lines.append(
            f"| {row['artifact']} | `{row['rows']}` | `{row['player_id_coverage_pct']}` | `{row['game_id_coverage_pct']}` | `{row['provider_event_id_coverage_pct']}` | `{row['classification']}` | `{row['path']}` |"
        )
    lines.extend(
        [
            "",
            "## Resolver Layer Status",
            "",
            "- Shared resolver package exists at `backend/mlb/identity/`.",
            "- Production callers have not been behavior-refactored by this health run.",
            "- Next migrations should add resolver calls and row-level provenance to priority artifacts one at a time.",
            "",
        ]
    )
    (out_dir / "mlb_identity_health.md").write_text("\n".join(lines), encoding="utf-8")


def _write_progress_report(out_dir: Path, health: list[dict[str, Any]], risks: list[dict[str, Any]]) -> None:
    priority = [
        ("hits environment", ("hits-environment", "hits environment")),
        ("review boards", ("review boards",)),
        ("expanded universe", ("expanded_o15_universe_rows", "expanded universe")),
        ("reconcile", ("reconcile",)),
        ("upload prep", ("upload prep",)),
        ("prediction outputs", ("model prediction outputs", "prediction outputs")),
    ]
    priority_rows = []
    for label, terms in priority:
        matched = [
            row
            for row in health
            if any(term in str(row.get("artifact", "")).lower() or term in str(row.get("path", "")).lower() for term in terms)
        ]
        migrated_labels = {"review boards": "phase3a_review_boards_migrated", "hits environment": "phase3b_hits_environment_migrated"}
        priority_rows.append(
            {
                "priority": label,
                "artifact_samples": len(matched),
                "rows": sum(int(row.get("rows") or 0) for row in matched),
                "rows_using_ids": sum(int(row.get("rows_using_ids") or 0) for row in matched),
                "fallback_rows": sum(int(row.get("rows_using_fallback") or 0) for row in matched),
                "ambiguous_rows": sum(int(row.get("rows_ambiguous") or 0) for row in matched),
                "unresolved_rows": sum(int(row.get("rows_unresolved") or 0) for row in matched),
                "migration_status": migrated_labels.get(label, "baseline_only"),
                "bugs_eliminated": 0,
            }
        )
    _write_csv(out_dir / "mlb_identity_migration_progress.csv", priority_rows)
    lines = [
        "# MLB Identity Migration Progress",
        "",
        "## Phase 3 Status",
        "",
        "- Shared resolver layer added: `backend/mlb/identity/`.",
        "- Health target added: `make mlb-identity-health`.",
        "- Production behavior changed: `no`.",
        "- Caller migrations completed: `review boards`, `hits environment`.",
        "",
        "Review-board CSVs and hits-environment row outputs now carry canonical player/game/team/opponent/market identity fields. Other priority callers remain baseline-only. The next safe step is to migrate one additional caller at a time and compare before/after ID coverage, fallback usage, ambiguous rows, unresolved rows, and bugs eliminated.",
        "",
        "## Priority Baseline",
        "",
        "| priority | artifact samples | rows | rows using IDs | fallback rows | ambiguous | unresolved | status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in priority_rows:
        lines.append(
            f"| {row['priority']} | `{row['artifact_samples']}` | `{row['rows']}` | `{row['rows_using_ids']}` | `{row['fallback_rows']}` | `{row['ambiguous_rows']}` | `{row['unresolved_rows']}` | `{row['migration_status']}` |"
        )
    lines.extend(
        [
            "",
            "## Highest-Value Migration Order",
            "",
            "1. Hits environment: starter/player ambiguity and probable-starter context are high-impact and diagnostic-heavy.",
            "2. Review boards: manual decision surfaces should show canonical identity/provenance clearly.",
            "3. Expanded universe: canonical research universe should retain resolver status for every row.",
            "4. Reconcile: already strong on IDs, but fallback/sidecar semantics should call shared identity helpers.",
            "5. Upload prep: current samples lack visible canonical IDs and should regain lineage columns before deeper use.",
            "6. Prediction outputs: mostly ID-complete; migrate after higher-risk artifacts to avoid needless churn.",
            "",
            "## Join Risk Baseline",
            "",
            f"- Total findings: `{len(risks)}`",
            f"- Should be ID-first: `{sum(1 for row in risks if row.get('risk_classification') == 'should be ID-first')}`",
            f"- Safe fallback: `{sum(1 for row in risks if row.get('risk_classification') == 'safe fallback')}`",
            f"- Acceptable display-only use: `{sum(1 for row in risks if row.get('risk_classification') == 'acceptable display-only use')}`",
            "",
        ]
    )
    (out_dir / "mlb_identity_migration_progress.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run MLB canonical identity health diagnostics.")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--max-files-per-spec", type=int, default=8)
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    max_rows = args.max_rows if args.max_rows > 0 else None
    coverage = build_coverage(out_dir, args.max_files_per_spec, max_rows)
    risks = build_join_risk_report()
    write_reports(out_dir, coverage, risks)
    health = [
        row
        for row in _health_rows(coverage)
        if "mlb_hits_environment_latest.json" not in str(row.get("path") or "")
    ]
    hits_environment_health = _load_hits_environment_health(out_dir)
    if hits_environment_health is not None:
        health.append(hits_environment_health)
    _write_health_report(out_dir, health, risks)
    _write_progress_report(out_dir, health, risks)
    print(
        json.dumps(
            {
                "status": "ok",
                "out_dir": out_dir.as_posix(),
                "artifact_samples": len(health),
                "join_risk_rows": len(risks),
                "health_md": (out_dir / "mlb_identity_health.md").as_posix(),
                "progress_md": (out_dir / "mlb_identity_migration_progress.md").as_posix(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
