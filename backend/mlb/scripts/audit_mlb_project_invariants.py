#!/usr/bin/env python3
"""Audit machine-checkable MLB project invariants.

This is a doctrine/visibility check only. It does not mutate model,
selector, upload, threshold, or grading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REVIEW_BOARD_PATTERNS = [
    "artifacts/analysis/mlb/review_aids/hits_o15_simple_filter_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_{date}.csv",
    "artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_{date}.csv",
]


@dataclass
class InvariantResult:
    category: str
    invariant: str
    status: str
    rows_checked: int
    rows_failed: int
    detail: str
    artifact: str


def _truthy(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _coverage(rows: list[dict[str, str]], field: str) -> float:
    if not rows:
        return 0.0
    filled = sum(1 for row in rows if str(row.get(field) or "").strip())
    return filled / len(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _norm_player_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return " ".join(text.split())


def _name_alias_compatible(left: Any, right: Any) -> bool:
    left_tokens = _norm_player_name(left).split()
    right_tokens = _norm_player_name(right).split()
    if not left_tokens or not right_tokens:
        return False
    left_first, left_last = left_tokens[0], left_tokens[-1]
    right_first, right_last = right_tokens[0], right_tokens[-1]
    if not left_first or not right_first or left_last != right_last:
        return False
    return left_first == right_first or left_first.startswith(right_first) or right_first.startswith(left_first)


def _likely_duplicate_identity_alias_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    resolved = [
        row
        for row in rows
        if str(row.get("canonical_player_id") or row.get("player_id") or "").strip()
        and str(row.get("player_name") or "").strip()
    ]
    unresolved = [
        row
        for row in rows
        if not str(row.get("canonical_player_id") or row.get("player_id") or "").strip()
        and str(row.get("player_name") or "").strip()
        and (
            "unresolved" in str(row.get("identity_status") or row.get("forecast_diagnostic") or "").lower()
            or "ambiguous" in str(row.get("identity_status") or row.get("forecast_diagnostic") or "").lower()
        )
    ]
    out: list[dict[str, str]] = []
    for bad in unresolved:
        bad_date = str(bad.get("slate_date") or bad.get("game_date") or "").strip()
        for good in resolved:
            good_date = str(good.get("slate_date") or good.get("game_date") or "").strip()
            if bad_date and good_date and bad_date != good_date:
                continue
            if not _name_alias_compatible(bad.get("player_name"), good.get("player_name")):
                continue
            out.append(bad)
            break
    return out


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _add(
    results: list[InvariantResult],
    *,
    category: str,
    invariant: str,
    status: str,
    rows_checked: int = 0,
    rows_failed: int = 0,
    detail: str = "",
    artifact: str = "",
) -> None:
    results.append(
        InvariantResult(
            category=category,
            invariant=invariant,
            status=status,
            rows_checked=rows_checked,
            rows_failed=rows_failed,
            detail=detail,
            artifact=artifact,
        )
    )


def _review_board_invariants(results: list[InvariantResult], audit_date: str) -> None:
    for pattern in REVIEW_BOARD_PATTERNS:
        path = Path(pattern.format(date=audit_date))
        board_name = path.name
        rows = _read_csv(path)
        if not path.exists():
            required = "alternate_discovery" not in path.name
            _add(
                results,
                category="identity",
                invariant=f"{board_name}: board artifact exists",
                status="FAIL" if required else "WARN",
                detail="missing routine board artifact" if required else "optional/research board missing",
                artifact=str(path),
            )
            continue

        rows_with_player = [row for row in rows if str(row.get("player_id") or row.get("canonical_player_id") or "").strip()]
        missing_game = [
            row
            for row in rows_with_player
            if not str(row.get("canonical_game_id") or "").strip()
        ]
        _add(
            results,
            category="identity",
            invariant=f"{board_name}: rows with player_id have canonical_game_id",
            status="PASS" if not missing_game else "FAIL",
            rows_checked=len(rows_with_player),
            rows_failed=len(missing_game),
            detail="ok" if not missing_game else "player identity present without canonical game identity",
            artifact=str(path),
        )

        missing_market = []
        for row in rows:
            canonical_key = str(row.get("canonical_market_key") or "").strip()
            fallback_key = str(row.get("fallback_market_key") or "").strip()
            fallback_used = _truthy(row.get("fallback_used"))
            if not canonical_key and not (fallback_key and fallback_used):
                missing_market.append(row)
        _add(
            results,
            category="identity",
            invariant=f"{board_name}: canonical_market_key or explicit fallback market key",
            status="PASS" if not missing_market else "FAIL",
            rows_checked=len(rows),
            rows_failed=len(missing_market),
            detail="ok" if not missing_market else "market identity missing without explicit fallback",
            artifact=str(path),
        )

        canonical_market_coverage = _coverage(rows, "canonical_market_key")
        _add(
            results,
            category="identity",
            invariant=f"{board_name}: canonical_market_key coverage >= 95%",
            status="PASS" if canonical_market_coverage >= 0.95 else "FAIL",
            rows_checked=len(rows),
            rows_failed=len(rows) - sum(1 for row in rows if str(row.get("canonical_market_key") or "").strip()),
            detail=f"canonical_market_key_coverage={canonical_market_coverage * 100:.2f}%",
            artifact=str(path),
        )

        ambiguous_without_warning = [
            row
            for row in rows
            if (
                "ambiguous" in str(row.get("identity_status") or "").lower()
                or "unresolved" in str(row.get("identity_status") or "").lower()
            )
            and not str(row.get("identity_warning") or "").strip()
        ]
        _add(
            results,
            category="identity",
            invariant=f"{board_name}: ambiguous/unresolved identity rows carry identity_warning",
            status="PASS" if not ambiguous_without_warning else "FAIL",
            rows_checked=len(rows),
            rows_failed=len(ambiguous_without_warning),
            detail="ok" if not ambiguous_without_warning else "ambiguous/unresolved identity silently passed without warning",
            artifact=str(path),
        )


def _hits_environment_invariants(results: list[InvariantResult], hits_env_csv: Path, hits_env_json: Path) -> None:
    rows = _read_csv(hits_env_csv)
    if not rows:
        _add(
            results,
            category="identity",
            invariant="hits-environment rows are readable",
            status="FAIL",
            detail="missing or empty hits-environment CSV",
            artifact=str(hits_env_csv),
        )
        return

    rows_with_game = [row for row in rows if str(row.get("canonical_game_id") or row.get("game_id") or "").strip()]
    blank_context = [
        row
        for row in rows_with_game
        if not str(row.get("pitcher_team") or row.get("canonical_team") or "").strip()
        or not str(row.get("offense_team") or row.get("canonical_opponent") or "").strip()
        or not str(row.get("player_name") or "").strip()
    ]
    _add(
        results,
        category="identity",
        invariant="hits-environment rows with canonical_game_id have team/opponent/starter labels",
        status="PASS" if not blank_context else "FAIL",
        rows_checked=len(rows_with_game),
        rows_failed=len(blank_context),
        detail="ok" if not blank_context else "blank team/opponent/starter label with canonical game identity",
        artifact=str(hits_env_csv),
    )

    player_coverage = _coverage(rows, "canonical_player_id")
    game_coverage = _coverage(rows, "canonical_game_id")
    _add(
        results,
        category="identity",
        invariant="hits-environment canonical_player_id coverage = 100%",
        status="PASS" if player_coverage >= 1.0 else "FAIL",
        rows_checked=len(rows),
        rows_failed=len(rows) - sum(1 for row in rows if str(row.get("canonical_player_id") or "").strip()),
        detail=f"canonical_player_id_coverage={player_coverage * 100:.2f}%",
        artifact=str(hits_env_csv),
    )
    _add(
        results,
        category="identity",
        invariant="hits-environment canonical_game_id coverage = 100%",
        status="PASS" if game_coverage >= 1.0 else "FAIL",
        rows_checked=len(rows),
        rows_failed=len(rows) - sum(1 for row in rows if str(row.get("canonical_game_id") or "").strip()),
        detail=f"canonical_game_id_coverage={game_coverage * 100:.2f}%",
        artifact=str(hits_env_csv),
    )

    ambiguous_without_warning = [
        row
        for row in rows
        if (
            "ambiguous" in str(row.get("identity_status") or "").lower()
            or "unresolved" in str(row.get("identity_status") or "").lower()
        )
        and not str(row.get("identity_warning") or "").strip()
    ]
    _add(
        results,
        category="identity",
        invariant="hits-environment ambiguous/unresolved identity rows carry identity_warning",
        status="PASS" if not ambiguous_without_warning else "FAIL",
        rows_checked=len(rows),
        rows_failed=len(ambiguous_without_warning),
        detail="ok" if not ambiguous_without_warning else "ambiguous/unresolved identity silently passed without warning",
        artifact=str(hits_env_csv),
    )

    duplicate_alias_rows = _likely_duplicate_identity_alias_rows(rows)
    _add(
        results,
        category="identity",
        invariant="hits-environment has no unresolved provider-name row beside compatible resolved canonical row",
        status="PASS" if not duplicate_alias_rows else "FAIL",
        rows_checked=len(rows),
        rows_failed=len(duplicate_alias_rows),
        detail="ok" if not duplicate_alias_rows else "likely provider/canonical duplicate identity alias split",
        artifact=str(hits_env_csv),
    )

    identity_equals_forecast = [
        row
        for row in rows
        if str(row.get("identity_status") or "").strip()
        and str(row.get("identity_status") or "").strip() == str(row.get("forecast_status") or "").strip()
    ]
    _add(
        results,
        category="lifecycle",
        invariant="hits-environment identity_status is separate from forecast_status",
        status="PASS" if not identity_equals_forecast else "FAIL",
        rows_checked=len(rows),
        rows_failed=len(identity_equals_forecast),
        detail="ok" if not identity_equals_forecast else "identity_status reused as forecast_status",
        artifact=str(hits_env_csv),
    )

    data = _read_json(hits_env_json)
    lifecycle = data.get("starter_market_lifecycle") or {}
    warnings = lifecycle.get("warnings") or []
    required_lifecycle_fields = [
        "identity_status",
        "role_status",
        "starter_status",
        "market_status",
        "forecast_status",
        "forecast_diagnostic",
        "actual_usage_status",
        "game_status",
        "lifecycle_warning",
    ]
    missing_lifecycle = [
        row
        for row in warnings
        if any(field not in row for field in required_lifecycle_fields)
    ]
    _add(
        results,
        category="lifecycle",
        invariant="starter lifecycle diagnostics expose identity/role/market/forecast/outcome fields separately",
        status="PASS" if not missing_lifecycle else "FAIL",
        rows_checked=len(warnings),
        rows_failed=len(missing_lifecycle),
        detail="ok" if not missing_lifecycle else "lifecycle warning row missing required separated fields",
        artifact=str(hits_env_json),
    )


def _upload_diagnostics_invariants(results: list[InvariantResult], audit_date: str) -> None:
    path = Path("backend/mlb/data/processed/mlb_uploads") / audit_date / f"upload_identity_diagnostics_{audit_date}.csv"
    rows = _read_csv(path)
    if not path.exists() or not rows:
        _add(
            results,
            category="identity",
            invariant="upload identity diagnostics artifact exists",
            status="FAIL",
            detail="missing or empty upload identity diagnostics artifact",
            artifact=str(path),
        )
        return

    for field, label in [
        ("canonical_player_id", "player ID"),
        ("canonical_game_id", "game ID"),
        ("canonical_market_key", "market key"),
    ]:
        pct = _coverage(rows, field)
        _add(
            results,
            category="identity",
            invariant=f"upload diagnostics {label} coverage >= 95%",
            status="PASS" if pct >= 0.95 else "FAIL",
            rows_checked=len(rows),
            rows_failed=len(rows) - sum(1 for row in rows if str(row.get(field) or "").strip()),
            detail=f"{field}_coverage={pct * 100:.2f}%",
            artifact=str(path),
        )

    silent_unresolved = [
        row
        for row in rows
        if (
            "ambiguous" in str(row.get("identity_status") or "").lower()
            or "unresolved" in str(row.get("identity_status") or "").lower()
        )
        and not str(row.get("identity_warning") or "").strip()
    ]
    _add(
        results,
        category="identity",
        invariant="upload diagnostics ambiguous/unresolved identity rows carry identity_warning",
        status="PASS" if not silent_unresolved else "FAIL",
        rows_checked=len(rows),
        rows_failed=len(silent_unresolved),
        detail="ok" if not silent_unresolved else "ambiguous/unresolved upload identity silently passed without warning",
        artifact=str(path),
    )


def _daily_durability_invariants(results: list[InvariantResult], audit_date: str, out_root: Path) -> None:
    daily_index = out_root / "daily" / audit_date / "INDEX.md"
    _add(
        results,
        category="daily_durability",
        invariant="today's Daily Index exists",
        status="PASS" if daily_index.exists() else "FAIL",
        detail="ok" if daily_index.exists() else "missing Daily Index",
        artifact=str(daily_index),
    )

    context_health = out_root / "expanded_o15_universe" / f"expanded_o15_context_health_{audit_date}.json"
    context_payload = _read_json(context_health)
    context_status = str(context_payload.get("status") or "").lower()
    if not context_health.exists():
        status = "FAIL"
        detail = "missing expanded O1.5 context health for audit date"
    elif context_status not in {"pass", "ok"}:
        status = "FAIL"
        detail = f"expanded O1.5 context health status={context_status or 'unknown'}"
    else:
        status = "PASS"
        detail = "ok"
    _add(
        results,
        category="daily_durability",
        invariant="mlb-expanded-o15-context-health exists and passes for today",
        status=status,
        rows_checked=len(context_payload.get("checks") or []),
        detail=detail,
        artifact=str(context_health),
    )

    identity_health = out_root / "identity" / "mlb_identity_health_summary.json"
    identity_payload = _read_json(identity_health)
    identity_status = str(identity_payload.get("status") or "").lower()
    _add(
        results,
        category="daily_durability",
        invariant="mlb-identity-health exists",
        status="PASS" if identity_health.exists() and identity_status in {"pass", "warn"} else "FAIL",
        rows_checked=int(identity_payload.get("artifact_samples") or 0),
        detail=f"status={identity_status or 'missing'}",
        artifact=str(identity_health),
    )

    ontology_health = out_root / "ontology" / "ontology_health.json"
    ontology_payload = _read_json(ontology_health)
    ontology_status = str(ontology_payload.get("status") or "").lower()
    if not ontology_health.exists():
        status = "FAIL"
        detail = "missing O1.5 ontology health"
    elif ontology_status not in {"pass", "warn"}:
        status = "FAIL"
        detail = f"O1.5 ontology health status={ontology_status or 'unknown'}"
    else:
        status = "PASS"
        detail = f"status={ontology_status}; rows_checked={ontology_payload.get('rows_checked', '')}"
    _add(
        results,
        category="daily_durability",
        invariant="O1.5 ontology health exists and passes",
        status=status,
        rows_checked=int(ontology_payload.get("artifact_count") or 0),
        rows_failed=int(ontology_payload.get("invalid_rows") or 0),
        detail=detail,
        artifact=str(ontology_health),
    )

    morning_workflow = out_root / "morning_workflow" / "morning_workflow_audit_latest.json"
    workflow_payload = _read_json(morning_workflow)
    workflow_status = str(workflow_payload.get("status") or "").lower()
    workflow_date = str(workflow_payload.get("date") or "")
    workflow_blockers = int(workflow_payload.get("workflow_blockers") or 0)
    broken_links = int(workflow_payload.get("broken_links") or 0)
    missing_artifacts = int(workflow_payload.get("missing_artifacts") or 0)
    root_failures = int(workflow_payload.get("root_failures") or workflow_blockers or 0)
    if not morning_workflow.exists():
        status = "FAIL"
        detail = "missing morning workflow audit"
    elif workflow_date != audit_date:
        status = "FAIL"
        detail = f"morning workflow audit date mismatch: {workflow_date} != {audit_date}"
    elif root_failures or broken_links or missing_artifacts:
        status = "FAIL"
        detail = (
            f"status={workflow_status}; score={workflow_payload.get('workflow_health_score', '')}; "
            f"root_failures={root_failures}; broken_links={broken_links}; missing_artifacts={missing_artifacts}"
        )
    else:
        status = "PASS"
        detail = (
            f"status={workflow_status}; score={workflow_payload.get('workflow_health_score', '')}; "
            f"root_failures={root_failures}; broken_links={broken_links}; missing_artifacts={missing_artifacts}"
        )
    _add(
        results,
        category="daily_durability",
        invariant="morning workflow audit exists and has no structural blockers for today",
        status=status,
        rows_checked=1 if workflow_payload else 0,
        rows_failed=0 if status == "PASS" else 1,
        detail=detail,
        artifact=str(morning_workflow),
    )

    manifest = out_root / "research_snapshots" / "snapshot_manifest.csv"
    latest_snapshot = _latest_snapshot(manifest)
    due = _snapshot_due(audit_date)
    if latest_snapshot:
        detail = f"latest snapshot={latest_snapshot.get('snapshot_date')} week={latest_snapshot.get('week')}"
        status = "PASS"
    elif due:
        detail = "weekly research snapshot is due but missing"
        status = "WARN"
    else:
        detail = "not due yet"
        status = "PASS"
    _add(
        results,
        category="daily_durability",
        invariant="latest research snapshot exists or is not due yet",
        status=status,
        detail=detail,
        artifact=str(manifest),
    )


def _latest_snapshot(manifest: Path) -> dict[str, str] | None:
    rows = _read_csv(manifest)
    if not rows:
        return None
    return sorted(rows, key=lambda row: str(row.get("snapshot_date") or ""))[-1]


def _snapshot_due(audit_date: str) -> bool:
    try:
        d = date.fromisoformat(audit_date)
    except Exception:
        return False
    return d.weekday() == 6


def _research_invariants(results: list[InvariantResult], out_root: Path, daily_index: Path) -> None:
    builder = Path("backend/mlb/scripts/build_mlb_research_snapshot.py")
    text = builder.read_text(encoding="utf-8", errors="ignore") if builder.exists() else ""
    refuses_overwrite = "Refusing to overwrite immutable research snapshot" in text
    _add(
        results,
        category="research",
        invariant="research snapshots refuse overwrite",
        status="PASS" if refuses_overwrite else "FAIL",
        detail="ok" if refuses_overwrite else "snapshot builder does not appear to refuse overwrite",
        artifact=str(builder),
    )

    manifest = out_root / "research_snapshots" / "snapshot_manifest.csv"
    latest = _latest_snapshot(manifest)
    if latest:
        snapshot_path = str(latest.get("snapshot_path") or "")
        index_text = daily_index.read_text(encoding="utf-8", errors="ignore") if daily_index.exists() else ""
        linked = bool(snapshot_path) and snapshot_path in index_text
        _add(
            results,
            category="research",
            invariant="Daily Index links latest research snapshot when present",
            status="PASS" if linked else "WARN",
            detail="ok" if linked else f"latest snapshot not linked in Daily Index: {snapshot_path}",
            artifact=str(daily_index),
        )
    else:
        _add(
            results,
            category="research",
            invariant="Daily Index links latest research snapshot when present",
            status="PASS",
            detail="no snapshot present",
            artifact=str(daily_index),
        )


def _backfill_doctrine_invariants(results: list[InvariantResult], audit_date: str, out_root: Path) -> None:
    candidates: list[Path] = []
    for pattern in ("*backfill*.md", "*status*.md"):
        candidates.extend(out_root.rglob(pattern))
    date_candidates = [
        path for path in candidates
        if audit_date in path.as_posix() or _mtime_date(path) == audit_date
    ]
    missing_docs: list[str] = []
    doc_re = re.compile(r"(daily sustainability|research-only|research only|review aid only|not production|doctrine)", re.I)
    for path in date_candidates:
        text = path.read_text(encoding="utf-8", errors="ignore")
        if not doc_re.search(text):
            missing_docs.append(path.as_posix())
    _add(
        results,
        category="backfill_doctrine",
        invariant="new backfill/status artifacts document daily sustainability or research-only scope",
        status="PASS" if not missing_docs else "WARN",
        rows_checked=len(date_candidates),
        rows_failed=len(missing_docs),
        detail="ok" if not missing_docs else "; ".join(missing_docs[:6]),
        artifact=str(out_root),
    )


def _mtime_date(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).date().isoformat()
    except Exception:
        return ""


def _write_markdown(path: Path, results: list[InvariantResult], audit_date: str, generated: str) -> None:
    counts = {status: sum(1 for r in results if r.status == status) for status in ("PASS", "WARN", "FAIL")}
    overall = "FAIL" if counts["FAIL"] else "WARN" if counts["WARN"] else "PASS"
    lines = [
        f"# MLB Project Invariants - {audit_date}",
        "",
        f"- Generated UTC: `{generated}`",
        f"- Status: `{overall}`",
        f"- PASS/WARN/FAIL: `{counts['PASS']}` / `{counts['WARN']}` / `{counts['FAIL']}`",
        "- Scope: audit and visibility only; no production behavior changed.",
        "",
        "## Checks",
        "",
        "| category | invariant | status | checked | failed | detail | artifact |",
        "|---|---|---|---:|---:|---|---|",
    ]
    for row in results:
        detail = str(row.detail or "").replace("|", "\\|")
        lines.append(
            f"| {row.category} | {row.invariant} | `{row.status}` | `{row.rows_checked}` | `{row.rows_failed}` | {detail} | `{row.artifact}` |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit machine-checkable MLB project invariants.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/invariants")
    ap.add_argument("--out-root", default="artifacts/analysis/mlb")
    ap.add_argument("--hits-environment-csv", default="tmp/analysis/mlb_hits_environment_hits_allowed_rows.csv")
    ap.add_argument("--hits-environment-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    args = ap.parse_args()

    audit_date = str(args.date).strip()
    out_dir = Path(args.out_dir)
    out_root = Path(args.out_root)
    results: list[InvariantResult] = []

    _review_board_invariants(results, audit_date)
    _hits_environment_invariants(results, Path(args.hits_environment_csv), Path(args.hits_environment_json))
    _upload_diagnostics_invariants(results, audit_date)
    _daily_durability_invariants(results, audit_date, out_root)
    _research_invariants(results, out_root, out_root / "daily" / audit_date / "INDEX.md")
    _backfill_doctrine_invariants(results, audit_date, out_root)

    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    counts = {status: sum(1 for r in results if r.status == status) for status in ("PASS", "WARN", "FAIL")}
    overall = "FAIL" if counts["FAIL"] else "WARN" if counts["WARN"] else "PASS"
    csv_path = out_dir / f"mlb_project_invariants_{audit_date}.csv"
    md_path = out_dir / f"mlb_project_invariants_{audit_date}.md"
    json_path = out_dir / f"mlb_project_invariants_{audit_date}.json"
    latest_json = out_dir / "mlb_project_invariants_latest.json"

    payload = {
        "generated_at_utc": generated,
        "date": audit_date,
        "status": overall.lower(),
        "ok": overall == "PASS",
        "pass_count": counts["PASS"],
        "warn_count": counts["WARN"],
        "fail_count": counts["FAIL"],
        "checks": [asdict(row) for row in results],
        "csv": str(csv_path),
        "md": str(md_path),
    }
    _write_csv(csv_path, [asdict(row) for row in results])
    _write_markdown(md_path, results, audit_date, generated)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latest_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        f"[mlb-project-invariants] date={audit_date} status={overall.lower()} "
        f"fail={counts['FAIL']} warn={counts['WARN']} out_md={md_path}"
    )
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
