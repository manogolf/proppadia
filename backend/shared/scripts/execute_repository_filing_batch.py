#!/usr/bin/env python3
"""Execute a planned repository filing batch.

This script moves only rows listed in a dry-run CSV, writes a hash manifest,
scans old-path references, runs validation gates, and writes the batch report.
It never deletes files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_md(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_command(command: list[str], *, check: bool = False) -> tuple[str, int, str]:
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    output = (proc.stdout or "") + (proc.stderr or "")
    label = " ".join(command)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{label} failed with rc={proc.returncode}\n{output}")
    return label, proc.returncode, output


def validate_dry_run(rows: list[dict[str, str]]) -> None:
    if not rows:
        raise SystemExit("dry-run has no rows")
    destinations = [row["proposed_destination"] for row in rows]
    duplicates = len(destinations) - len(set(destinations))
    if duplicates:
        raise SystemExit(f"dry-run has {duplicates} duplicate destinations")
    forbidden_tokens = (
        "backend/mlb/data/processed/mlb_uploads/",
        "backend/mlb/exports/odds_history/",
        "execution_reconcile",
        "/tmp/",
        "tmp/",
    )
    for row in rows:
        source = row["current_path"]
        dest = row["proposed_destination"]
        if row.get("path_risk_level") != "none":
            raise SystemExit(f"risk row included: {source}")
        if "2026-07-03" in source:
            raise SystemExit(f"current-date file included: {source}")
        if "latest" in Path(source).name.lower() or "/latest" in source.lower():
            raise SystemExit(f"latest file included: {source}")
        if any(token in source for token in forbidden_tokens):
            raise SystemExit(f"forbidden source path included: {source}")
        if source.startswith("backend/") or source.startswith("docs/") or source == "Makefile":
            raise SystemExit(f"source/code/doc path included: {source}")
        if Path(dest).exists() or Path(dest).is_symlink():
            raise SystemExit(f"destination already exists: {dest}")
        path = Path(source)
        if not path.exists() or not path.is_file() or path.is_symlink():
            raise SystemExit(f"source missing or not regular file: {source}")


def move_rows(rows: list[dict[str, str]], manifest_path: Path) -> list[dict[str, object]]:
    moved_at = datetime.now(timezone.utc).isoformat()
    manifest_rows: list[dict[str, object]] = []
    for row in rows:
        source = Path(row["current_path"])
        dest = Path(row["proposed_destination"])
        before = sha256(source)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.rename(source, dest)
        after = sha256(dest)
        ok = before == after and dest.exists() and not source.exists()
        manifest_rows.append(
            {
                "current_path": source.as_posix(),
                "new_path": dest.as_posix(),
                "owner": row.get("owner_research_thread", ""),
                "lifecycle_class": row.get("lifecycle_class", ""),
                "sha256_before": before,
                "sha256_after": after,
                "moved_timestamp": moved_at,
                "moved_successfully": "true" if ok else "false",
                "notes": "moved_as_planned_normalized_destination" if ok else "move_verification_failed",
            }
        )
    write_csv(
        manifest_path,
        manifest_rows,
        [
            "current_path",
            "new_path",
            "owner",
            "lifecycle_class",
            "sha256_before",
            "sha256_after",
            "moved_timestamp",
            "moved_successfully",
            "notes",
        ],
    )
    return manifest_rows


def verify_manifest(manifest_rows: list[dict[str, object]]) -> dict[str, int]:
    return {
        "missing_new": sum(1 for row in manifest_rows if not Path(str(row["new_path"])).exists()),
        "old_paths_still_exist": sum(
            1 for row in manifest_rows if Path(str(row["current_path"])).exists() or Path(str(row["current_path"])).is_symlink()
        ),
        "hash_mismatches": sum(1 for row in manifest_rows if row["sha256_before"] != row["sha256_after"]),
        "move_failures": sum(1 for row in manifest_rows if row["moved_successfully"] != "true"),
    }


def scan_old_references(manifest_rows: list[dict[str, object]], batch: str) -> list[str]:
    pattern = Path(f"/tmp/proppadia_batch{batch}_old_paths.txt")
    pattern.write_text("\n".join(str(row["current_path"]) for row in manifest_rows) + "\n", encoding="utf-8")
    command = [
        "rg",
        "-n",
        "-F",
        "-f",
        str(pattern),
        "Makefile",
        "docs",
        "backend",
        "artifacts/analysis",
        "--glob",
        "!artifacts/analysis/repository_hygiene/**",
        "--glob",
        "!artifacts/archive/**",
        "--glob",
        "!**/.git/**",
    ]
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    return [line for line in proc.stdout.splitlines() if line.strip()]


def directory_reduction(impact_path: Path) -> tuple[list[dict[str, object]], int]:
    rows = read_csv(impact_path)
    out: list[dict[str, object]] = []
    total = 0
    for row in rows:
        directory = Path(row["directory"])
        before = int(row.get("current_visible_file_count") or 0)
        planned = int(row.get("files_proposed_to_move") or 0)
        after = sum(1 for child in directory.iterdir() if child.is_file() or child.is_symlink()) if directory.exists() else 0
        reduction = before - after
        total += reduction
        out.append(
            {
                "directory": row["directory"],
                "before": before,
                "planned": planned,
                "after": after,
                "reduction": reduction,
            }
        )
    return out, total


def run_validations(date_text: str, *, include_wrapper: bool) -> list[dict[str, object]]:
    commands = [
        ["make", "mlb-morning-workflow-audit", f"DATE={date_text}"],
        ["make", "mlb-project-invariants", f"DATE={date_text}"],
        ["make", "mlb-daily-preflight", f"DATE={date_text}"],
    ]
    if include_wrapper:
        commands.append(["/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh", "--check"])
    commands.append(["git", "diff", "--check"])
    results: list[dict[str, object]] = []
    for command in commands:
        label, rc, output = run_command(command)
        results.append({"command": label, "returncode": rc, "status": "PASS" if rc == 0 else "FAIL", "output": output[-4000:]})
        if rc != 0:
            break
    return results


def write_report(
    *,
    report_path: Path,
    batch: str,
    date_text: str,
    dry_run_path: Path,
    manifest_path: Path,
    dry_rows: list[dict[str, str]],
    manifest_rows: list[dict[str, object]],
    verification: dict[str, int],
    old_refs: list[str],
    reductions: list[dict[str, object]],
    reduction_total: int,
    validation_results: list[dict[str, object]],
    total_refiled: int,
    visible_reduction_estimate: int,
    remaining_movable: int,
) -> None:
    classes = Counter(str(row["lifecycle_class"]) for row in manifest_rows)
    owners = Counter(str(row["owner"]) for row in manifest_rows)
    validation_pass = all(row["status"] == "PASS" for row in validation_results)
    lines = [
        f"# Repository Working-Set Reduction Batch {batch} Move Report",
        "",
        f"- Date: `{date_text}`",
        f"- Source dry-run: `{dry_run_path.as_posix()}`",
        f"- Move manifest: `{manifest_path.as_posix()}`",
        "- Filing architecture: normalized",
        "",
        "## Summary",
        "",
        f"- Files planned: `{len(dry_rows)}`",
        f"- Files moved: `{sum(1 for row in manifest_rows if row['moved_successfully'] == 'true')}`",
        "- Files deleted: `0`",
        f"- Hash mismatches: `{verification['hash_mismatches']}`",
        f"- Missing new paths: `{verification['missing_new']}`",
        f"- Original paths still present: `{verification['old_paths_still_exist']}`",
        f"- Move failures: `{verification['move_failures']}`",
        f"- Old-path references found in active scan: `{len(old_refs)}`",
        "- Navigation stubs created: `0`",
        f"- Rollback required: `{'no' if validation_pass and not old_refs and not any(verification.values()) else 'yes'}`",
        "",
        "## Lifecycle Classes",
        "",
    ]
    for key, value in classes.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Owners", ""])
    for key, value in owners.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Reference Scan", ""])
    if old_refs:
        for line in old_refs[:50]:
            lines.append(f"- `{line}`")
    else:
        lines.append("- No exact old-path references found in active scanned areas.")
    lines.extend(["", "## Directory Counts", ""])
    lines.append("| directory | before visible files | planned moves | after visible files | net reduction |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in sorted(reductions, key=lambda item: (-int(item["planned"]), str(item["directory"])))[:60]:
        lines.append(
            f"| `{row['directory']}` | `{row['before']}` | `{row['planned']}` | `{row['after']}` | `{row['reduction']}` |"
        )
    if len(reductions) > 60:
        lines.append("| ... | ... | ... | ... | ... |")
    lines.extend(
        [
            "",
            f"- Directories affected: `{len(reductions)}`",
            f"- Batch direct visible reduction: `{reduction_total}` files/symlinks",
            "",
            "## Validation Summary",
            "",
        ]
    )
    for result in validation_results:
        lines.append(f"- `{result['command']}`: `{result['status']}`")
    lines.extend(
        [
            "",
            "## Repository Working-Set Status",
            "",
            f"- Total files safely refiled after this batch: `{total_refiled}`",
            f"- Estimated visible clutter reduction after this batch: `{visible_reduction_estimate}` files/symlinks",
            f"- Remaining proposed movable files from original movement plan: `{remaining_movable}`",
            f"- Repository maintenance healthy: `{'yes' if validation_pass and not old_refs and not any(verification.values()) else 'no'}`",
            "",
            "## Recommendation",
            "",
            "Continue with the next controlled batch only if this batch has zero validation failures.",
        ]
    )
    write_md(report_path, lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--dry-run", type=Path, required=True)
    parser.add_argument("--impact", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, default=Path("artifacts/analysis/repository_hygiene"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--no-wrapper", action="store_true")
    parser.add_argument("--total-refiled-before", type=int, default=0)
    parser.add_argument("--visible-reduction-before", type=int, default=0)
    parser.add_argument("--remaining-movable", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dry_rows = read_csv(args.dry_run)
    validate_dry_run(dry_rows)
    manifest_path = args.manifest or args.out_dir / f"repository_batch{args.batch}_move_manifest_{args.date}.csv"
    report_path = args.report or args.out_dir / f"repository_batch{args.batch}_move_report_{args.date}.md"
    manifest_rows = move_rows(dry_rows, manifest_path)
    verification = verify_manifest(manifest_rows)
    old_refs = scan_old_references(manifest_rows, args.batch)
    reductions, reduction_total = directory_reduction(args.impact)
    validation_results = run_validations(args.date, include_wrapper=not args.no_wrapper)
    total_refiled = args.total_refiled_before + sum(1 for row in manifest_rows if row["moved_successfully"] == "true")
    visible_reduction = args.visible_reduction_before + reduction_total
    write_report(
        report_path=report_path,
        batch=args.batch,
        date_text=args.date,
        dry_run_path=args.dry_run,
        manifest_path=manifest_path,
        dry_rows=dry_rows,
        manifest_rows=manifest_rows,
        verification=verification,
        old_refs=old_refs,
        reductions=reductions,
        reduction_total=reduction_total,
        validation_results=validation_results,
        total_refiled=total_refiled,
        visible_reduction_estimate=visible_reduction,
        remaining_movable=args.remaining_movable,
    )
    print(f"manifest={manifest_path}")
    print(f"report={report_path}")
    print(f"files_moved={sum(1 for row in manifest_rows if row['moved_successfully'] == 'true')}")
    print(f"hash_mismatches={verification['hash_mismatches']}")
    print(f"old_path_refs={len(old_refs)}")
    print(f"visible_reduction={reduction_total}")
    failed_validations = [row for row in validation_results if row["status"] != "PASS"]
    print(f"validation_failures={len(failed_validations)}")
    if old_refs or any(verification.values()) or failed_validations:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
