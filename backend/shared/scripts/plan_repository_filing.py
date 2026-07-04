#!/usr/bin/env python3
"""Plan repository artifact filing batches.

This script is intentionally planning-only: it writes maps and dry-run CSVs,
but never moves or deletes files.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


ALLOWED_BATCH_CLASSES = {"GENERATED_DAILY", "RESEARCH_EVIDENCE", "ARCHIVE"}
ALWAYS_ACTIVE_PATHS = {
    "artifacts/analysis/mlb/morning_gate_summary.md",
    "artifacts/analysis/mlb/morning_workflow_audit.csv",
    "artifacts/analysis/mlb/morning_workflow_validation.csv",
    "artifacts/analysis/mlb/morning_workflow_validation.md",
    "artifacts/analysis/mlb/morning_timing_template.md",
}
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")
TEXT_SUFFIXES = {
    ".csv",
    ".md",
    ".json",
    ".txt",
    ".py",
    ".sh",
    ".zsh",
    ".sql",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
    ".html",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".css",
    ".xml",
    ".plist",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def write_csv(path: Path, rows: Iterable[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_md(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def owner_slug(value: str) -> str:
    text = (value or "").strip().lower()
    if text in {"", "unknown", "unknown_owner"}:
        return "unknown"
    return text.replace(" ", "_").replace("/", "_")


def infer_sport(path: str, row: dict[str, str] | None = None) -> str:
    if row:
        sport = (row.get("sport") or "").strip().lower()
        if sport and sport != "unknown":
            return sport
    lower = path.lower()
    if "/mlb/" in lower or lower.startswith("artifacts/analysis/mlb") or lower.startswith("backend/mlb"):
        return "mlb"
    if "/nhl/" in lower or lower.startswith("artifacts/analysis/nhl") or lower.startswith("backend/nhl"):
        return "nhl"
    if "/nba/" in lower or lower.startswith("artifacts/analysis/nba") or lower.startswith("backend/nba"):
        return "nba"
    return "unknown"


def date_from_path(path: str) -> str:
    match = DATE_RE.search(path)
    return match.group(0) if match else "undated"


def source_rel(path: str) -> Path:
    p = Path(path)
    for root in (
        Path("artifacts/analysis/mlb"),
        Path("artifacts/analysis/nhl"),
        Path("artifacts/analysis/nba"),
        Path("artifacts/analysis"),
    ):
        try:
            return p.relative_to(root)
        except ValueError:
            continue
    return Path(*p.parts)


def date_stripped_rel(path: str) -> Path:
    rel = source_rel(path)
    parts = list(rel.parts)
    return Path(*[part for part in parts if not DATE_RE.fullmatch(part)])


def final_home(row: dict[str, str], *, path_key: str = "current_path") -> str:
    current_path = row.get(path_key) or row.get("current_path") or ""
    lifecycle = row.get("lifecycle_class") or ""
    owner = owner_slug(row.get("owner_research_thread") or row.get("owner") or "")
    sport = infer_sport(current_path, row)
    date_text = date_from_path(current_path)
    rel = source_rel(current_path)
    stripped = date_stripped_rel(current_path)
    if lifecycle == "GENERATED_DAILY":
        return (Path("artifacts/archive/generated_daily") / sport / date_text / stripped).as_posix()
    if lifecycle == "RESEARCH_EVIDENCE":
        owner_part = owner if owner != "unknown" else "needs_owner_review"
        return (Path("artifacts/archive/research_evidence") / owner_part / rel).as_posix()
    if lifecycle == "ARCHIVE":
        owner_part = owner if owner != "unknown" else sport
        return (Path("artifacts/archive/legacy") / owner_part / rel).as_posix()
    if lifecycle == "RECONCILE_INPUT":
        return (Path("artifacts/archive/reconcile_inputs") / sport / date_text / stripped).as_posix()
    if lifecycle == "DISPOSABLE":
        return (Path("artifacts/archive/disposable_candidates") / sport / rel).as_posix()
    return current_path


def is_text_file(path: Path) -> bool:
    return path.name == "Makefile" or path.suffix.lower() in TEXT_SUFFIXES


def active_files(roots: list[str]) -> list[Path]:
    files: list[Path] = []
    for root_text in roots:
        root = Path(root_text)
        if not root.exists():
            continue
        paths = [root] if root.is_file() else root.rglob("*")
        for path in paths:
            if not path.is_file():
                continue
            text = path.as_posix()
            if "artifacts/archive/" in text:
                continue
            if text.startswith("artifacts/analysis/repository_hygiene/"):
                continue
            if "/.git/" in text:
                continue
            if is_text_file(path):
                files.append(path)
    return files


def resolve_link(source: Path, href: str, repo_root: Path) -> str | None:
    href = href.strip().split("#", 1)[0]
    if not href or href.startswith("#") or "://" in href or href.startswith("mailto:"):
        return None
    target = Path(href)
    if not target.is_absolute():
        target = source.parent / target
    try:
        return target.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return target.as_posix().lstrip("./")


def markdown_link_refs(
    candidate_paths: set[str],
    files: list[Path],
    entrypoints: list[Path],
    repo_root: Path,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    markdown_refs: dict[str, list[str]] = defaultdict(list)
    entry_refs: dict[str, list[str]] = defaultdict(list)
    md_files = [path for path in files if path.suffix.lower() == ".md"]
    for entry in entrypoints:
        if entry.exists() and entry not in md_files:
            md_files.append(entry)
    for path in md_files:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for label, href in LINK_RE.findall(text):
            target = resolve_link(path, href, repo_root)
            if not target or target not in candidate_paths:
                continue
            desc = f"{path.as_posix()}: markdown link label={label}"
            markdown_refs[target].append(desc)
            source_text = path.as_posix()
            if (
                path in entrypoints
                or source_text.startswith("artifacts/analysis/mlb/daily/")
                or "morning" in source_text
                or "ops_brief" in source_text
            ):
                entry_refs[target].append(desc)
    return markdown_refs, entry_refs


def exact_refs(candidate_paths: list[str], roots: list[str]) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    if not candidate_paths:
        return {}, {}
    pattern = Path("/tmp/proppadia_repository_filing_candidate_paths.txt")
    pattern.write_text("\n".join(candidate_paths) + "\n", encoding="utf-8")
    cmd = [
        "rg",
        "-n",
        "-F",
        "-f",
        str(pattern),
        *roots,
        "--glob",
        "!artifacts/analysis/repository_hygiene/**",
        "--glob",
        "!artifacts/archive/**",
        "--glob",
        "!**/.git/**",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    exact: dict[str, list[str]] = defaultdict(list)
    generator: dict[str, list[str]] = defaultdict(list)
    for line in proc.stdout.splitlines():
        file_part = line.split(":", 1)[0]
        for candidate in candidate_paths:
            if candidate not in line:
                continue
            exact[candidate].append(line[:500])
            source = Path(file_part)
            if source.name == "Makefile" or source.suffix in {".py", ".sh", ".zsh"}:
                generator[candidate].append(line[:500])
    return exact, generator


def moved_paths(manifests: list[Path]) -> set[str]:
    out: set[str] = set()
    for manifest in manifests:
        for row in read_csv(manifest):
            out.add(row.get("current_path") or "")
    return out


def candidate_rows(plan: list[dict[str, str]], moved: set[str], today: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    forbidden = (
        "backend/mlb/data/processed/mlb_uploads/",
        "backend/mlb/exports/odds_history/",
        "execution_reconcile",
        "/tmp/",
        "tmp/",
    )
    for row in plan:
        current = row.get("current_path") or ""
        path = Path(current)
        if not current or current in moved:
            continue
        if current in ALWAYS_ACTIVE_PATHS:
            continue
        if row.get("safe_to_move_now", "").lower() != "yes":
            continue
        if row.get("lifecycle_class") not in ALLOWED_BATCH_CLASSES:
            continue
        if owner_slug(row.get("owner_research_thread", "")) == "unknown":
            continue
        if today in current:
            continue
        if "latest" in path.name.lower() or "/latest" in current.lower():
            continue
        if any(token in current for token in forbidden):
            continue
        if current.startswith("backend/") or current.startswith("docs/") or current == "Makefile":
            continue
        if not path.exists() or path.is_symlink() or not path.is_file():
            continue
        rows.append(row)
    return rows


def build_risks(
    rows: list[dict[str, str]],
    scan_roots: list[str],
    entrypoints: list[Path],
    repo_root: Path,
) -> list[dict[str, object]]:
    candidate_list = [row["current_path"] for row in rows]
    candidate_set = set(candidate_list)
    files = active_files(scan_roots)
    md_refs, entry_refs = markdown_link_refs(candidate_set, files, entrypoints, repo_root)
    exact, generator = exact_refs(candidate_list, scan_roots)
    risk_rows: list[dict[str, object]] = []
    for row in rows:
        current = row["current_path"]
        risks: list[tuple[str, list[str]]] = []
        if entry_refs.get(current):
            risks.append(("active_entrypoint_markdown_link", entry_refs[current]))
        elif md_refs.get(current):
            risks.append(("markdown_link_reference", md_refs[current]))
        if generator.get(current):
            risks.append(("active_generator_or_makefile_reference", generator[current]))
        elif exact.get(current):
            risks.append(("exact_path_reference", exact[current]))
        if risks:
            risk_types = [risk_type for risk_type, _ in risks]
            level = "high" if any(
                risk_type in {"active_entrypoint_markdown_link", "active_generator_or_makefile_reference"}
                for risk_type in risk_types
            ) else "medium"
            examples: list[str] = []
            for _, values in risks:
                examples.extend(values[:3])
            risk_rows.append(
                {
                    "current_path": current,
                    "proposed_destination": final_home(row),
                    "included_in_batch": "no",
                    "risk_level": level,
                    "risk_type": ";".join(risk_types),
                    "references_found": sum(len(values) for _, values in risks),
                    "reference_examples": " | ".join(examples[:5]),
                    "recommendation": "exclude_from_batch_or_plan_stub_strategy",
                }
            )
        else:
            risk_rows.append(
                {
                    "current_path": current,
                    "proposed_destination": final_home(row),
                    "included_in_batch": "candidate",
                    "risk_level": "none",
                    "risk_type": "",
                    "references_found": 0,
                    "reference_examples": "",
                    "recommendation": "ok_to_move_candidate_pool",
                }
            )
    return risk_rows


def build_filing_map(
    plan_rows: list[dict[str, str]],
    manifests: list[tuple[str, Path]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for source, manifest in manifests:
        for row in read_csv(manifest):
            normalized = {
                "current_path": row.get("current_path", ""),
                "lifecycle_class": row.get("lifecycle_class", ""),
                "owner_research_thread": row.get("owner", ""),
                "sport": infer_sport(row.get("current_path", "")),
            }
            final = final_home(normalized)
            current_archive = row.get("new_path", "")
            fits = "yes" if current_archive == final else "partial"
            action = "none" if fits == "yes" else "corrective_followup_not_in_this_task"
            if "unknown_owner" in current_archive:
                action = "normalize_unknown_owner_to_inferred_owner"
            elif row.get("lifecycle_class") == "GENERATED_DAILY" and not current_archive.startswith(
                f"artifacts/archive/generated_daily/{infer_sport(row.get('current_path', ''))}/"
            ):
                action = "normalize_generated_daily_to_sport_date"
            rows.append(
                {
                    "source": source,
                    "current_path": row.get("current_path", ""),
                    "current_archive_path": current_archive,
                    "lifecycle_class": row.get("lifecycle_class", ""),
                    "owner_research_thread": row.get("owner", ""),
                    "normalized_final_home": final,
                    "fits_normalized_architecture": fits,
                    "recommended_action": action,
                    "reason": "existing_archive_path_matches_normalized_home" if fits == "yes" else "existing_archive_path_differs_from_normalized_home",
                }
            )
    moved = {str(row["current_path"]) for row in rows}
    for row in plan_rows:
        current = row.get("current_path", "")
        if row.get("safe_to_move_now") != "yes" or current in moved:
            continue
        rows.append(
            {
                "source": "movement_plan",
                "current_path": current,
                "current_archive_path": "",
                "lifecycle_class": row.get("lifecycle_class", ""),
                "owner_research_thread": row.get("owner_research_thread", ""),
                "normalized_final_home": final_home(row),
                "fits_normalized_architecture": "planned",
                "recommended_action": "use_normalized_destination_for_future_batch",
                "reason": "planned_destination_from_normalized_lifecycle_rules",
            }
        )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-07-03")
    parser.add_argument("--batch", default="4")
    parser.add_argument("--movement-plan", type=Path, default=Path("artifacts/analysis/repository_hygiene/repository_working_set_movement_plan_2026-07-02.csv"))
    parser.add_argument("--batch1-manifest", type=Path, default=Path("artifacts/analysis/repository_hygiene/repository_batch1_move_manifest_2026-07-02.csv"))
    parser.add_argument("--batch2-manifest", type=Path, default=Path("artifacts/analysis/repository_hygiene/repository_batch2_move_manifest_2026-07-03.csv"))
    parser.add_argument(
        "--moved-manifest",
        action="append",
        type=Path,
        default=[],
        help="Additional move manifest to exclude from future batches and include in normalized filing map.",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("artifacts/analysis/repository_hygiene"))
    parser.add_argument("--limit", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(".").resolve()
    plan = read_csv(args.movement_plan)
    manifests = [("batch1", args.batch1_manifest), ("batch2", args.batch2_manifest)]
    manifests.extend((manifest.stem, manifest) for manifest in args.moved_manifest)
    moved = moved_paths([manifest for _, manifest in manifests])
    scan_roots = ["Makefile", "backend", "docs", "artifacts/analysis"]
    entrypoints = [
        Path(f"artifacts/analysis/mlb/daily/{args.date}/INDEX.md"),
        Path(f"artifacts/analysis/mlb/mlb_daily_ops_brief_{args.date}.md"),
        Path("artifacts/analysis/mlb/morning_gate_summary.md"),
        Path("artifacts/analysis/mlb/morning_workflow_validation.md"),
        Path("artifacts/analysis/mlb/review_aids/performance/o15_morning_workbench.md"),
        Path("artifacts/analysis/mlb/review_aids/performance/README.md"),
        Path("artifacts/analysis/mlb/feature_lineage/README.md"),
    ]

    filing_map = build_filing_map(plan, manifests)
    map_path = args.out_dir / f"repository_filing_map_normalized_{args.date}.csv"
    write_csv(
        map_path,
        filing_map,
        [
            "source",
            "current_path",
            "current_archive_path",
            "lifecycle_class",
            "owner_research_thread",
            "normalized_final_home",
            "fits_normalized_architecture",
            "recommended_action",
            "reason",
        ],
    )

    candidates = candidate_rows(plan, moved, args.date)
    risk_rows = build_risks(candidates, scan_roots, entrypoints, repo_root)
    risk_by_path = {str(row["current_path"]): row for row in risk_rows}
    eligible = [row for row in candidates if risk_by_path[row["current_path"]]["risk_level"] == "none"]
    selected = eligible[: args.limit]
    selected_paths = {row["current_path"] for row in selected}
    for risk in risk_rows:
        if risk["current_path"] in selected_paths:
            risk["included_in_batch"] = "yes"
            risk["recommendation"] = f"ok_to_move_in_batch{args.batch}"
        elif risk["risk_level"] == "none":
            risk["included_in_batch"] = "no"
            risk["recommendation"] = "not_selected_batch_size_limit"

    batch_rows = [
        {
            "current_path": row["current_path"],
            "proposed_destination": final_home(row),
            "lifecycle_class": row["lifecycle_class"],
            "owner_research_thread": row["owner_research_thread"],
            "reason": row["reason"],
            "confidence": row["confidence"],
            "why_safe": "Not moved in Batch 1/2; normalized destination follows final filing architecture; non-current, non-latest, non-canonical, non-reconcile artifact; upload/odds/reconcile/tmp/source paths excluded; hardened scanner found no active generator, Makefile, Morning/Home/Ops/Workbench, README/navigation, or markdown-link references.",
            "path_risk_level": "none",
        }
        for row in selected
    ]
    dry_path = args.out_dir / f"repository_batch{args.batch}_safe_move_dry_run_{args.date}.csv"
    write_csv(
        dry_path,
        batch_rows,
        [
            "current_path",
            "proposed_destination",
            "lifecycle_class",
            "owner_research_thread",
            "reason",
            "confidence",
            "why_safe",
            "path_risk_level",
        ],
    )

    risk_path = args.out_dir / f"repository_batch{args.batch}_path_risks_{args.date}.csv"
    write_csv(
        risk_path,
        risk_rows,
        [
            "current_path",
            "proposed_destination",
            "included_in_batch",
            "risk_level",
            "risk_type",
            "references_found",
            "reference_examples",
            "recommendation",
        ],
    )

    by_dir = Counter(str(Path(row["current_path"]).parent) for row in selected)
    impact_rows: list[dict[str, object]] = []
    for directory, count in sorted(by_dir.items(), key=lambda item: (-item[1], item[0])):
        path = Path(directory)
        visible = sum(1 for child in path.iterdir() if child.is_file() or child.is_symlink()) if path.exists() else 0
        impact_rows.append(
            {
                "directory": directory,
                "current_visible_file_count": visible,
                "files_proposed_to_move": count,
                "estimated_remaining_visible_count": max(visible - count, 0),
            }
        )
    impact_path = args.out_dir / f"repository_batch{args.batch}_directory_impact_{args.date}.csv"
    write_csv(
        impact_path,
        impact_rows,
        ["directory", "current_visible_file_count", "files_proposed_to_move", "estimated_remaining_visible_count"],
    )

    batch_existing = [row for row in filing_map if row["source"] in {"batch1", "batch2"}]
    unknown_normalized = [
        row for row in batch_existing
        if "unknown_owner" in str(row["current_archive_path"]) and "needs_owner_review" not in str(row["normalized_final_home"])
    ]
    generated_normalized = [
        row for row in batch_existing
        if row["lifecycle_class"] == "GENERATED_DAILY"
        and not str(row["current_archive_path"]).startswith(f"artifacts/archive/generated_daily/{infer_sport(str(row['current_path']))}/")
    ]
    risks = [row for row in risk_rows if row["risk_level"] != "none"]
    risk_types = Counter()
    for risk in risks:
        for risk_type in str(risk["risk_type"]).split(";"):
            if risk_type:
                risk_types[risk_type] += 1
    class_counts = Counter(row["lifecycle_class"] for row in selected)
    owner_counts = Counter(row["owner_research_thread"] for row in selected)

    summary_path = args.out_dir / f"repository_batch{args.batch}_safe_move_summary_{args.date}.md"
    summary = [
        f"# Repository Working-Set Reduction Batch {args.batch} Dry Run",
        "",
        f"- Date: `{args.date}`",
        "- Mode: dry-run only",
        "- Files moved: `0`",
        "- Files deleted: `0`",
        "",
        "## Recommendation",
        "",
        f"- Actual movement recommendation: `{'YES_FOR_LISTED_NO_RISK_BATCH' if len(selected) >= 300 else 'NO_OR_REDUCE_BATCH'}`",
        f"- Files proposed: `{len(selected)}`",
        f"- Directories affected: `{len(impact_rows)}`",
        "- Included path risk level: `none`",
        f"- Hardened scanner risks excluded: `{len(risks)}`",
        "",
        "## Lifecycle Classes",
        "",
    ]
    for key, value in class_counts.most_common():
        summary.append(f"- `{key}`: `{value}`")
    summary.extend(["", "## Owners Represented", ""])
    for key, value in owner_counts.most_common():
        summary.append(f"- `{key}`: `{value}`")
    summary.extend(["", "## Path Risk Results", ""])
    if risk_types:
        for key, value in risk_types.most_common():
            summary.append(f"- `{key}`: `{value}`")
    else:
        summary.append("- No risks detected in candidate universe.")
    summary.extend(["", "## Destination Normalization", ""])
    summary.append(f"- Batch {args.batch} proposed destinations use normalized final-home rules, not the older movement-plan destinations.")
    summary.append("- Collision fallback should preserve source subpath under the normalized lifecycle root.")
    write_md(summary_path, summary)

    report_path = args.out_dir / f"repository_filing_normalization_{args.date}.md"
    report = [
        "# Repository Filing Normalization",
        "",
        f"- Date: `{args.date}`",
        "- Scope: destination-rule normalization and dry-run planning only",
        "- Files moved: `0`",
        "- Files deleted: `0`",
        "",
        "## Destination Rules Changed",
        "",
        "- `RESEARCH_EVIDENCE` now resolves to `artifacts/archive/research_evidence/{owner_or_thread}/{source_subpath}`.",
        "- `GENERATED_DAILY` now resolves to `artifacts/archive/generated_daily/{sport}/{date}/{source_subpath_without_date_segment}`.",
        "- `ARCHIVE` now resolves to `artifacts/archive/legacy/{owner_or_sport}/{source_subpath}`.",
        "- Unknown ownership is routed to `needs_owner_review` unless owner can be inferred from the classification row or manifest owner.",
        "- Collision fallback must stay inside the normalized lifecycle root and preserve source subpath context; it must not create ad hoc owner branches.",
        "",
        "## Batch 1/2 Normalization Review",
        "",
        f"- Unknown-owner archive cases normalized by inferred owner in the map: `{len(unknown_normalized)}`",
        f"- Generated-daily archive cases normalized under sport/date in the map: `{len(generated_normalized)}`",
        "- Existing Batch 1/2 files were not moved in this task.",
        "- Corrective follow-up is recommended for existing partial-fit archive paths before large-scale maintenance mode.",
        "",
        f"## Batch {args.batch} Dry Run",
        "",
        f"- Candidate universe after lifecycle/safety filters: `{len(candidates)}`",
        f"- Referenced/risky candidates excluded: `{len(risks)}`",
        f"- Files proposed for Batch {args.batch}: `{len(selected)}`",
        f"- Directories affected: `{len(impact_rows)}`",
        f"- Movement recommendation: `{'YES_FOR_LISTED_NO_RISK_BATCH' if len(selected) >= 300 else 'NO_OR_REDUCE_BATCH'}`",
        "",
        "## Confirmation",
        "",
        "- No files were moved.",
        "- No files were deleted.",
        "- No production behavior changed.",
    ]
    write_md(report_path, report)

    print(f"normalized_map={map_path}")
    print(f"batch_dry_run={dry_path}")
    print(f"batch_path_risks={risk_path}")
    print(f"batch_directory_impact={impact_path}")
    print(f"batch_summary={summary_path}")
    print(f"normalization_report={report_path}")
    print(f"unknown_owner_normalized={len(unknown_normalized)}")
    print(f"generated_daily_normalized={len(generated_normalized)}")
    print(f"batch_files={len(selected)}")
    print(f"batch_risks={len(risks)}")


if __name__ == "__main__":
    main()
