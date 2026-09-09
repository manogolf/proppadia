#!/usr/bin/env python3
"""Path-only credential containment for generated repository evidence.

This utility never reads an API key from the environment, never sends a request,
and never emits a matched value.  It mechanically replaces credential-shaped
values following recognized key names in generated text files.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_oddsapi_historical_joint_strength_transfer_v1/2026-09-09"
)
GENERATED_ROOTS = (
    ROOT / "artifacts",
    ROOT / "nhl daily run logs",
    ROOT / "logs",
)
CREDENTIAL_TEXT = (
    r"(?i)(?<![A-Za-z0-9_])(?:apiKey|api_key|ODDS_API_KEY|THE_ODDS_API_KEY)"
    r"(?:[\"']?\s*(?:=|:)|%3[dD])\s*[\"']?[A-Za-z0-9_-]{20,}"
)
GIT_CREDENTIAL_ERE = (
    r"(^|[^[:alnum:]_])(apiKey|api_key|ODDS_API_KEY|THE_ODDS_API_KEY)"
    r"(\"?[[:space:]]*(=|:)|%3[Dd])[[:space:]]*\"?[A-Za-z0-9_-]{20,}"
)
REDACTED_TEXT = (
    r"(?i)(?<![A-Za-z0-9_])(?:apiKey|api_key|ODDS_API_KEY|THE_ODDS_API_KEY)"
    r"(?:[\"']?\s*(?:=|:)|%3[dD])\s*[\"']?\[REDACTED\]"
)
CREDENTIAL = re.compile(
    rb"(?i)((?<![A-Za-z0-9_])(?:apiKey|api_key|ODDS_API_KEY|THE_ODDS_API_KEY)"
    rb"(?:[\"']?\s*(?:=|:)|%3[dD])\s*[\"']?)([A-Za-z0-9_-]{20,})"
)
REDACTED = rb"\1[REDACTED]"
RESTRICTED_PATH = Path("artifacts/analysis/network_monitor/2026-09-03/private_packet_capture")
GOTRUE_DOCUMENTATION = Path(".venv/lib/python3.11/site-packages/gotrue-2.12.0.dist-info/METADATA")
PACKAGE_PATHS = (
    "artifacts/analysis/model_development/mlb_oddsapi_historical_joint_strength_transfer_v1/2026-09-09",
    "backend/mlb/scripts/acquire_and_audit_mlb_oddsapi_historical_joint_strength_transfer_v1.py",
    "backend/mlb/scripts/audit_mlb_oddsapi_credential_containment_v1.py",
    "backend/mlb/tests/test_mlb_oddsapi_historical_joint_strength_transfer_v1.py",
)


def rg_paths(pattern: str) -> list[Path]:
    roots = [str(path) for path in GENERATED_ROOTS if path.exists()]
    completed = subprocess.run(
        ["rg", "-l", "-0", "-I", "--no-ignore", "--pcre2", pattern, *roots],
        cwd=ROOT, capture_output=True, check=False,
    )
    return sorted({Path(raw.decode()) for raw in completed.stdout.split(b"\0") if raw})


def git_history_candidates() -> list[Path]:
    completed = subprocess.run(
        ["git", "log", "--all", "--extended-regexp", f"-G{GIT_CREDENTIAL_ERE}",
         "--name-only", "--pretty=format:", "--", "."],
        cwd=ROOT, capture_output=True, text=True, check=True,
    )
    return sorted({Path(line) for line in completed.stdout.splitlines() if line.strip()})


def committed_package_candidates() -> list[Path]:
    completed = subprocess.run(
        ["git", "grep", "-I", "-l", "-E", GIT_CREDENTIAL_ERE, "HEAD", "--", *PACKAGE_PATHS],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if completed.returncode not in (0, 1):
        raise RuntimeError("Committed package credential scan failed")
    return sorted({Path(line) for line in completed.stdout.splitlines() if line.strip()})


def scan_and_optionally_redact(remediate: bool) -> tuple[list[dict[str, object]], list[str]]:
    findings: list[dict[str, object]] = []
    unreadable = [ROOT / RESTRICTED_PATH]
    for path in rg_paths(CREDENTIAL_TEXT):
        try:
            data = path.read_bytes()
        except (OSError, PermissionError):
            unreadable.append(path)
            continue
        if not CREDENTIAL.search(data):
            continue
        remediated = False
        if remediate:
            scrubbed = CREDENTIAL.sub(REDACTED, data)
            if scrubbed != data:
                path.write_bytes(scrubbed)
                remediated = not CREDENTIAL.search(path.read_bytes())
        findings.append({
            "scope": "GENERATED_LOG_REPORT_OR_ARTIFACT",
            "path": str(path.relative_to(ROOT)),
            "finding": "SECRET_BEARING_URL_OR_LITERAL_REDACTED" if remediated else "SECRET_BEARING_URL_OR_LITERAL",
            "remediation_required": not remediated,
        })
    remediated_inventory = rg_paths(REDACTED_TEXT)
    already_recorded = {str(row["path"]) for row in findings}
    for path in remediated_inventory:
        relative = str(path.relative_to(ROOT) if path.is_absolute() else path)
        if relative not in already_recorded:
            findings.append({
                "scope": "GENERATED_LOG_REPORT_OR_ARTIFACT",
                "path": relative,
                "finding": "SECRET_BEARING_URL_OR_LITERAL_REDACTED",
                "remediation_required": False,
            })
    relative_unreadable = []
    for path in sorted(set(unreadable)):
        try:
            relative_unreadable.append(str(path.relative_to(ROOT)))
        except ValueError:
            relative_unreadable.append(str(path))
    return findings, relative_unreadable


def write_results(output: Path, remediate: bool) -> dict[str, object]:
    output.mkdir(parents=True, exist_ok=True)
    findings, unreadable = scan_and_optionally_redact(remediate)
    if str(RESTRICTED_PATH) not in unreadable and not (ROOT / RESTRICTED_PATH).is_dir():
        unreadable.append(str(RESTRICTED_PATH))
    findings.extend({
        "scope": "UNREADABLE_GENERATED_ARTIFACT_SUBTREE",
        "path": path,
        "finding": "OWNER_LEVEL_SCAN_REQUIRED",
        "remediation_required": True,
    } for path in sorted(set(unreadable)))
    package_candidates = committed_package_candidates()
    if package_candidates:
        findings.extend({
            "scope": "COMMITTED_ACQUISITION_PACKAGE",
            "path": str(path),
            "finding": "CREDENTIAL_LIKE_LITERAL",
            "remediation_required": True,
        } for path in package_candidates)
    else:
        findings.append({
            "scope": "COMMITTED_ACQUISITION_PACKAGE",
            "path": PACKAGE_PATHS[0],
            "finding": "NO_CREDENTIAL_LITERAL_DETECTED",
            "remediation_required": False,
        })
    history_candidates = git_history_candidates()
    for path in history_candidates:
        is_gotrue_documentation = path == GOTRUE_DOCUMENTATION
        findings.append({
            "scope": "GIT_HISTORY_REVIEWED_FALSE_POSITIVE" if is_gotrue_documentation else "GIT_HISTORY",
            "path": str(path),
            "finding": ("NON_ODDS_API_THIRD_PARTY_DOCUMENTATION_CONTEXT" if is_gotrue_documentation
                        else "CREDENTIAL_LIKE_LITERAL"),
            "remediation_required": not is_gotrue_documentation,
        })
    fields = ("scope", "path", "finding", "remediation_required")
    with (output / "credential_containment_findings.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(sorted(findings, key=lambda row: (str(row["scope"]), str(row["path"]))))
    remediated_paths = sum(row["finding"] == "SECRET_BEARING_URL_OR_LITERAL_REDACTED" for row in findings)
    outstanding = [str(row["path"]) for row in findings if row["remediation_required"]]
    summary = {
        "audit": "MLB_ODDSAPI_CREDENTIAL_CONTAINMENT_V1",
        "network_requests": 0,
        "credential_environment_read": False,
        "credential_values_printed": False,
        "generated_paths_remediated": remediated_paths,
        "outstanding_path_count": len(outstanding),
        "outstanding_paths": sorted(outstanding),
        "committed_acquisition_package": ("CREDENTIAL_LIKE_LITERAL_DETECTED" if package_candidates
                                          else "NO_CREDENTIAL_LITERAL_DETECTED"),
        "git_history_odds_api_credential": ("CREDENTIAL_LIKE_LITERAL_DETECTED"
                                            if any(path != GOTRUE_DOCUMENTATION for path in history_candidates)
                                            else "NO_ODDS_API_CREDENTIAL_DETECTED"),
        "git_history_false_positive": ("GOTRUE_THIRD_PARTY_DOCUMENTATION_CONTEXT"
                                       if GOTRUE_DOCUMENTATION in history_candidates else "NONE"),
        "restricted_artifact_status": "OWNER_LEVEL_SCAN_REQUIRED",
    }
    (output / "credential_containment_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--remediate-generated", action="store_true")
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    summary = write_results(output, args.remediate_generated)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
