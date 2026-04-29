#!/usr/bin/env python3
"""Report-only MLB pre-cron validation for daily/weekly LaunchAgent runs."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


EXPECTED_PROD12_PROPS = [
    "hits",
    "total_bases",
    "strikeouts_batting",
    "earned_runs",
    "doubles",
    "hits_allowed",
    "strikeouts_pitching",
    "walks",
    "hits_runs_rbis",
    "runs_scored",
    "walks_allowed",
    "rbis",
]


def _split_csv(value: str | None) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _has_token(text: str, token: str) -> bool:
    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])", text) is not None


def _extract_json_object(text: str) -> dict:
    decoder = json.JSONDecoder()
    fallback: dict | None = None
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        if {"ok", "prop_types", "failed_props"}.issubset(obj):
            return obj
        fallback = obj
    if fallback is None:
        raise ValueError("no JSON object found in command output")
    return fallback


def _run(cmd: list[str], *, cwd: Path) -> tuple[int, str]:
    proc = subprocess.run(cmd, cwd=str(cwd), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return int(proc.returncode), proc.stdout


def _print_check(name: str, ok: bool, details: Iterable[str] = ()) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}")
    for line in details:
        print(f"  {line}")


def _read_prop_counts(path: Path) -> tuple[dict[str, int], dict[str, int]]:
    counts: dict[str, int] = {}
    outcome_counts: dict[str, int] = {}
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or "prop_type" not in reader.fieldnames:
            return counts, outcome_counts
        has_outcome = "actual_model_pick_outcome" in reader.fieldnames
        for row in reader:
            prop = str(row.get("prop_type") or "").strip()
            if not prop:
                continue
            counts[prop] = counts.get(prop, 0) + 1
            if has_outcome and str(row.get("actual_model_pick_outcome") or "").strip():
                outcome_counts[prop] = outcome_counts.get(prop, 0) + 1
    return counts, outcome_counts


def _artifact_paths(repo: Path, date_label: str) -> list[Path]:
    return [
        repo / "artifacts" / "analysis" / "mlb" / "execution_vs_model" / date_label / "full_slate_by_prop.csv",
        repo / "artifacts" / "analysis" / "mlb" / "execution_vs_model" / date_label / "reconcile_rows.csv",
    ]


def _latest_artifact_date(repo: Path) -> str | None:
    root = repo / "artifacts" / "analysis" / "mlb" / "execution_vs_model"
    if not root.exists():
        return None
    dates = []
    for child in root.iterdir():
        if not child.is_dir() or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", child.name):
            continue
        if any(path.exists() for path in _artifact_paths(repo, child.name)):
            dates.append(child.name)
    return sorted(dates)[-1] if dates else None


def _artifact_sanity(repo: Path, mlb_date: str) -> tuple[bool, bool, list[str]]:
    """Return (ok, warning_only, details)."""
    details: list[str] = []
    candidates = _artifact_paths(repo, mlb_date)
    existing = [path for path in candidates if path.exists()]
    if not existing:
        latest = _latest_artifact_date(repo)
        details.append(f"no same-day execution/reconcile artifact found for MLB_DATE={mlb_date}; warning only")
        if latest:
            details.append(f"latest available artifact date={latest}")
            for path in _artifact_paths(repo, latest):
                if not path.exists():
                    continue
                counts, outcomes = _read_prop_counts(path)
                details.append(f"{path}: props={','.join(sorted(counts)) or '<none>'}")
                if "rbis" in counts:
                    outcome_note = f", rbis_outcomes={outcomes.get('rbis', 0)}" if outcomes else ""
                    details.append(f"{path}: rbis_rows={counts['rbis']}{outcome_note}")
                if "runs_rbis" in counts:
                    details.append(f"{path}: runs_rbis_rows={counts['runs_rbis']} (latest historical artifact)")
        else:
            details.append("no dated execution/reconcile artifacts found")
        return True, True, details

    ok = True
    saw_rbis = False
    saw_runs_rbis = False
    for path in existing:
        counts, outcomes = _read_prop_counts(path)
        prop_list = ",".join(sorted(counts))
        details.append(f"{path}: props={prop_list or '<none>'}")
        if "rbis" in counts:
            saw_rbis = True
            outcome_note = f", rbis_outcomes={outcomes.get('rbis', 0)}" if outcomes else ""
            details.append(f"{path}: rbis_rows={counts['rbis']}{outcome_note}")
        if "runs_rbis" in counts:
            saw_runs_rbis = True
            details.append(f"{path}: runs_rbis_rows={counts['runs_rbis']}")

    if not saw_rbis:
        ok = False
        details.append("rbis absent from same-day artifact(s)")
    if saw_runs_rbis:
        ok = False
        details.append("runs_rbis present in same-day active artifact(s)")
    return ok, False, details


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report-only MLB pre-cron validation.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--mlb-date", required=True)
    parser.add_argument("--prod12-prop-types", required=True)
    parser.add_argument(
        "--expected-prop-types",
        default=",".join(EXPECTED_PROD12_PROPS),
    )
    parser.add_argument(
        "--weekly-wrapper",
        default="/Users/jerrystrain/bin/proppadia_mlb_retrain_weekly.sh",
    )
    args = parser.parse_args(argv)

    repo = Path(args.repo_root).resolve()
    expected = _split_csv(args.expected_prop_types)
    active = _split_csv(args.prod12_prop_types)
    failures: list[str] = []
    warnings: list[str] = []

    print("MLB pre-cron validation")
    print(f"repo_root={repo}")
    print(f"MLB_DATE={args.mlb_date}")
    print()

    prop_ok = active == expected and "rbis" in active and "runs_rbis" not in active
    _print_check(
        "active prop set",
        prop_ok,
        [
            f"MLB_PROD12_PROP_TYPES={','.join(active)}",
            f"expected={','.join(expected)}",
            f"rbis_present={'yes' if 'rbis' in active else 'no'}",
            f"runs_rbis_absent={'yes' if 'runs_rbis' not in active else 'no'}",
        ],
    )
    if not prop_ok:
        failures.append("active prop set mismatch")

    rc, model_output = _run(["make", "mlb-model-artifact-validate-prod12"], cwd=repo)
    model_ok = rc == 0
    model_details = [f"exit_code={rc}"]
    try:
        payload = _extract_json_object(model_output)
        failed_props = payload.get("failed_props") or []
        model_ok = model_ok and not failed_props and bool(payload.get("ok"))
        model_details.extend(
            [
                f"status={payload.get('status')}",
                f"failed_props={','.join(map(str, failed_props)) if failed_props else '<none>'}",
            ]
        )
    except Exception as exc:
        model_details.append(f"could_not_parse_validator_json={exc}")
    index_path = repo / "models_out" / "latest" / "MODEL_INDEX.json"
    if index_path.exists():
        try:
            index = json.loads(index_path.read_text())
            model_details.append(f"MODEL_INDEX={','.join(sorted(map(str, index.keys())))}")
        except Exception as exc:
            model_details.append(f"MODEL_INDEX_parse_error={exc}")
            model_ok = False
    else:
        model_details.append(f"missing {index_path}")
        model_ok = False
    _print_check("model artifact validation", model_ok, model_details)
    if not model_ok:
        failures.append("model artifact validation failed")

    wrapper_path = Path(args.weekly_wrapper)
    wrapper_details = [f"path={wrapper_path}"]
    wrapper_ok = wrapper_path.exists()
    wrapper_text = ""
    if wrapper_ok:
        wrapper_text = wrapper_path.read_text()
        overrides = re.findall(r'MLB_PROD12_CANDIDATE_REQUIRED_PROPS="([^"]*)"', wrapper_text)
        stale_override = False
        for override in overrides:
            override_props = _split_csv(override)
            if "rbis" not in override_props or "strikeouts_batting" not in override_props or "runs_rbis" in override_props:
                stale_override = True
        wrapper_ok = (
            not stale_override
            and _has_token(wrapper_text, "strikeouts_batting")
            and _has_token(wrapper_text, "rbis")
            and not _has_token(wrapper_text, "runs_rbis")
        )
        wrapper_details.extend(
            [
                f"required_prop_overrides={len(overrides)}",
                f"strikeouts_batting_present={'yes' if _has_token(wrapper_text, 'strikeouts_batting') else 'no'}",
                f"rbis_present={'yes' if _has_token(wrapper_text, 'rbis') else 'no'}",
                f"runs_rbis_absent={'yes' if not _has_token(wrapper_text, 'runs_rbis') else 'no'}",
                f"stale_override={'yes' if stale_override else 'no'}",
            ]
        )
    else:
        wrapper_details.append("wrapper_missing")
    _print_check("weekly wrapper sanity", wrapper_ok, wrapper_details)
    if not wrapper_ok:
        failures.append("weekly wrapper sanity failed")

    dry_cmd = [
        "make",
        "-n",
        "mlb-prod12-phase2-weekly-cycle",
        f"MLB_DATE={args.mlb_date}",
        f"MLB_PROD12_CANDIDATE_REQUIRED_PROPS={','.join(expected)}",
    ]
    rc, dry_output = _run(dry_cmd, cwd=repo)
    dry_ok = (
        rc == 0
        and _has_token(dry_output, "rbis")
        and _has_token(dry_output, "strikeouts_batting")
        and not _has_token(dry_output, "runs_rbis")
    )
    _print_check(
        "weekly dry-run command",
        dry_ok,
        [
            f"exit_code={rc}",
            f"rbis_present={'yes' if _has_token(dry_output, 'rbis') else 'no'}",
            f"strikeouts_batting_present={'yes' if _has_token(dry_output, 'strikeouts_batting') else 'no'}",
            f"runs_rbis_absent={'yes' if not _has_token(dry_output, 'runs_rbis') else 'no'}",
        ],
    )
    if not dry_ok:
        failures.append("weekly dry-run command failed")

    artifact_ok, artifact_warning_only, artifact_details = _artifact_sanity(repo, args.mlb_date)
    _print_check("latest artifact sanity", artifact_ok, artifact_details)
    if not artifact_ok:
        failures.append("latest artifact sanity failed")
    if artifact_warning_only:
        warnings.append("same-day artifact missing")

    print()
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    if failures:
        print("Failed checks:")
        for failure in failures:
            print(f"  - {failure}")
        print("PRE-CRON CHECK: NO-GO")
        return 1

    print("PRE-CRON CHECK: GO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
