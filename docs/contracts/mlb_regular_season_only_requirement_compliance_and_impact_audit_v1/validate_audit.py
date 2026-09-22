#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free, read-only validator for the regular-season evidence audit."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


PACKAGE = Path(__file__).resolve().parent
REPO = PACKAGE.parents[2]
EXPECTED_HEAD = "0ef19ac4d2cf5d141853605abaa246a77397f652"
EXPECTED_VENV_TARGET = "/Users/jerrystrain/Projects/.proppadia-py311-scipy1152-macos12-arm64-candidate"
EXPECTED_VENV = {"size": 78, "mtime": 1790025129, "inode": 137012602}
EXPECTED_LOCK = {"size": 0, "mtime": 1789577504, "inode": 135377680}
LOCK = REPO / "backend/mlb/data/research/dh_forward_validation/v1/rolling_forward_evidence_status_v1.json.publish.lock"

checks: list[str] = []


def check(condition: bool, name: str) -> None:
    if not condition:
        raise AssertionError(name)
    checks.append(name)


def run_git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    ).stdout.strip()


def rows(name: str) -> list[dict[str, str]]:
    with (PACKAGE / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stat_tuple(path: Path, follow_symlinks: bool) -> dict[str, int]:
    value = path.stat() if follow_symlinks else path.lstat()
    return {"size": value.st_size, "mtime": int(value.st_mtime), "inode": value.st_ino}


def main() -> int:
    canonical = REPO / ".venv/bin/python"
    check(Path(sys.executable).resolve() == canonical.resolve(), "canonical interpreter")
    check(run_git("rev-parse", "HEAD") == EXPECTED_HEAD, "expected HEAD")
    check(run_git("merge-base", "--is-ancestor", "7f22c7623ff5b94462749f51e29288c26e473e03", "HEAD") == "", "earliest control is ancestor")

    status = run_git("status", "--porcelain=v1", "--untracked-files=all").splitlines()
    check(all(line.startswith("?? ") for line in status), "no tracked or staged changes")
    allowed_prefixes = ("?? .venv", "?? backend/mlb/data/research/dh_forward_validation/v1/rolling_forward_evidence_status_v1.json.publish.lock", f"?? {PACKAGE.relative_to(REPO)}/")
    check(all(line.startswith(allowed_prefixes) for line in status), "only allowed untracked paths")
    check((REPO / ".venv").is_symlink() and os.readlink(REPO / ".venv") == EXPECTED_VENV_TARGET, "venv symlink target")
    check(stat_tuple(REPO / ".venv", False) == EXPECTED_VENV, "venv symlink metadata")
    check(LOCK.is_file() and not LOCK.is_symlink() and stat_tuple(LOCK, True) == EXPECTED_LOCK, "publish lock metadata")
    check(sha256(LOCK) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "publish lock content")

    matrix = rows("consumer_compliance_matrix.csv")
    check(len(matrix) == 24, "24 mapped consumers")
    check([int(row["ordinal"]) for row in matrix] == list(range(1, 25)), "consumer ordinals")
    allowed_behaviors = {"AUTHORITATIVE_POSITIVE_MEMBERSHIP", "RAW_TYPE_ENFORCED", "PHASE_PRESENT_BUT_NOT_ENFORCED", "DATE_INFERRED", "MISSING_DEFAULTS_TO_REGULAR", "NO_PHASE_CONTROL", "NOT_APPLICABLE", "UNPROVABLE"}
    check({row["pre_correction_classification"] for row in matrix} <= allowed_behaviors, "consumer classifications")
    buckets = {name: sum(row["compliance_bucket"] == name for row in matrix) for name in ("COMPLIANT", "VIOLATING", "UNPROVABLE", "NOT_APPLICABLE")}
    check(buckets == {"COMPLIANT": 3, "VIOLATING": 19, "UNPROVABLE": 0, "NOT_APPLICABLE": 2}, "consumer bucket counts")
    check(all(len(row["introducing_or_control_commit"]) == 40 for row in matrix), "consumer commit identities")
    sidecar = rows("../mlb_2026_game_phase_sidecar_design_v1/consumer_cutover_map.csv")
    check(len(sidecar) == 24, "sidecar universe count")

    timeline = rows("violation_introduction_timeline.csv")
    required_commits = {"7f22c7623ff5b94462749f51e29288c26e473e03", "d5a5a4f466123c658aa18b1b7cf3b43283c1f5be", "5b24d71143fe120c41f9d1b40bc60bbb5cfbf8de", "01c610c4ed88c2a7fa814cce3dae6d3e25a5aa46", "0ae9d87f4e7cd7a758e87862dcca6d579f1bcd13", EXPECTED_HEAD}
    check(required_commits <= {row["commit"] for row in timeline}, "required timeline commits")
    fallback_dates = [row["date"] for row in timeline if row["violation_kind"] == "MISSING_DEFAULTS_TO_REGULAR"]
    check(fallback_dates and min(fallback_dates) == "2026-03-29", "earliest missing-to-R fallback")
    check(all(None not in row for row in timeline), "timeline rectangular CSV")

    affected = rows("affected_game_ledger.csv")
    check(affected == [], "zero realized affected games")

    provenance = json.loads((PACKAGE / "requirement_provenance.json").read_text())
    earliest = provenance["earliest_authoritative_requirement"]
    check(earliest["commit"] == "7f22c7623ff5b94462749f51e29288c26e473e03", "earliest requirement commit")
    check(earliest["implementation_at_introduction"]["positive_membership"] == "gameType == R", "earliest positive membership")
    check(earliest["implementation_at_introduction"]["missing_type_behavior"] == "excluded", "earliest missing behavior")
    check(not provenance["preservation_assessment"]["post_control_corrections_are_prior_compliance_evidence"], "post-control correction posture")

    lane = json.loads((PACKAGE / "lane_level_reconciliation.json").read_text())
    authority = lane["authority"]
    check(authority["distinct_game_pks"] == 2919 and authority["source_type_counts"] == {"E": 38, "R": 2430, "S": 451}, "canonical authority counts")
    check(not authority["date_inference_used"] and authority["missing"] == authority["unknown"] == authority["conflicting"] == 0, "canonical authority fail-closed state")
    populations = lane["fully_reconciled_populations"]
    check(len(populations) == 21, "fully reconciled component count")
    check(all(p["total_rows"] == p["authoritative_regular_rows"] + p["preseason_rows"] + p["postseason_rows"] + p["special_or_unknown_rows"] for p in populations), "native row phase totals")
    check(all(p["rows_lacking_exact_game_pk"] == 0 and p["rows_lacking_authoritative_type"] == 0 for p in populations), "complete exact authority joins")
    check(all(p["authoritative_coverage_pct"] == 100.0 for p in populations), "100 percent bounded coverage")
    check(all(not p["affected_game_pks"] for p in populations), "no affected gamePks in bounded populations")
    hits = next(p for p in populations if p["lane"] == "Hits candidate frozen evaluation")
    check((hits["total_rows"], hits["distinct_game_pks"], hits["certification_status"]) == (7564, 651, "RESULT_SAME_BUT_CONTROL_VIOLATED"), "preserved Hits finding")
    check(lane["aggregate_findings"] == {"fully_reconciled_component_count": 21, "components_with_realized_nonregular_rows": 0, "affected_game_pk_count": 0, "metrics_proven_to_require_restatement": 0, "repository_wide_proof": False}, "aggregate lane findings")

    impacts = rows("metric_and_certification_impact.csv")
    check(any(row["certification_status"] == "UNPROVABLE_FROM_RETAINED_EVIDENCE" for row in impacts), "unprovable impact surfaces retained")
    check(all(row["certification_status"] != "AFFECTED_REQUIRES_RESTATEMENT" for row in impacts), "no proven restatement")
    hits_impact = next(row for row in impacts if row["lane_or_result"] == "Hits candidate evaluation")
    check("57.218403" in hits_impact["prediction_quality"] and "0.5404937507" in hits_impact["prediction_quality"], "Hits metrics preserved")

    defects = rows("realized_versus_latent_defect_register.csv")
    check(len(defects) == 19 and {row["defect_id"] for row in defects} == {f"D{i:02d}" for i in range(1, 20)}, "19 defect records")
    check(any(row["realized_or_latent"] == "LATENT_POSTSEASON_RISK" for row in defects), "latent postseason risk separated")
    check(any(row["defect_class"] == "ABANDONED_PROPOSAL" for row in defects), "abandoned proposal separated")

    coverage = rows("audit_coverage_and_missing_evidence_register.csv")
    check(len(coverage) == 17, "coverage register surfaces")
    check(any(row["coverage_status"].startswith("INCOMPLETE") for row in coverage), "missing evidence disclosed")

    sources = rows("source_evidence_manifest.csv")
    check(len(sources) == 25, "source evidence count")
    check(all((REPO / row["path"]).stat().st_size == int(row["bytes"]) and sha256(REPO / row["path"]) == row["sha256"] for row in sources), "source evidence hashes")

    report = json.loads((PACKAGE / "validation_report.json").read_text())
    check(report["status"] == "PASS" and report["classification"] == "REGULAR_SEASON_EVIDENCE_PARTIALLY_UNPROVABLE", "validation report disposition")
    readme = (PACKAGE / "README.md").read_text()
    check("REGULAR_SEASON_EVIDENCE_PARTIALLY_UNPROVABLE" in readme and "0ae9d87f" in readme and "0ef19ac4" in readme, "README bounded classification")
    check(run_git("diff", "--check") == "", "tracked diff check")

    manifest_lines = [line.split("  ", 1) for line in (PACKAGE / "sha256_manifest.txt").read_text().splitlines() if line]
    expected_files = sorted(path.name for path in PACKAGE.iterdir() if path.is_file() and path.name != "sha256_manifest.txt")
    check(sorted(name for _, name in manifest_lines) == expected_files, "package manifest coverage")
    check(all(sha256(PACKAGE / name) == digest for digest, name in manifest_lines), "package manifest hashes")

    output = {
        "status": "PASS",
        "checks_passed": len(checks),
        "checks_failed": 0,
        "checks": checks,
        "classification": "REGULAR_SEASON_EVIDENCE_PARTIALLY_UNPROVABLE",
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "checks_passed": len(checks), "checks_failed": 1, "failure": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
