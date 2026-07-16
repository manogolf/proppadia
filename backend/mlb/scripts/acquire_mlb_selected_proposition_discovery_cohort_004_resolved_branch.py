#!/usr/bin/env python3
"""Execute only the frozen COHORT_004 seven-side resolved acquisition branch.

This wrapper shares the governed implementation with the full COHORT_004
low-sample policy utility and writes only the resolved-acquisition subpackage.
It does not reconstruct, remediate, mutate qualification, score models, write a
database, upload, or change production behavior.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.mlb.scripts.audit_and_execute_mlb_selected_proposition_cohort_004_low_sample_start_discovery import (
    BRANCH_DIR,
    DEFAULT_OUT_DIR,
    DISCOVERY_DIR,
    EXPECTED_BRANCH_SHA,
    EXPECTED_DISCOVERY_SHA,
    EXPECTED_PARENT_SHA,
    PARENT_DIR,
    RUN_DATE,
    compute_package_manifest,
    parse_validation,
    run_resolved_acquisition,
    verify_sha_manifest,
    write_csv,
    write_static_guards,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--mode", choices=["execute", "replay"], default="execute")
    args = parser.parse_args()
    out_dir = Path(args.output_dir)
    dependency_rows = [
        verify_sha_manifest(BRANCH_DIR, EXPECTED_BRANCH_SHA, "cohort_004_branch_governance"),
        verify_sha_manifest(DISCOVERY_DIR, EXPECTED_DISCOVERY_SHA, "cohort_004_discovery"),
        verify_sha_manifest(PARENT_DIR, EXPECTED_PARENT_SHA, "cohort_003_parent_state"),
    ]
    write_csv(out_dir / "resolved_acquisition_branch" / f"resolved_branch_dependency_sha_audit_{RUN_DATE}.csv", dependency_rows)
    if any(r["status"] != "PASS" for r in dependency_rows):
        raise SystemExit("authoritative dependency SHA verification failed")
    resolved = run_resolved_acquisition(out_dir, args.mode)
    write_static_guards(out_dir)
    write_csv(
        out_dir / "resolved_acquisition_branch" / f"resolved_branch_wrapper_validation_{RUN_DATE}.csv",
        [
            {
                "check": "resolved_branch_only",
                "status": "PASS",
                "detail": "wrapper invokes only run_resolved_acquisition",
            },
            {
                "check": "no_reconstruction_or_remediation",
                "status": "PASS",
                "detail": "artifact-only wrapper",
            },
        ],
    )
    parse_validation(out_dir)
    manifest, package_hash = compute_package_manifest(out_dir)
    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "package_sha256_manifest": str(manifest),
                "package_sha256_manifest_hash": package_hash,
                "resolved": resolved,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
