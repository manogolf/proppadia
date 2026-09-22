#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free validation for the local-evidence correction contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[2]
CANONICAL = ROOT / "docs/contracts/mlb_2026_authoritative_regular_season_close_inventory_v1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def main() -> int:
    failures: list[str] = []
    manifest_entries = 0
    for line in (PACKAGE / "sha256_manifest.txt").read_text(
        encoding="utf-8"
    ).splitlines():
        if not line.strip():
            continue
        manifest_entries += 1
        expected, relative = line.split("  ", 1)
        path = ROOT / relative
        if not path.is_file() or sha256(path) != expected:
            failures.append(f"MANIFEST_MISMATCH:{relative}")

    before_after = json.loads(
        (PACKAGE / "before_after_reconciliation.json").read_text(encoding="utf-8")
    )
    binding = json.loads(
        (PACKAGE / "exact_88_blocker_binding.json").read_text(encoding="utf-8")
    )
    correction_validation = json.loads(
        (PACKAGE / "validation_report.json").read_text(encoding="utf-8")
    )
    inventory_manifest = json.loads(
        (CANONICAL / "close_inventory_manifest.json").read_text(encoding="utf-8")
    )
    inventory_validation = json.loads(
        (CANONICAL / "validation_report.json").read_text(encoding="utf-8")
    )
    scheduled = json.loads(
        (CANONICAL / "scheduled_not_final_game_pks.json").read_text(
            encoding="utf-8"
        )
    )
    current = binding["current_date_nonterminal_game_pks"]
    future = binding["future_scheduled_game_pks"]
    expected_counts = {
        "AUTHORITATIVELY_CANCELLED": 0,
        "FINAL": 2316,
        "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED": 25,
        "SCHEDULED_NOT_FINAL": 88,
        "SUSPENDED_RESUMED_IDENTITY_RESOLVED": 1,
        "UNRESOLVED_IDENTITY_OR_STATUS": 0,
    }
    checks = {
        "canonical_population_unchanged": before_after["canonical_population_change"]
        == 0,
        "exact_348_recoveries": before_after["local_terminal_recoveries"] == 348,
        "exact_corrected_counts": inventory_manifest["disposition_counts"]
        == expected_counts,
        "exact_88_ledger": len(scheduled) == 88
        and scheduled == sorted(current + future),
        "exact_16_current": len(current) == 16,
        "exact_72_future": len(future) == 72,
        "current_population_hash": canonical_sha256(current)
        == binding["current_date_population_sha256"],
        "future_population_hash": canonical_sha256(future)
        == binding["future_population_sha256"],
        "remaining_population_hash": canonical_sha256(scheduled)
        == binding["remaining_population_sha256"],
        "scheduled_ledger_hash": sha256(
            CANONICAL / "scheduled_not_final_game_pks.json"
        )
        == binding["scheduled_not_final_ledger_sha256"],
        "inventory_validation_passed": inventory_validation["validation_passed"]
        and not inventory_validation["failed_checks"],
        "all_18_tests_passed": inventory_validation["tests"]["executed"] == 18
        and inventory_validation["tests"]["passed"] == 18
        and inventory_validation["tests"]["failed"] == 0
        and inventory_validation["tests"]["skipped"] == 0,
        "close_still_blocked": inventory_validation["close_readiness"]
        == "REGULAR_SEASON_CLOSE_BLOCKED",
        "close_checker_exit_nonzero": correction_validation["close_checker"][
            "exit_code"
        ]
        == 1,
        "no_close_package": not correction_validation["close_checker"][
            "close_package_created"
        ],
        "correction_validation_passed": correction_validation["validation_passed"],
    }
    failures.extend(name for name, passed in checks.items() if not passed)
    report = {
        "checks": {
            name: "PASS" if passed else "FAIL" for name, passed in checks.items()
        },
        "failed_checks": failures,
        "manifest_entries": manifest_entries,
        "passed": not failures,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
