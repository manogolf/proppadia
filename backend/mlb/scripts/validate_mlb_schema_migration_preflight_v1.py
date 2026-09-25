#!/usr/bin/env python3
"""Offline, fail-closed validator for a captured MLB migration preflight."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from backend.mlb.migration_ledger_bootstrap_v1 import BOOTSTRAP_ID, BOOTSTRAP_SQL, EXACT_ID, LEDGER, build_plan, file_sha256

EXPECTED_DEFINITION_SHA256 = file_sha256(BOOTSTRAP_SQL)


def validate(snapshot: dict[str, Any]) -> dict[str, Any]:
    mode = snapshot.get("mode")
    internal = bool(snapshot.get("supabase_internal_ledger_present"))
    exists = bool(snapshot.get("project_ledger_exists"))
    failures: list[str] = []
    if internal:
        failures.append("SUPABASE_INTERNAL_LEDGER_REJECTED")
    if mode == "ordinary" and not exists:
        failures.append("MIGRATION_GOVERNANCE_UNPROVEN")
    if mode not in ("ordinary", "bootstrap"):
        failures.append("UNKNOWN_MIGRATION_MODE")
    if mode == "bootstrap" and exists:
        failures.append("BOOTSTRAP_LEDGER_ALREADY_EXISTS")
    if exists:
        if snapshot.get("ledger_relation") != LEDGER:
            failures.append("PROJECT_LEDGER_IDENTITY_MISMATCH")
        if snapshot.get("ledger_owner") != "postgres":
            failures.append("PROJECT_LEDGER_OWNER_MISMATCH")
        if snapshot.get("ledger_definition_sha256") != EXPECTED_DEFINITION_SHA256:
            failures.append("PROJECT_LEDGER_DEFINITION_MISMATCH")
        if snapshot.get("can_select") is not True or snapshot.get("can_insert") is not True:
            failures.append("PROJECT_LEDGER_REQUIRED_GRANTS_MISSING")
    if mode == "bootstrap" and exists is False:
        if snapshot.get("expected_absent_objects_verified") is not True:
            failures.append("BOOTSTRAP_PRESTATE_UNPROVEN")
        if snapshot.get("target_identity") in (None, ""):
            failures.append("TARGET_IDENTITY_UNPROVEN")
    applied = snapshot.get("migration_ids", [])
    expected_plan = build_plan()
    expected_ids = {BOOTSTRAP_ID, EXACT_ID}
    expected_hashes = {expected_plan.bootstrap.migration_sha256, expected_plan.exact.migration_sha256}
    if expected_ids.intersection(applied):
        failures.append("INITIAL_MIGRATION_ID_NOT_ABSENT")
    if expected_hashes.intersection(set(snapshot.get("migration_sha256s", []))):
        failures.append("INITIAL_MIGRATION_CHECKSUM_NOT_ABSENT")
    return {
        "validator": "MLB_MIGRATION_GOVERNANCE_PREFLIGHT_V1",
        "classification": "READY" if not failures else "BLOCKED_" + failures[0],
        "readiness": "READY" if not failures else "BLOCKED",
        "failures": failures,
        "governance_proven": not failures,
        "mode": mode,
        "ledger": LEDGER,
    }


def false_ready_regression_fixture() -> dict[str, Any]:
    return {
        "mode": "ordinary", "supabase_internal_ledger_present": False,
        "project_ledger_exists": False, "ledger_relation": None,
        "ledger_owner": None, "ledger_definition_sha256": None,
        "can_select": False, "can_insert": False,
        "expected_absent_objects_verified": False,
        "target_identity": "fixture-target", "migration_ids": [], "migration_sha256s": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-json", type=Path, required=True, help="Captured preflight JSON; never opens a database")
    args = parser.parse_args()
    result = validate(json.loads(args.input_json.read_text()))
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result["readiness"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
