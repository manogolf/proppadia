#!/usr/bin/env python3
"""Offline/static validator for the exact-game shadow writer contract."""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

from backend.mlb.prospective_exact_game_feature_shadow_writer_v1 import verify_package


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "backend/mlb/prospective_exact_game_feature_shadow_writer_v1.py"
CLI = ROOT / "backend/mlb/scripts/run_mlb_exact_game_feature_shadow_v1.py"
HOOK = ROOT / "bin/mlb_exact_game_feature_shadow_daily_hook.sh"
CONTRACT = ROOT / "backend/mlb/contracts/mlb_2026_prospective_exact_game_feature_shadow_writer_v1.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path)
    args = parser.parse_args()
    checks: list[tuple[str, bool]] = []
    contract = json.loads(CONTRACT.read_text())
    source = MODULE.read_text()
    cli = CLI.read_text()
    hook = HOOK.read_text()
    ast.parse(source)
    ast.parse(cli)
    checks.extend(
        [
            ("shadow_only_contract", contract["production"]["shadow_only"] is True and contract["production"]["consumers"] == []),
            ("no_database_backend", contract["storage"]["database_backend"] is False),
            ("no_latest_pointer", contract["storage"]["latest_pointer"] is False),
            ("repeatable_read_only", "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in source),
            ("no_transaction_id_request", "txid_" not in source),
            ("no_legacy_daily_source", "player_derived_stats" not in source),
            ("no_max_game_id", "MAX(" not in source.upper()),
            ("no_network_client", all(token not in source and token not in cli for token in ("requests.", "urllib.request", "httpx."))),
            ("hook_visible_nonblocking_contract", "production consumers remain isolated" in hook),
            ("hook_no_bvp", "bvp" not in hook.lower()),
        ]
    )
    if args.package:
        validation = verify_package(args.package)
        checks.append(("package_manifest", validation["status"] == "PASS"))
    failed = [name for name, passed in checks if not passed]
    print(json.dumps({"contract": contract["contract"], "status": "PASS" if not failed else "FAIL", "checks": [{"name": name, "status": "PASS" if passed else "FAIL"} for name, passed in checks]}, sort_keys=True))
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
