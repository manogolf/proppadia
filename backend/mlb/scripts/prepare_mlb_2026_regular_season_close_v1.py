#!/usr/bin/env python3
"""Governed, fail-closed regular-season close package builder.

Check-only is the default.  Package creation requires two explicit flags and a
fully passing frozen inventory.  The command never changes a database, model,
prediction, scheduler, publication state, or external service.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from backend.mlb.season_transition.contract_v1 import manifest_lines, validate_close_inventory

AUTHORIZATION_TOKEN = "MLB_2026_REGULAR_SEASON_COMPLETE"


def _write_package(payload: dict, report: dict, output_dir: Path) -> None:
    if output_dir.exists():
        raise RuntimeError(f"CLOSE_OUTPUT_ALREADY_EXISTS:{output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        (temporary / "close_inventory.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (temporary / "validation_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        games_path = temporary / "canonical_regular_season_games.jsonl"
        games_path.write_text(
            "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in payload["canonical_regular_season_games"]),
            encoding="utf-8",
        )
        summary = {
            "contract_name": payload["contract_name"],
            "season": payload["season"],
            "decision": report["decision"],
            "canonical_regular_season_games": len(payload["canonical_regular_season_games"]),
            "unresolved_rows": len(payload.get("outstanding_unresolved_rows") or []),
            "important": "This package freezes evidence; it does not promote, publish, wager, or enable offseason mode.",
        }
        (temporary / "close_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        package_files = [path for path in temporary.iterdir() if path.is_file()]
        lines = manifest_lines(package_files, root=temporary)
        (temporary / "sha256_manifest.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(temporary, output_dir)
    except Exception:
        for path in temporary.glob("*"):
            if path.is_file():
                path.unlink()
        temporary.rmdir()
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--execute-close", action="store_true")
    parser.add_argument("--authorization-token", default="")
    args = parser.parse_args()
    payload = json.loads(args.inventory.read_text(encoding="utf-8"))
    report = validate_close_inventory(payload)
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["passed"]:
        return 1
    if not args.execute_close:
        print("CHECK_ONLY_PASS: no close package written")
        return 0
    if args.authorization_token != AUTHORIZATION_TOKEN:
        raise SystemExit("CLOSE_AUTHORIZATION_TOKEN_INVALID")
    if args.output_dir is None:
        raise SystemExit("--output-dir is required with --execute-close")
    _write_package(payload, report, args.output_dir)
    print(f"CLOSE_PACKAGE_WRITTEN:{args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
