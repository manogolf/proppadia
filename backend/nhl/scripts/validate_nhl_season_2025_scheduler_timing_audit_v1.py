#!/usr/bin/env python3
"""Deterministically validate the NHL season-2025 scheduler timing audit."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/nhl_season_2025_scheduler_timing_audit_v1/2026-09-18"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(name: str) -> list[dict]:
    with (OUT / name).open(newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    summary = json.loads((OUT / "summary.json").read_text())
    invocations = rows("invocation_ledger.csv")
    odds = rows("odds_capture_lead_time_ledger.csv")
    inventory = rows("scheduler_inventory.csv")
    assert summary["github"]["runs"] == 186
    assert summary["github"]["scheduled"] + summary["github"]["manual_dispatch"] == 186
    assert summary["automator"]["rows"] == 270
    assert len(invocations) == 271
    assert sum(int(row["population_count"]) for row in invocations) == 456
    assert summary["automator"]["window_counts"]["AUTOMATOR_0545"] == 106
    assert summary["automator"]["window_counts"]["AUTOMATOR_1430"] == 96
    assert summary["automator"]["window_counts"]["OUTSIDE_DOMINANT_WINDOWS"] == 68
    assert summary["automator"]["successes"] == 235
    assert summary["automator"]["failures_or_incomplete"] == 35
    assert summary["odds"]["historical_bundle_rows"] == 153
    assert summary["odds"]["contemporaneous_capture_rows"] == 42
    assert len(odds) == 195
    assert summary["odds"]["mainline_historical_objects"] == 0
    assert summary["odds"]["puck_line_historical_objects"] == 0
    assert any(row["mechanism"] == "MACOS_AUTOMATOR_CALENDAR" for row in inventory)
    assert summary["current"]["morning"]["template_matches_installed"] is True
    assert summary["current"]["conditional_shadow"]["template_matches_installed"] is True
    expected = {}
    for line in (OUT / "SHA256SUMS").read_text().splitlines():
        if line.strip():
            value, name = line.split("  ", 1)
            expected[name] = value
    actual_names = sorted(path.name for path in OUT.iterdir() if path.is_file() and path.name != "SHA256SUMS")
    assert sorted(expected) == actual_names
    for name, value in expected.items():
        assert digest(OUT / name) == value, name
    print(json.dumps({"status":"PASS","invocation_rows":len(invocations),"represented_invocations":456,
                      "odds_rows":len(odds),"manifest_files":len(expected)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
