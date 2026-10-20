#!/usr/bin/env python3
"""Publish per-game resilient Moneyline/Puck Line grades beside a sealed package.

Packages sealed before per-game resilient grading (for example 2026-10-17)
graded no cross-market cards when no full-slate final run existed, although a
later pregame refresh had captured some games. This re-runs the current
resilient grader read-only against the package's sealed outcomes and writes
only the resilient cross-market grade files to a create-only restatement in
learning_restatements/<date>/. The package is never modified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.postgame_reconcile import core  # noqa: E402

CONTRACT = "NHL_RESILIENT_CROSS_MARKET_RESTATEMENT_V1"
GRADE_FILES = {
    "resilient_moneyline_production_champion": "graded_resilient_moneyline_production_champion.csv",
    "resilient_moneyline_shadow": "graded_resilient_moneyline_shadow.csv",
    "resilient_puck_line": "graded_resilient_puck_line.csv",
}
SOURCE_NAMES = ("SHA256SUMS", "canonical_admitted_slate.csv", "canonical_game_outcomes.csv",
                "canonical_skater_outcomes.csv", "canonical_goalie_outcomes.csv")


def _sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def restate(slate_date: str, operational_root: Path) -> Path:
    packages = sorted((operational_root / "postgame_reconciliation" / slate_date).glob("reconciliation=*"))
    if len(packages) != 1:
        raise RuntimeError(f"RECONCILIATION_PACKAGE_CARDINALITY:{len(packages)}")
    package = packages[0]
    source_hashes = {name: _sha(package / name) for name in SOURCE_NAMES}
    for line in (package / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        if name in SOURCE_NAMES and _sha(package / name) != digest:
            raise RuntimeError(f"PACKAGE_MANIFEST_MISMATCH:{name}")
    grades, index = core._resilient_cross_market_grades(
        slate_date=slate_date, operational_root=operational_root,
        schedule=pd.read_csv(package / "canonical_admitted_slate.csv"),
        games=pd.read_csv(package / "canonical_game_outcomes.csv"),
        skaters=pd.read_csv(package / "canonical_skater_outcomes.csv"),
        goalies=pd.read_csv(package / "canonical_goalie_outcomes.csv"))
    grader_sha = _sha(Path(core.__file__))
    identity = hashlib.sha256(json.dumps({
        "contract": CONTRACT, "source": str(package), "source_hashes": source_hashes,
        "grader_sha256": grader_sha}, sort_keys=True).encode()).hexdigest()
    destination = (operational_root / "postgame_reconciliation" / "learning_restatements"
                   / slate_date / f"restatement={identity[:20]}")
    if destination.is_dir():
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    counts = {}
    for key, filename in GRADE_FILES.items():
        frame = grades.get(key, pd.DataFrame())
        frame.to_csv(destination / filename, index=False)
        counts[filename] = int(len(frame))
    (destination / "lineage.json").write_text(json.dumps({
        "contract_version": CONTRACT, "source_package": str(package),
        "source_hashes": source_hashes, "grader_sha256": grader_sha,
        "graded_rows": counts,
        "coverage": {lane: index["lanes"].get(lane, {}).get("coverage_status")
                     for lane in ("MONEYLINE", "PUCK_LINE")},
        "reason": ("per-game resilient grading of cross-market predictions captured pregame; "
                   "the sealed package graded none because no full-slate final run existed"),
    }, indent=2, sort_keys=True) + "\n")
    files = sorted(path for path in destination.iterdir() if path.is_file())
    (destination / "SHA256SUMS").write_text("".join(f"{_sha(path)}  {path.name}\n" for path in files))
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--operational-root", type=Path, default=ROOT / "artifacts/operational/nhl")
    args = parser.parse_args()
    print(restate(args.date, args.operational_root.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
