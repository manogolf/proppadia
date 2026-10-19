"""Resolve a reconciliation grade file to its verified restatement, if any.

Reconciliation packages are sealed and never rewritten. Corrected grades are
published beside them as phase restatements. Readers that want the current
grades for a package use this resolver instead of reading the package file.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


RESTATED_GRADE_FILES = frozenset({
    "graded_sog.csv", "graded_points.csv",
    "graded_resilient_sog.csv", "graded_resilient_points.csv",
})
CURRENT_RESTATEMENT_CONTRACTS = frozenset({"NHL_PHASE_RESTATEMENT_V4"})


def _sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def restated_grade_path(package: Path, filename: str) -> Path:
    """Return the restated grade file bound to this package, else the package file.

    A restatement is used only when its lineage names this package and its
    manifest, and its own SHA256SUMS verifies; a tampered restatement raises.
    """
    package = Path(package)
    original = package / filename
    if filename not in RESTATED_GRADE_FILES:
        return original
    root = package.parents[1] / "learning_restatements" / package.parent.name
    if not root.is_dir() or not (package / "SHA256SUMS").is_file():
        return original
    package_manifest = _sha(package / "SHA256SUMS")
    for candidate in sorted(root.glob("restatement=*")):
        lineage_path = candidate / "lineage.json"
        if not lineage_path.is_file():
            continue
        lineage = json.loads(lineage_path.read_text())
        if (lineage.get("contract_version") not in CURRENT_RESTATEMENT_CONTRACTS
                or Path(str(lineage.get("source_package", ""))).resolve() != package.resolve()
                or (lineage.get("source_hashes") or {}).get("SHA256SUMS") != package_manifest
                or not (candidate / filename).is_file()):
            continue
        for line in (candidate / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ", 1)
            if _sha(candidate / name) != digest:
                raise RuntimeError(f"RESTATEMENT_MANIFEST_MISMATCH:{candidate.name}:{name}")
        return candidate / filename
    return original
