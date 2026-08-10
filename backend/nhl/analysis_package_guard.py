"""Fail-closed helpers for governed NHL analysis packages."""
from __future__ import annotations

import hashlib
from pathlib import Path

EXISTS_ABORT = "GOVERNED_PACKAGE_EXISTS_ABORT"
PARENT_ABORT = "PARENT_MANIFEST_MISMATCH_ABORT"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_manifest(package: Path, expected_manifest_sha256: str | None = None) -> str:
    manifest = package / "SHA256SUMS"
    if not manifest.is_file():
        raise RuntimeError(PARENT_ABORT if expected_manifest_sha256 else "PACKAGE_MANIFEST_MISSING")
    actual = sha256_file(manifest)
    if expected_manifest_sha256 and actual != expected_manifest_sha256:
        raise RuntimeError(PARENT_ABORT)
    for entry in manifest.read_text().splitlines():
        expected, relative = entry.split("  ", 1)
        target = package / relative
        if not target.is_file() or sha256_file(target) != expected:
            raise RuntimeError(PARENT_ABORT if expected_manifest_sha256 else "PACKAGE_MANIFEST_MISMATCH")
    return actual


def verify_parents(parents: list[tuple[Path, str]]) -> None:
    """Verify every parent before a child output path is created."""
    for package, expected in parents:
        verify_manifest(package, expected)


def require_create_only(target: Path) -> None:
    """Reject any pre-existing path, including stale partial directories."""
    if target.exists():
        raise RuntimeError(EXISTS_ABORT)


def regeneration_path(canonical: Path, regeneration_id: str) -> Path:
    """Return a visibly separate path; never alias the canonical directory."""
    if not regeneration_id or any(x in regeneration_id for x in ("/", "\\", "..")):
        raise ValueError("INVALID_REGENERATION_ID")
    return canonical.parent.parent / f"{canonical.parent.name}_regenerated" / canonical.name / regeneration_id


def begin_package(target: Path) -> Path:
    """Create a non-complete staging directory after a create-only check."""
    require_create_only(target)
    staging = target.with_name(f".{target.name}.incomplete")
    require_create_only(staging)
    staging.mkdir(parents=True)
    return staging


def finalize_package(staging: Path, target: Path) -> None:
    """Publish only a manifest-complete package via same-filesystem rename."""
    verify_manifest(staging)
    require_create_only(target)
    staging.rename(target)
