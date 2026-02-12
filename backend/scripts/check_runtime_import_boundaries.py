#!/usr/bin/env python3
"""
Fail when active runtime packages import archive/legacy modules.

Active runtime roots:
- backend/app
- backend/domains

Forbidden import markers:
- archive
- legacy
- _legacy
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List, Sequence, Tuple


RUNTIME_ROOTS: Sequence[Path] = (Path("backend/app"), Path("backend/domains"))
FORBIDDEN_MARKERS = {"archive", "legacy", "_legacy"}


def _contains_forbidden_marker(module_path: str) -> bool:
    parts = [part.strip() for part in module_path.split(".") if part.strip()]
    return any(part in FORBIDDEN_MARKERS for part in parts)


def _candidate_module_paths(
    node: ast.AST, package_parts: Sequence[str]
) -> Sequence[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]

    if isinstance(node, ast.ImportFrom):
        # from x.y import z  -> x.y.z and x.y
        module = node.module or ""
        if node.level:
            base = list(package_parts[: max(0, len(package_parts) - node.level)])
            if module:
                base.extend(module.split("."))
            module = ".".join(base)
        out = [module] if module else []
        for alias in node.names:
            if module:
                out.append(f"{module}.{alias.name}")
            else:
                out.append(alias.name)
        return out

    return []


def _package_parts_for_file(path: Path) -> List[str]:
    parts = list(path.with_suffix("").parts)
    return parts


def _scan_file(path: Path) -> List[Tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package_parts = _package_parts_for_file(path)
    violations: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        for module_path in _candidate_module_paths(node, package_parts):
            if module_path and _contains_forbidden_marker(module_path):
                violations.append((getattr(node, "lineno", 1), module_path))
    return violations


def _iter_runtime_py_files() -> Sequence[Path]:
    files: List[Path] = []
    for root in RUNTIME_ROOTS:
        if not root.exists():
            continue
        files.extend(path for path in root.rglob("*.py") if path.is_file())
    return sorted(files)


def main() -> int:
    violations: List[Tuple[str, int, str]] = []
    for path in _iter_runtime_py_files():
        for lineno, module_path in _scan_file(path):
            violations.append((str(path), lineno, module_path))

    if violations:
        print("FAIL runtime import boundary check:")
        print("Active runtime modules must not import archive/legacy paths.")
        for path, lineno, module_path in violations:
            print(f"- {path}:{lineno} imports {module_path}")
        return 1

    print("PASS runtime import boundary check")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
