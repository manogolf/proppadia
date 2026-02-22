"""Shared console output helpers for smoke and post-deploy checks."""

from __future__ import annotations

from typing import Iterable, Tuple


def print_check_rows(results: Iterable, *, name_width: int = 24, path_width: int = 30, detail_limit: int | None = None) -> Tuple[int, int]:
    total = 0
    failed = 0
    for r in results:
        total += 1
        state = "PASS" if getattr(r, "ok", False) else "FAIL"
        if state == "FAIL":
            failed += 1
        detail = str(getattr(r, "detail", ""))
        if detail_limit is not None:
            detail = detail[:detail_limit]
        print(
            f"{state} {getattr(r, 'name', ''):{name_width}s} "
            f"{getattr(r, 'method', ''):4s} {getattr(r, 'path', ''):{path_width}s} "
            f"status={getattr(r, 'status', '')} detail={detail}"
        )
    return total, failed


def print_warn_rows(warns: Iterable[str], *, label: str = "data-richness") -> None:
    for w in warns:
        print(f"WARN {label:24s} {w}")


def print_summary(*, passed: int, total: int) -> None:
    print(f"\nSummary: {passed}/{total} passed")
