"""Helpers for deriving sparse-data warnings from check results."""

from __future__ import annotations

from typing import Iterable, Literal, Sequence, Tuple


WarnRule = Tuple[str, Literal["contains", "missing"], str, str]


def find_sparse_warnings(checks: Iterable, rules: Sequence[WarnRule]) -> list[str]:
    by_name = {getattr(c, "name", ""): c for c in checks}
    warns: list[str] = []
    for check_name, mode, fragment, message in rules:
        check = by_name.get(check_name)
        if check is None:
            continue
        detail = str(getattr(check, "detail", ""))
        should_warn = (fragment in detail) if mode == "contains" else (fragment not in detail)
        if should_warn:
            warns.append(message)
    return warns
