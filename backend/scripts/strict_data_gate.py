"""Shared strict-data gating for post-deploy checks."""

from __future__ import annotations

from typing import Sequence


def enforce_strict_data_gate(*, require_data: bool, allow_sparse: bool, warns: Sequence[str]) -> int:
    """Return process exit status for strict-data gating."""
    if not require_data or not warns:
        return 0
    if allow_sparse:
        print("PASS strict-data gate         allow-sparse enabled; warnings tolerated")
        return 0
    print("FAIL strict-data gate         sparse probe data; run without --require-data or use --allow-sparse")
    return 1
