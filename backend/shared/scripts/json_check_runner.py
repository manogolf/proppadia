#!/usr/bin/env python3
"""Helpers for running script checks that emit JSON on stdout."""

from __future__ import annotations

import json
from contextlib import redirect_stdout
from io import StringIO
from json import JSONDecoder
from typing import Any, Callable, Sequence


def parse_json_payload(output: str) -> dict[str, Any]:
    """Parse JSON payload from stdout text with optional non-JSON prefixes."""
    raw = (output or "").strip()
    if not raw:
        raise ValueError("empty output; expected JSON payload")

    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        pass

    decoder = JSONDecoder()
    for idx, ch in enumerate(raw):
        if ch not in "{[":
            continue
        try:
            parsed, end = decoder.raw_decode(raw[idx:])
        except ValueError:
            continue
        if raw[idx + end :].strip():
            continue
        if isinstance(parsed, dict):
            return parsed
        raise ValueError(f"expected JSON object payload, got {type(parsed).__name__}")

    preview = raw[:200].replace("\n", "\\n")
    raise ValueError(f"could not parse JSON payload from output: {preview}")


def run_json_check(fn: Callable[[Sequence[str]], int], args: Sequence[str]) -> tuple[int, dict[str, Any]]:
    out = StringIO()
    with redirect_stdout(out):
        rc = fn(list(args))
    return rc, parse_json_payload(out.getvalue())

