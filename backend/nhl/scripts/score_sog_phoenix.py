#!/usr/bin/env python3
"""Compatibility scorer wrapper for legacy workflow path.

Normalizes legacy args to score_sog_denali.py.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def resolve_model_root_parent(model_root_arg: str) -> str:
    candidate = Path(model_root_arg)
    if candidate.exists():
        return str(candidate)
    fallback = Path("backend/nhl/models/sog")
    return str(fallback)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-csv", required=True)
    parser.add_argument("--model-root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    cmd = [
        sys.executable,
        "backend/nhl/scripts/score_sog_denali.py",
        "--features-csv",
        args.features_csv,
        "--model-root",
        resolve_model_root_parent(args.model_root),
        "--out",
        args.out,
    ]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
