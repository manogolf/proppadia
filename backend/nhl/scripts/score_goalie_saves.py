#!/usr/bin/env python3
"""Compatibility scorer wrapper for legacy workflow path.

Normalizes legacy args to score_nhl_props.py.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_LINES = "18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,30.5"


def resolve_model_dir(model_root_arg: str) -> str:
    candidate = Path(model_root_arg)
    if candidate.exists():
        return str(candidate)

    fallback = Path("backend/nhl/models/latest/goalie_saves")
    return str(fallback)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-csv", required=True)
    parser.add_argument("--model-root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    cmd = [
        sys.executable,
        "backend/nhl/scripts/score_nhl_props.py",
        "--model-dir",
        resolve_model_dir(args.model_root),
        "--csv",
        args.features_csv,
        "--feature-json",
        "backend/nhl/features/feature_metadata_nhl.json",
        "--feature-key",
        "goalie_saves",
        "--line",
        DEFAULT_LINES,
        "--out",
        args.out,
    ]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
