#!/usr/bin/env python3
"""Run the frozen Points scorer and bind every line to canonical input lineage."""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from pathlib import Path

from backend.nhl.prediction_lineage import bind_scorer_output_lineage


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-csv", required=True)
    parser.add_argument("--model-root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    unbound = output.with_name(f".{output.name}.{uuid.uuid4().hex}.unbound.csv")
    scorer = Path(__file__).with_name("score_points_phoenix.py")
    command = [
        sys.executable, str(scorer),
        "--features-csv", args.features_csv,
        "--model-root", args.model_root,
        "--out", str(unbound),
    ]
    try:
        subprocess.run(command, check=True)
        bind_scorer_output_lineage(
            scoring_input=Path(args.features_csv), unbound_output=unbound,
            output=output, output_cardinality="many_to_one")
    finally:
        unbound.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
