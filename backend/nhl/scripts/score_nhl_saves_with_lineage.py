#!/usr/bin/env python3
"""Run the frozen generic Saves scorer and bind its output to input lineage."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path

from backend.nhl.prediction_lineage import bind_scorer_output_lineage
from backend.nhl.model_identity import fitted_model_identity


def bind_lineage(*, scoring_input: Path, unbound_output: Path, output: Path) -> None:
    bind_scorer_output_lineage(
        scoring_input=scoring_input, unbound_output=unbound_output, output=output,
        output_cardinality="one_to_one")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--csv", required=True)
    parser.add_argument("--feature-json", required=True)
    parser.add_argument("--feature-key", required=True)
    parser.add_argument("--line", required=True)
    parser.add_argument("--date-col", default="game_date")
    parser.add_argument("--out", required=True)
    parser.add_argument("--no-monotonic", action="store_true")
    args = parser.parse_args()

    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    unbound = output.with_name(f".{output.name}.{uuid.uuid4().hex}.unbound.csv")
    scorer = Path(__file__).with_name("score_nhl_props.py")
    command = [
        sys.executable, str(scorer),
        "--model-dir", args.model_dir,
        "--csv", args.csv,
        "--feature-json", args.feature_json,
        "--feature-key", args.feature_key,
        "--line", args.line,
        "--date-col", args.date_col,
        "--out", str(unbound),
    ]
    if args.no_monotonic:
        command.append("--no-monotonic")
    try:
        subprocess.run(command, check=True)
        bind_lineage(
            scoring_input=Path(args.csv), unbound_output=unbound, output=output)
        model_dir = Path(args.model_dir)
        components = [
            ("scorer", scorer),
            ("model_index", model_dir / "MODEL_INDEX.json"),
            ("fitted_model_and_calibration", model_dir / "MODEL_ARTIFACT.json"),
            ("feature_metadata", Path(args.feature_json)),
        ]
        evidence = fitted_model_identity(
            model_family="phoenix", model_version="phoenix_v2", components=components,
            prediction_path=output, scoring_run_id=output.parent.name,
            scoring_configuration={"feature_key": args.feature_key,
                                   "lines": sorted(float(value) for value in args.line.split(",") if value.strip()),
                                   "date_column": args.date_col, "no_monotonic": args.no_monotonic})
        print("NHL_CHILD_SUMMARY_JSON=" + json.dumps({"fitted_model_evidence": evidence}, sort_keys=True))
    finally:
        unbound.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
