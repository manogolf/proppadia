#!/usr/bin/env python3
"""Run the frozen Points scorer and bind every line to canonical input lineage."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path
import pandas as pd

from backend.nhl.prediction_lineage import bind_scorer_output_lineage
from backend.nhl.model_identity import fitted_model_identity
from backend.nhl.points_shadow.core import verify_feature_contract_identity


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-csv", required=True)
    parser.add_argument("--model-root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    feature_contract = verify_feature_contract_identity()
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
        scored = pd.read_csv(output)
        lines = sorted({float(value) for value in scored.line}) if "line" in scored else []
        if not lines:
            lines = sorted(float(directory.name.replace("_", ".")) for directory in Path(args.model_root).iterdir()
                           if directory.is_dir() and directory.name.replace("_", "").replace(".", "").isdigit())
        components = [
            ("scorer", scorer),
            ("feature_construction", Path(__file__).resolve().parents[1] / "sql" / "export_points.sql"),
        ]
        for line in lines:
            tag = f"{line:g}".replace(".", "_")
            line_dir = Path(args.model_root) / tag
            components.extend(((f"line_{tag}_fitted_model", line_dir / "lr.joblib"),
                               (f"line_{tag}_feature_metadata", line_dir / "feature_metadata.json")))
        evidence = fitted_model_identity(
            model_family="phoenix", model_version="phoenix_v2", components=components,
            prediction_path=output, scoring_run_id=output.parent.name,
            scoring_configuration={
                "scored_lines": lines,
                "output_cardinality": "many_to_one",
                "feature_contract_version": feature_contract["version"],
                "feature_contract_sha256": feature_contract["sha256"],
                "feature_construction_sha256": feature_contract["feature_construction_sha256"],
            })
        print("NHL_CHILD_SUMMARY_JSON=" + json.dumps({"fitted_model_evidence": evidence}, sort_keys=True))
    finally:
        unbound.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
