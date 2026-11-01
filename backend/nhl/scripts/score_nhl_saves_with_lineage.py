#!/usr/bin/env python3
"""Score the daily Saves lane with the production Saves model and bind lineage.

``backend/nhl/saves_production.PRODUCTION`` selects the model: the Saves refit
(shots against x save rate, from 2026-11-01) scores the run's own Saves input
from read-only history; ``PHOENIX_V2`` runs the frozen generic scorer as before.
The output layout (``p_over_<line>`` per goalie-game, lineage columns) is the
same for both.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path

from backend.nhl.prediction_lineage import bind_scorer_output_lineage
from backend.nhl.model_identity import fitted_model_identity
from backend.nhl import saves_production


def bind_lineage(*, scoring_input: Path, unbound_output: Path, output: Path) -> None:
    bind_scorer_output_lineage(
        scoring_input=scoring_input, unbound_output=unbound_output, output=output,
        output_cardinality="one_to_one")


def score_refit_lane(args, output: Path, unbound: Path) -> None:
    """Refit scoring of the lane's input rows; same output columns as the generic scorer."""
    import pandas as pd
    from backend.nhl.database_access import database_url
    from backend.nhl.game_phase import authorized_season_for_date

    lines = sorted(float(value) for value in args.line.split(",") if value.strip())
    if lines != list(saves_production.srm.LINES):
        raise RuntimeError("SAVES_REFIT_LINE_SET_MISMATCH")
    source = pd.read_csv(args.csv)
    dates = source[args.date_col].astype(str).str[:10].unique()
    if len(dates) != 1:
        raise RuntimeError("SAVES_REFIT_SINGLE_SLATE_REQUIRED")
    slate_date = str(dates[0])
    model, contract = saves_production.load_refit()
    wide, history_info = saves_production.score_refit(
        source, slate_date=slate_date, season=authorized_season_for_date(slate_date),
        dsn=database_url("research"), model=model)
    columns = ["player_id", "game_id", *[f"p_over_{str(line).replace('.', '_')}" for line in lines]]
    try:
        wide[columns].to_csv(unbound, index=False)
        bind_lineage(scoring_input=Path(args.csv), unbound_output=unbound, output=output)
        label = saves_production.labels()
        evidence = fitted_model_identity(
            model_family=label["model_family"], model_version=label["model_version"],
            components=saves_production.components(), prediction_path=output,
            scoring_run_id=output.parent.name,
            scoring_configuration={"model_id": contract["model_id"], "lines": lines,
                                   "model_identity_sha256": contract["model_identity_sha256"],
                                   "calibration_layer": "NONE", "date_column": args.date_col})
        evidence["history"] = history_info
        print("NHL_CHILD_SUMMARY_JSON=" + json.dumps({"fitted_model_evidence": evidence}, sort_keys=True))
    finally:
        unbound.unlink(missing_ok=True)


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
    if saves_production.PRODUCTION == saves_production.SHOTS_X_SAVE_RATE_V1:
        score_refit_lane(args, output, unbound)
        return
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
