#!/usr/bin/env python3
"""Fail-closed, default-off recovered UBO-5 Total Bases 1.5 scorer.

This utility does not train a model. It scores only when an exact serialized
UBO-5 artifact, feature ledger, and hash are supplied by a future activation
task. The current certified UBO-5 package has no such serialized artifact.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ENABLE_FLAG = "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE"
IDENTITY = ["slate_date", "game_pk", "batter_mlb_id", "prop_type", "line"]
REQUIRED_FEATURE_META = [
    "scheduled_start_utc", "player_name", "team", "opponent",
    "strict_prior_pa", "starter_certification", "source_lineage_pointer",
]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--input-ledger", required=True, type=Path)
    parser.add_argument("--output-ledger", required=True, type=Path)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--artifact-sha256")
    parser.add_argument("--feature-order", type=Path)
    parser.add_argument("--shadow-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frame = pd.read_csv(args.input_ledger)
    aliases = {
        "game_id": "game_pk", "player_id": "batter_mlb_id",
        "game_time": "scheduled_start_utc", "game_start_time_utc": "scheduled_start_utc",
    }
    frame = frame.rename(columns={k: v for k, v in aliases.items() if k in frame})
    frame = frame[frame.slate_date.astype(str).eq(args.slate_date)].copy()
    for column in IDENTITY + REQUIRED_FEATURE_META + ["production_prob_over"]:
        if column not in frame:
            frame[column] = np.nan
    frame["run_tag"] = args.run_tag
    frame["prediction_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    frame["ubo5_probability_over"] = np.nan
    frame["probability_delta"] = np.nan
    frame["ubo5_artifact_hash"] = ""
    frame["recovered_artifact_path"] = ""
    frame["production_artifact_hash"] = ""
    frame["feature_vector_hash"] = ""
    frame["route_eligibility"] = False
    frame["feature_completeness_status"] = "INCOMPLETE"
    frame["exclusion_reason"] = ""

    enabled = os.environ.get(ENABLE_FLAG, "0") == "1"
    artifact_ok = bool(
        args.artifact and args.artifact.exists() and args.artifact_sha256
        and file_sha256(args.artifact) == args.artifact_sha256
    )
    order_ok = bool(args.feature_order and args.feature_order.exists())
    eligible = (
        frame.prop_type.eq("total_bases")
        & frame.line.eq(1.5)
        & frame.strict_prior_pa.ge(100)
        & frame.starter_certification.eq("CERTIFIED_PREGAME_STARTER")
        & pd.to_datetime(frame.scheduled_start_utc, utc=True, errors="coerce").gt(
            pd.Timestamp.now(tz="UTC")
        )
    )

    if not enabled and not args.shadow_only:
        frame["exclusion_reason"] = "DEFAULT_OFF_FLAG_DISABLED"
    elif not artifact_ok:
        frame["exclusion_reason"] = "UBO5_SERIALIZED_ARTIFACT_MISSING_OR_HASH_MISMATCH"
    elif not order_ok:
        frame["exclusion_reason"] = "FROZEN_FEATURE_ORDER_MISSING"
    else:
        features = pd.read_csv(args.feature_order).sort_values("ordinal").feature.tolist()
        missing = [column for column in features if column not in frame]
        if missing:
            frame["exclusion_reason"] = "MISSING_FEATURES:" + "|".join(missing)
        else:
            bundle = joblib.load(args.artifact)
            if bundle["features"] != features:
                raise RuntimeError("artifact feature order does not match frozen schema")
            model = bundle["model"]
            score_mask = eligible
            probability = model.predict_proba(frame.loc[score_mask, features])
            classes = list(model.classes_)
            score_indexes = list(frame.index[score_mask])
            for row_pos, idx in enumerate(score_indexes):
                p = dict(zip(classes, probability[row_pos]))
                frame.at[idx, "ubo5_probability_over"] = 1.0 - p.get(0, 0.0) - p.get(1, 0.0)
                vector = "|".join(f"{feature}={frame.at[idx, feature]!r}" for feature in features)
                frame.at[idx, "feature_vector_hash"] = hashlib.sha256(vector.encode()).hexdigest()
            frame.loc[score_mask, "ubo5_artifact_hash"] = args.artifact_sha256
            frame.loc[score_mask, "recovered_artifact_path"] = str(args.artifact)
            frame.loc[score_mask, "feature_completeness_status"] = "COMPLETE"
            frame.loc[score_mask, "route_eligibility"] = True
            frame.loc[score_mask, "exclusion_reason"] = ""
            frame.loc[~eligible, "exclusion_reason"] = "ROUTE_INELIGIBLE"
            frame["probability_delta"] = frame.ubo5_probability_over - frame.production_prob_over
            if args.shadow_only and not enabled:
                frame.loc[score_mask, "exclusion_reason"] = "SHADOW_ONLY_DEFAULT_OFF_NO_PRODUCTION_ROUTE"

    if frame[IDENTITY].duplicated().any():
        raise RuntimeError("duplicate canonical Total Bases identities")
    cols = [
        "slate_date", "run_tag", "prediction_timestamp_utc", "scheduled_start_utc",
        "game_pk", "batter_mlb_id", "player_name", "team", "opponent", "line",
        "strict_prior_pa", "starter_certification", "feature_completeness_status",
        "feature_vector_hash", "ubo5_probability_over",
        "production_prob_over", "probability_delta", "ubo5_artifact_hash",
        "recovered_artifact_path", "production_artifact_hash", "route_eligibility",
        "exclusion_reason", "source_lineage_pointer",
    ]
    args.output_ledger.parent.mkdir(parents=True, exist_ok=True)
    frame[cols].to_csv(args.output_ledger, index=False)


if __name__ == "__main__":
    main()
