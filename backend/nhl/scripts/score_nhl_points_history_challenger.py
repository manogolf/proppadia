#!/usr/bin/env python3
"""Score three read-only player-history feature contracts with frozen Points models."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.scripts.score_points_phoenix import load_line_model

ROOT = Path(__file__).resolve().parents[3]
MODEL_ROOT = ROOT / "backend/nhl/models/latest/points"
SCORER = ROOT / "backend/nhl/scripts/score_points_phoenix.py"
ARMS = {
    "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2": "cross_season",
    "POINTS_PLAYER_HISTORY_120_DAY_LEGACY_SHADOW": "legacy_120_day",
    "POINTS_PLAYER_HISTORY_CURRENT_SEASON_ONLY_SHADOW": "current_season_only",
}
DYNAMIC_PLAYER_FIELDS = {
    "d5_sog_per60", "d10_sog_per60", "attempts_d10_per60", "last10_team_sog_share",
    "num_shotwasongoal_last5", "num_shotwasongoal_last10", "num_event_shot_last5",
    "num_event_shot_last10", "hot_last5_flag",
}
IDENTITY = ["player_id", "game_id"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_features(paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    frames = {arm: pd.read_csv(path) for arm, path in paths.items()}
    baseline = frames["cross_season"].sort_values(IDENTITY).reset_index(drop=True)
    keys = set(map(tuple, baseline[IDENTITY].astype(int).to_numpy()))
    for arm, frame in frames.items():
        if frame.duplicated(IDENTITY).any() or set(map(tuple, frame[IDENTITY].astype(int).to_numpy())) != keys:
            raise ValueError(f"ARM_IDENTITY_MISMATCH:{arm}")
        aligned = frame.sort_values(IDENTITY).reset_index(drop=True)
        shared = [c for c in baseline.columns if c in aligned.columns and c not in DYNAMIC_PLAYER_FIELDS]
        for col in shared:
            a, b = baseline[col], aligned[col]
            if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
                same = np.allclose(a.to_numpy(dtype=float), b.to_numpy(dtype=float), equal_nan=True)
            else:
                same = a.fillna("<NA>").astype(str).equals(b.fillna("<NA>").astype(str))
            if not same:
                raise ValueError(f"NON_PLAYER_HISTORY_FEATURE_DIFFERS:{arm}:{col}")
        frames[arm] = aligned
    return frames


def score(features: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for line in (0.5, 1.5, 2.5):
        feature_names, model = load_line_model(MODEL_ROOT / str(line).replace(".", "_") )
        missing = [name for name in feature_names if name not in features]
        if missing:
            raise ValueError(f"MISSING_MODEL_FEATURES:{line}:{missing}")
        probability = model.predict_proba(features[feature_names].astype(float).to_numpy())[:, 1]
        for row, p in zip(features[IDENTITY].itertuples(index=False, name=None), probability):
            rows.append({"player_id": int(row[0]), "game_id": int(row[1]), "line": line, "raw_prob_over": float(p)})
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of-date", required=True)
    ap.add_argument("--cross-season-csv", type=Path, required=True)
    ap.add_argument("--legacy-120-day-csv", type=Path, required=True)
    ap.add_argument("--current-season-only-csv", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    out = args.output_dir if args.output_dir.is_absolute() else ROOT / args.output_dir
    if out.exists():
        raise SystemExit(f"Refusing to overwrite shadow output: {out}")
    in_paths = {"cross_season": args.cross_season_csv, "legacy_120_day": args.legacy_120_day_csv, "current_season_only": args.current_season_only_csv}
    in_paths = {k: (p if p.is_absolute() else ROOT / p) for k, p in in_paths.items()}
    features = load_features(in_paths)
    model_paths = sorted(MODEL_ROOT.glob("*/lr.joblib"))
    model_hashes_before = {str(p.relative_to(ROOT)): sha(p) for p in model_paths}
    scorer_hash = sha(SCORER)
    out.mkdir(parents=True)
    summaries = {}
    for contract, arm in ARMS.items():
        scores = score(features[arm])
        scores.insert(0, "feature_contract_identity", contract)
        scores.insert(1, "as_of_date", args.as_of_date)
        scores.to_csv(out / f"{arm}_predictions.csv", index=False)
        summaries[contract] = {"feature_input_path": str(in_paths[arm].relative_to(ROOT)), "feature_input_sha256": sha(in_paths[arm]), "rows": len(features[arm]), "prediction_rows": len(scores)}
    if model_hashes_before != {str(p.relative_to(ROOT)): sha(p) for p in model_paths} or scorer_hash != sha(SCORER):
        raise RuntimeError("FROZEN_SCORER_OR_MODEL_CHANGED_DURING_SHADOW_SCORING")
    manifest = {"schema_version": "NHL_POINTS_HISTORY_SHADOW_RUN_V1", "as_of_date": args.as_of_date, "arms": summaries, "shared_model_root": str(MODEL_ROOT.relative_to(ROOT)), "model_sha256": model_hashes_before, "scorer_sha256": scorer_hash, "line_set": [0.5, 1.5, 2.5], "dynamic_player_history_fields": sorted(DYNAMIC_PLAYER_FIELDS), "identities_equal_across_arms": True, "shared_non_player_history_features_equal": True, "team_history_unchanged": True, "provider_calls": 0, "paid_credits": 0, "database_mutations": 0, "production_outputs_modified": False}
    (out / "shadow_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (out / "README.md").write_text("# NHL Points history challenger shadow\n\nImmutable raw-score comparison under the three named player-history contracts. The scorer uses the same retained frozen Phoenix line models and lines for all arms. Only player rolling-history fields may differ; identity, team features, and current-season-to-date fields must match or the run fails closed. No market join or production write occurs.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
