#!/usr/bin/env python3
"""Score and retain isolated, prospective NHL Points HGB research shadows.

This scorer deliberately accepts only the frozen 10-column HGB contract. It
does not construct features from Phoenix inputs or write to production systems.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/shadow_contract.json"
MODEL_PATH = CONTRACT_PATH.parent / "nhl_points_count_hgb_v1.joblib"
FEATURES = [
    "is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
    "current_season_points_prior", "current_season_games_prior",
    "mean_toi_last10", "mean_pp_toi_last10", "team_d10_sf_per_game",
    "last10_team_sog_share",
]
HISTORY_CONTRACT = "NHL_POINTS_COUNT_HGB_V1_HISTORY_120_DAY_VALIDATED"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def score(features: pd.DataFrame, model) -> pd.DataFrame:
    required = {"game_id", "player_id", *FEATURES}
    missing = sorted(required.difference(features.columns))
    if missing:
        raise ValueError(f"MISSING_FROZEN_HGB_FEATURES:{','.join(missing)}")
    if features.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_PLAYER_GAME_KEY")
    x = features[FEATURES].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0).astype(float)
    if isinstance(model, dict):
        scaler = model["scaler"]
        estimator = model["model"]
        x = scaler.transform(x.to_numpy())
    else:
        estimator = model
    mu = np.maximum(np.asarray(estimator.predict(x), dtype=float), 1e-8)
    out = features[[c for c in ("game_id", "player_id", "player_name", "game_date", "game_start_utc") if c in features]].copy()
    out["expected_points"] = mu
    for threshold, name in zip((1, 2, 3), ("prob_over_0_5", "prob_over_1_5", "prob_over_2_5")):
        out[name] = poisson.sf(threshold - 1, mu)
    if (out.prob_over_0_5 < out.prob_over_1_5).any() or (out.prob_over_1_5 < out.prob_over_2_5).any():
        raise ValueError("POISSON_THRESHOLD_COHERENCE_FAILURE")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path, required=True, help="Pregame CSV with all frozen HGB features")
    ap.add_argument("--out-root", type=Path, required=True, help="Dedicated immutable research output root")
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--capture-phase", required=True)
    ap.add_argument("--parent-run-id", required=True)
    ap.add_argument("--pregame-cutoff-utc", required=True)
    args = ap.parse_args()
    contract = json.loads(CONTRACT_PATH.read_text())
    if contract["history_contract"] != "120_DAY_LEGACY_BOUND":
        raise ValueError("UNEXPECTED_FROZEN_HISTORY_CONTRACT")
    features = pd.read_csv(args.features)
    if "game_date" not in features or "game_start_utc" not in features:
        raise ValueError("MISSING_PREGAME_TIME_BINDING")
    starts = pd.to_datetime(features.game_start_utc, utc=True, errors="coerce")
    cutoff = pd.Timestamp(args.pregame_cutoff_utc)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    if starts.isna().any() or not (starts > cutoff).all():
        raise ValueError("NOT_STRICTLY_PREGAME")
    if not features.game_date.astype(str).eq(args.slate_date).all():
        raise ValueError("NON_SLATE_ROWS_PRESENT")
    if "history_contract" not in features or not features.history_contract.eq("120_DAY_LEGACY_BOUND").all():
        raise ValueError("FEATURE_HISTORY_CONTRACT_NOT_BOUND")
    fitted = joblib.load(MODEL_PATH)
    if fitted.get("feature_columns") != FEATURES:
        raise ValueError("FITTED_ARTIFACT_FEATURE_CONTRACT_MISMATCH")
    predictions = score(features, fitted)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out = args.out_root / f"season=2026/slate_date={args.slate_date}/phase={args.capture_phase}/captured_at={stamp}"
    out.mkdir(parents=True, exist_ok=False)
    feature_out = out / "features.csv"
    prediction_out = out / "predictions.csv"
    features.to_csv(feature_out, index=False, float_format="%.17g")
    predictions.to_csv(prediction_out, index=False, float_format="%.17g")
    manifest = {
        "schema_version": "NHL_POINTS_HGB_PROSPECTIVE_SHADOW_V1",
        "research_model_id": contract["research_model_id"],
        "history_contract": HISTORY_CONTRACT,
        "canonical_game_ids": sorted(map(int, features.game_id.unique())),
        "canonical_game_set_hash": hashlib.sha256("\n".join(map(str, sorted(map(int, features.game_id.unique())))).encode()).hexdigest(),
        "slate_date": args.slate_date, "capture_phase": args.capture_phase,
        "parent_operational_run_id": args.parent_run_id,
        "capture_time_utc": datetime.now(timezone.utc).isoformat(),
        "pregame_cutoff_utc": cutoff.isoformat(),
        "feature_path": feature_out.name, "feature_sha256": sha(feature_out),
        "prediction_path": prediction_out.name, "prediction_sha256": sha(prediction_out),
        "model_path": MODEL_PATH.name, "model_sha256": sha(MODEL_PATH),
        "contract_sha256": sha(CONTRACT_PATH), "feature_columns": FEATURES,
        "row_count": len(predictions), "coherence_crossings": 0,
        "production_authority": False,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": "CAPTURED", "path": str(out), **manifest}, indent=2))


if __name__ == "__main__":
    main()
