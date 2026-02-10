# backend/nhl/scripts/train_sog_denali_pairings_ordinal_lgbm.py

from __future__ import annotations

import json
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split


DATA = "backend/nhl/exports/train_nhl_sog_denali_pairings_v1__no_shiftcounts.csv"

FEATURE_META = (
    "backend/nhl/models/latest/shots_on_goal/"
    "sog_player_denali_pairings_v1/1_5/"
    "feature_metadata__no_shiftcounts.json"
)

OUT_ROOT = Path(
    "backend/nhl/models/experimental/shots_on_goal/"
    "sog_player_denali_pairings_ordinal_v1__no_shiftcounts"
)
PARAMS = {
    "objective": "binary",
    "boosting_type": "gbdt",
    "learning_rate": 0.03,
    "num_leaves": 31,
    "max_depth": -1,
    "min_data_in_leaf": 100,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "metric": ["auc"],
    "verbosity": -1,
    # keep training deterministic
    "seed": 1337,
    "feature_fraction_seed": 1337,
    "bagging_seed": 1337,
}

def _load_feature_list() -> list[str]:
    with open(FEATURE_META) as f:
        meta = json.load(f)
    feats = meta.get("features")
    if not feats or not isinstance(feats, list):
        raise SystemExit(f"feature_metadata missing 'features': {FEATURE_META}")
    return feats


def _prep_X(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    miss = [c for c in feats if c not in df.columns]
    if miss:
        raise SystemExit(f"Training CSV missing {len(miss)} feature cols (sample): {miss[:20]}")

    X = (
        df[feats]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
        .astype(float)
    )
    return X


def train_one_threshold(
    X: pd.DataFrame,
    y: np.ndarray,
    threshold_name: str,
    threshold_def: str,
    feats: list[str],
) -> dict:
    # Holdout split (not time-based, but stable + fast)
    X_tr, X_va, y_tr, y_va = train_test_split(
        X, y, test_size=0.2, random_state=1337, stratify=y
    )

    dtrain = lgb.Dataset(X_tr, label=y_tr, feature_name=feats)
    dvalid = lgb.Dataset(X_va, label=y_va, feature_name=feats)

    model = lgb.train(
        PARAMS,
        dtrain,
        num_boost_round=5000,
        valid_sets=[dtrain, dvalid],
        valid_names=["train", "valid"],
        callbacks=[lgb.early_stopping(stopping_rounds=200, verbose=False)],
    )

    p_tr = model.predict(X_tr, num_iteration=model.best_iteration)
    p_va = model.predict(X_va, num_iteration=model.best_iteration)

    out = {
        "threshold": threshold_name,
        "definition": threshold_def,
        "rows": int(len(X)),
        "base_rate_all": float(np.mean(y)),
        "base_rate_train": float(np.mean(y_tr)),
        "base_rate_valid": float(np.mean(y_va)),
        "auc_train": float(roc_auc_score(y_tr, p_tr)),
        "auc_valid": float(roc_auc_score(y_va, p_va)),
        "best_iteration": int(model.best_iteration),
    }
    return model, out


def main() -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    feats = _load_feature_list()
    df = pd.read_csv(DATA)

    if "shots_on_goal" not in df.columns:
        raise SystemExit(f"{DATA} missing shots_on_goal")

    # Build X once
    X = _prep_X(df, feats)

    # True count target
    y = pd.to_numeric(df["shots_on_goal"], errors="coerce").fillna(0).astype(int).clip(lower=0).to_numpy()

    # Ordinal / cumulative thresholds (locks monotonicity by construction)
    targets = [
        ("ge_2", "shots_on_goal >= 2", (y >= 2).astype(int)),
        ("ge_3", "shots_on_goal >= 3", (y >= 3).astype(int)),
        ("ge_4", "shots_on_goal >= 4", (y >= 4).astype(int)),
    ]

    summary = {
        "model_family": "ordinal_cumulative_thresholds",
        "data": DATA,
        "feature_meta": FEATURE_META,
        "n_rows": int(len(df)),
        "n_features": int(len(feats)),
        "targets": [],
    }

    for name, definition, y_bin in targets:
        print(f"\n=== TRAIN {name} ({definition}) ===")
        model, metrics = train_one_threshold(X, y_bin, name, definition, feats)

        print(
            f"{name}: base_rate={metrics['base_rate_all']:.4f} "
            f"AUC_valid={metrics['auc_valid']:.4f} "
            f"best_iter={metrics['best_iteration']}"
        )

        out_dir = OUT_ROOT / name
        out_dir.mkdir(exist_ok=True)

        joblib.dump(model, out_dir / "lgbm.joblib")
        with open(out_dir / "metadata.json", "w") as f:
            json.dump(
                {
                    **metrics,
                    "features": feats,
                    "params": PARAMS,
                    "trained_from": DATA,
                },
                f,
                indent=2,
            )

        summary["targets"].append(metrics)

    with open(OUT_ROOT / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n✅ Done. Wrote models to: {OUT_ROOT}")


if __name__ == "__main__":
    main()