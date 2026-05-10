#!/usr/bin/env python3
"""Train a v0 hits residual ranker.

This is an isolated model-v2 experiment. It does not modify the current MLB
probability model, production model artifacts, database rows, or frontend data.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.pipeline import Pipeline


DEFAULT_INPUT_CSV = Path("backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit.csv")
DEFAULT_MODEL_OUT = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib")
DEFAULT_FEATURES_OUT = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker_features.json")
DEFAULT_DIAGNOSTICS_OUT = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker_train_diagnostics.json")
TRAIN_FROM = "2024-03-28"
TRAIN_TO = "2025-09-28"

EXCLUDE_EXACT = {
    "actual_value",
    "residual",
    "player_id",
    "game_id",
    "player_id_key",
    "game_id_key",
    "game_date",
    "date",
    "player_name",
    "prop_type",
    "prop_type_norm",
    "side",
    "source_reconcile_file",
    "joined_to_player_derived_stats",
}

EXCLUDE_SUBSTRINGS = (
    "outcome",
    "pnl",
    "profit",
    "odds",
    "price",
    "model_prob",
    "implied",
    "fair",
    "bookmaker",
    "market",
)


def _load_training_rows(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Missing input CSV: {path}")
    df = pd.read_csv(path, low_memory=False)
    required = {"game_date", "prop_type", "actual_value", "line", "residual"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{path} missing required columns: {missing}")

    out = df.copy()
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["actual_value"] = pd.to_numeric(out["actual_value"], errors="coerce")
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["residual"] = pd.to_numeric(out["residual"], errors="coerce")

    mask = (
        out["prop_type_norm"].eq("hits")
        & out["game_date"].between(pd.Timestamp(TRAIN_FROM), pd.Timestamp(TRAIN_TO), inclusive="both")
        & out["actual_value"].notna()
        & out["line"].notna()
        & out["residual"].notna()
    )
    if "joined_to_player_derived_stats" in out.columns:
        mask &= out["joined_to_player_derived_stats"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    out = out[mask].copy()
    if out.empty:
        raise SystemExit("No eligible training rows after filters.")
    return out


def _feature_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        low = col.strip().lower()
        if low in EXCLUDE_EXACT:
            continue
        if any(part in low for part in EXCLUDE_SUBSTRINGS):
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if series.notna().any():
            cols.append(col)
    if "line" not in cols and "line" in df.columns:
        cols.append("line")
    # Keep line first because it is the only non-PDS numeric feature by design.
    return ["line"] + sorted(c for c in cols if c != "line")


def _make_model(model_type: str, random_state: int) -> Pipeline:
    if model_type == "random_forest":
        reg = RandomForestRegressor(
            n_estimators=250,
            min_samples_leaf=20,
            random_state=random_state,
            n_jobs=-1,
        )
    else:
        reg = HistGradientBoostingRegressor(
            learning_rate=0.06,
            max_iter=250,
            min_samples_leaf=30,
            l2_regularization=0.01,
            random_state=random_state,
        )
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("model", reg),
        ]
    )


def _feature_importances(
    model: Pipeline,
    X: pd.DataFrame,
    y: pd.Series,
    feature_cols: list[str],
    *,
    sample_rows: int,
    random_state: int,
) -> list[dict[str, Any]]:
    reg = model.named_steps["model"]
    raw = getattr(reg, "feature_importances_", None)
    if raw is not None:
        pairs = sorted(zip(feature_cols, raw), key=lambda item: float(item[1]), reverse=True)
        return [{"feature": f, "importance": float(v), "method": "native"} for f, v in pairs[:20]]

    # HistGradientBoostingRegressor has no native feature_importances_.
    # Use a bounded in-sample permutation diagnostic so the v0 audit remains quick.
    if sample_rows <= 0:
        return []
    sample = X.sample(n=min(sample_rows, len(X)), random_state=random_state)
    target = y.loc[sample.index]
    result = permutation_importance(
        model,
        sample,
        target,
        n_repeats=3,
        random_state=random_state,
        scoring="neg_mean_squared_error",
        n_jobs=1,
    )
    pairs = sorted(
        zip(feature_cols, result.importances_mean),
        key=lambda item: float(item[1]),
        reverse=True,
    )
    return [{"feature": f, "importance": float(v), "method": "permutation_neg_mse"} for f, v in pairs[:20]]


def _decile_table(pred: np.ndarray, residual: pd.Series) -> list[dict[str, Any]]:
    work = pd.DataFrame({"prediction": pred, "residual": pd.to_numeric(residual, errors="coerce")})
    work = work.dropna()
    if work.empty:
        return []
    ranked = work["prediction"].rank(method="first")
    bins = min(10, len(work))
    work["predicted_rank_decile"] = pd.qcut(ranked, q=bins, labels=False, duplicates="drop") + 1
    table = (
        work.groupby("predicted_rank_decile", dropna=False)
        .agg(
            rows=("residual", "size"),
            prediction_min=("prediction", "min"),
            prediction_max=("prediction", "max"),
            prediction_mean=("prediction", "mean"),
            actual_avg_residual=("residual", "mean"),
            actual_median_residual=("residual", "median"),
        )
        .reset_index()
        .sort_values("predicted_rank_decile")
    )
    return table.to_dict(orient="records")


def _describe_target(y: pd.Series) -> dict[str, Any]:
    return {
        "mean": float(y.mean()),
        "std": float(y.std(ddof=0)),
        "min": float(y.min()),
        "max": float(y.max()),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    input_csv = Path(args.input_csv)
    model_out = Path(args.model_out)
    features_out = Path(args.features_out)
    diagnostics_out = Path(args.diagnostics_out)
    model_out.parent.mkdir(parents=True, exist_ok=True)
    features_out.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_out.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_training_rows(input_csv)
    feature_cols = _feature_columns(rows)
    if not feature_cols:
        raise SystemExit("No numeric feature columns available.")

    X = rows[feature_cols].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(rows["residual"], errors="coerce")
    valid = y.notna()
    X = X.loc[valid]
    y = y.loc[valid]

    model = _make_model(args.model_type, args.random_state)
    model.fit(X, y)
    pred = model.predict(X)

    spearman = float(pd.Series(pred, index=y.index).corr(y, method="spearman"))
    top_importances = _feature_importances(
        model,
        X,
        y,
        feature_cols,
        sample_rows=args.importance_sample_rows,
        random_state=args.random_state,
    )

    artifact = {
        "model": model,
        "feature_columns": feature_cols,
        "target": "residual",
        "prop_type": "hits",
        "train_from": TRAIN_FROM,
        "train_to": TRAIN_TO,
        "model_type": args.model_type,
    }
    joblib.dump(artifact, model_out)

    features_payload = {
        "prop_type": "hits",
        "target": "residual = actual_value - line",
        "train_from": TRAIN_FROM,
        "train_to": TRAIN_TO,
        "feature_count": len(feature_cols),
        "feature_columns": feature_cols,
        "excluded_rules": {
            "exact": sorted(EXCLUDE_EXACT),
            "substrings": list(EXCLUDE_SUBSTRINGS),
        },
    }
    features_out.write_text(json.dumps(features_payload, indent=2, sort_keys=True), encoding="utf-8")

    diagnostics = {
        "input_csv": str(input_csv),
        "model_out": str(model_out),
        "features_out": str(features_out),
        "diagnostics_out": str(diagnostics_out),
        "model_type": args.model_type,
        "rows_trained": int(len(X)),
        "feature_count": int(len(feature_cols)),
        "target": _describe_target(y),
        "in_sample_spearman_prediction_vs_residual": spearman,
        "top_20_feature_importances": top_importances,
        "decile_table": _decile_table(pred, y),
    }
    diagnostics_out.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    return diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train v0 hits residual ranker.")
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV))
    parser.add_argument("--model-out", default=str(DEFAULT_MODEL_OUT))
    parser.add_argument("--features-out", default=str(DEFAULT_FEATURES_OUT))
    parser.add_argument("--diagnostics-out", default=str(DEFAULT_DIAGNOSTICS_OUT))
    parser.add_argument("--model-type", choices=["hist_gradient_boosting", "random_forest"], default="hist_gradient_boosting")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--importance-sample-rows", type=int, default=10000)
    return parser.parse_args()


def main() -> None:
    diagnostics = run(parse_args())
    print(f"Wrote {diagnostics['model_out']}")
    print(f"Wrote {diagnostics['features_out']}")
    print(f"Wrote {diagnostics['diagnostics_out']}")
    print(
        "rows={rows_trained} features={feature_count} spearman={in_sample_spearman_prediction_vs_residual:.4f}".format(
            **diagnostics
        )
    )


if __name__ == "__main__":
    main()
