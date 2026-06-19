#!/usr/bin/env python3
"""Shadow-score the dedicated total_bases + rolling candidate.

This is analysis-only infrastructure. It does not modify production
predictions, selectors, thresholds, upload rows, grading, or matching.
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_TRAINING_DATASET = ROOT / (
    "artifacts/analysis/mlb/model_quality/total_bases_canonical_spine_rolling_hydrated/"
    "2026-04-01_2026-06-14/total_bases_canonical_spine_dry_run_dataset.csv"
)
DEFAULT_SLATE_OUTPUT = ROOT / "backend/mlb/data/processed/mlb_slate_output.csv"
DEFAULT_OUT_ROOT = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow"
PROP_TYPE = "total_bases"

ROLLING_PRODUCTION = [
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "d7_total_bases",
    "d15_total_bases",
    "d30_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "d7_strikeouts_batting",
    "d15_strikeouts_batting",
    "d30_strikeouts_batting",
]
NUMERIC_FEATURES = ["line", *ROLLING_PRODUCTION]
CATEGORICAL_FEATURES = ["line_bucket"]
OUTPUT_ID_COLS = [
    "slate_date",
    "game_date",
    "game_id",
    "game_time",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def line_bucket(value: Any) -> str:
    try:
        line = float(value)
    except Exception:
        return "missing"
    if line <= 0.5:
        return "0.5"
    if line <= 1.5:
        return "1.5"
    if line <= 2.5:
        return "2.5"
    if line <= 3.5:
        return "3.5"
    return "4.5+"


def prob_bin(value: Any) -> str:
    try:
        prob = float(value)
    except Exception:
        return "missing"
    for lo, hi in [(0, 0.4), (0.4, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0)]:
        if lo <= prob < hi or (hi == 1.0 and prob <= hi):
            return f"{lo:.1f}-{hi:.1f}"
    return "other"


def normalize_bool(value: Any) -> float:
    return float(str(value).strip().lower() in {"true", "1", "1.0", "yes"})


def add_model_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "date" not in out.columns:
        out["date"] = out.get("slate_date", out.get("game_date"))
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["line_bucket"] = out["line"].map(line_bucket)
    for col in NUMERIC_FEATURES:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in CATEGORICAL_FEATURES:
        if col not in out.columns:
            out[col] = "missing"
        out[col] = out[col].fillna("missing").astype(str)
    return out


def make_pipeline(*, class_weight: str | None) -> Pipeline:
    try:
        onehot = OneHotEncoder(handle_unknown="ignore", min_frequency=10, sparse_output=True)
    except TypeError:
        onehot = OneHotEncoder(handle_unknown="ignore", min_frequency=10, sparse=True)
    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), NUMERIC_FEATURES),
            ("cat", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", onehot)]), CATEGORICAL_FEATURES),
        ],
        remainder="drop",
    )
    clf = LogisticRegression(max_iter=2000, solver="lbfgs", class_weight=class_weight)
    return Pipeline([("pre", pre), ("clf", clf)])


def load_training(path: Path, train_through: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = df[df["prop_type"].astype(str).str.lower().eq(PROP_TYPE)].copy()
    df = add_model_features(df)
    df = df[df["date"].le(train_through)].copy()
    df = df[df["actual_over_outcome"].astype(str).str.lower().isin({"win", "loss"})].copy()
    df["y_over"] = df["actual_over_outcome"].astype(str).str.lower().eq("win").astype(int)
    return df


def load_slate(path: Path, slate_date: str | None) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = df[df["prop_type"].astype(str).str.lower().eq(PROP_TYPE)].copy()
    if slate_date:
        date_col = "slate_date" if "slate_date" in df.columns else "game_date"
        df = df[pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d").eq(slate_date)].copy()
    df = add_model_features(df)
    prob_col = "prob_over" if "prob_over" in df.columns else "model_prob_over"
    if prob_col not in df.columns:
        raise ValueError(f"slate output missing production probability column: tried {prob_col}")
    df["production_prob_over"] = pd.to_numeric(df[prob_col], errors="coerce")
    return df[df["production_prob_over"].notna()].copy()


def feature_importance(pipe: Pipeline, *, model_name: str) -> pd.DataFrame:
    try:
        names = pipe.named_steps["pre"].get_feature_names_out()
    except Exception:
        names = []
    coef = getattr(pipe.named_steps["clf"], "coef_", None)
    if coef is None or len(names) == 0:
        return pd.DataFrame()
    out = pd.DataFrame({"model": model_name, "feature": names, "coefficient": coef[0]})
    out["abs_coefficient"] = out["coefficient"].abs()
    return out.sort_values("abs_coefficient", ascending=False)


def score_slate(train: pd.DataFrame, slate: pd.DataFrame, balanced_pipe: Pipeline, unweighted_pipe: Pipeline) -> pd.DataFrame:
    scored = slate.copy()
    scored["tb_rolling_balanced_shadow_prob_over"] = balanced_pipe.predict_proba(scored[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[:, 1]
    scored["tb_rolling_balanced_shadow_prob_under"] = 1.0 - scored["tb_rolling_balanced_shadow_prob_over"]
    scored["tb_rolling_unweighted_shadow_prob_over"] = unweighted_pipe.predict_proba(scored[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[:, 1]
    scored["tb_rolling_unweighted_shadow_prob_under"] = 1.0 - scored["tb_rolling_unweighted_shadow_prob_over"]
    # Backward-compatible alias for pre-existing readers. The explicit balanced
    # name is the canonical column going forward.
    scored["tb_rolling_shadow_prob_over"] = scored["tb_rolling_balanced_shadow_prob_over"]
    scored["tb_rolling_shadow_prob_under"] = scored["tb_rolling_balanced_shadow_prob_under"]
    scored["production_prob_under"] = 1.0 - scored["production_prob_over"]
    scored["tb_rolling_balanced_probability_delta_over"] = scored["tb_rolling_balanced_shadow_prob_over"] - scored["production_prob_over"]
    scored["tb_rolling_unweighted_probability_delta_over"] = scored["tb_rolling_unweighted_shadow_prob_over"] - scored["production_prob_over"]
    scored["probability_delta_over"] = scored["tb_rolling_balanced_probability_delta_over"]
    scored["production_pick_side"] = np.where(scored["production_prob_over"] >= 0.5, "over", "under")
    scored["tb_rolling_balanced_shadow_pick_side"] = np.where(scored["tb_rolling_balanced_shadow_prob_over"] >= 0.5, "over", "under")
    scored["tb_rolling_unweighted_shadow_pick_side"] = np.where(scored["tb_rolling_unweighted_shadow_prob_over"] >= 0.5, "over", "under")
    scored["tb_rolling_shadow_pick_side"] = scored["tb_rolling_balanced_shadow_pick_side"]
    scored["tb_rolling_balanced_side_changed"] = scored["production_pick_side"].ne(scored["tb_rolling_balanced_shadow_pick_side"])
    scored["tb_rolling_unweighted_side_changed"] = scored["production_pick_side"].ne(scored["tb_rolling_unweighted_shadow_pick_side"])
    scored["side_changed"] = scored["tb_rolling_balanced_side_changed"]
    scored["rolling_context_present"] = scored[ROLLING_PRODUCTION].notna().any(axis=1)
    scored["rolling_context_complete"] = scored[ROLLING_PRODUCTION].notna().all(axis=1)
    scored["model_train_rows"] = len(train)
    scored["shadow_model_name"] = "dedicated_total_bases_plus_rolling_dual_shadow"
    keep = [c for c in OUTPUT_ID_COLS if c in scored.columns]
    keep += [
        "production_prob_over",
        "production_prob_under",
        "tb_rolling_balanced_shadow_prob_over",
        "tb_rolling_balanced_shadow_prob_under",
        "tb_rolling_unweighted_shadow_prob_over",
        "tb_rolling_unweighted_shadow_prob_under",
        "tb_rolling_shadow_prob_over",
        "tb_rolling_shadow_prob_under",
        "tb_rolling_balanced_probability_delta_over",
        "tb_rolling_unweighted_probability_delta_over",
        "probability_delta_over",
        "production_pick_side",
        "tb_rolling_balanced_shadow_pick_side",
        "tb_rolling_unweighted_shadow_pick_side",
        "tb_rolling_shadow_pick_side",
        "tb_rolling_balanced_side_changed",
        "tb_rolling_unweighted_side_changed",
        "side_changed",
        "rolling_context_present",
        "rolling_context_complete",
        *[c for c in ROLLING_PRODUCTION if c in scored.columns],
        "prediction_source_file",
        "generated_at_utc",
        "shadow_model_name",
        "model_train_rows",
    ]
    return scored[[c for c in keep if c in scored.columns]].copy()


def safe_auc(y: pd.Series, p: pd.Series) -> float | None:
    try:
        if pd.Series(y).nunique() < 2:
            return None
        return float(roc_auc_score(y.astype(int), np.clip(p.astype(float), 1e-6, 1 - 1e-6)))
    except Exception:
        return None


def metric_row(label: str, y: pd.Series, p: pd.Series) -> dict[str, Any]:
    yy = y.astype(int)
    pp = np.clip(p.astype(float), 1e-6, 1 - 1e-6)
    return {
        "model": label,
        "rows": int(len(yy)),
        "brier": float(brier_score_loss(yy, pp)) if len(yy) else None,
        "log_loss": float(log_loss(yy, pp, labels=[0, 1])) if len(yy) else None,
        "auc": safe_auc(yy, pp),
        "avg_prob": float(pp.mean()) if len(yy) else None,
        "actual_over_rate": float(yy.mean()) if len(yy) else None,
    }


def evaluation_from_outcomes(shadow: pd.DataFrame, outcomes_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    outcomes = pd.read_csv(outcomes_csv, low_memory=False)
    outcomes = outcomes[outcomes["prop_type"].astype(str).str.lower().eq(PROP_TYPE)].copy()
    date_col = "slate_date" if "slate_date" in outcomes.columns else "game_date"
    outcomes["slate_date"] = pd.to_datetime(outcomes[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    shadow["slate_date"] = pd.to_datetime(shadow["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["game_id", "player_id", "line"]:
        outcomes[col] = pd.to_numeric(outcomes[col], errors="coerce")
        shadow[col] = pd.to_numeric(shadow[col], errors="coerce")
    cols = ["slate_date", "game_id", "player_id", "prop_type", "line"]
    outcomes = outcomes.drop_duplicates(cols, keep="first")
    joined = shadow.merge(
        outcomes[cols + ["actual_over_outcome", "actual_value"]],
        on=cols,
        how="left",
        validate="one_to_one",
    )
    resolved = joined[joined["actual_over_outcome"].astype(str).str.lower().isin({"win", "loss"})].copy()
    resolved["y_over"] = resolved["actual_over_outcome"].astype(str).str.lower().eq("win").astype(int)
    overall = pd.DataFrame(
        [
            metric_row("production", resolved["y_over"], resolved["production_prob_over"]),
            metric_row("tb_rolling_balanced_shadow", resolved["y_over"], resolved["tb_rolling_balanced_shadow_prob_over"]),
            metric_row("tb_rolling_unweighted_shadow", resolved["y_over"], resolved["tb_rolling_unweighted_shadow_prob_over"]),
        ]
    )
    breakdown_rows: list[dict[str, Any]] = []
    for model_name, pred_col in [
        ("production", "production_prob_over"),
        ("tb_rolling_balanced_shadow", "tb_rolling_balanced_shadow_prob_over"),
        ("tb_rolling_unweighted_shadow", "tb_rolling_unweighted_shadow_prob_over"),
    ]:
        for breakdown, values in [
            ("line_bucket", resolved["line"].map(line_bucket)),
            ("predicted_side", np.where(resolved[pred_col] >= 0.5, "over", "under")),
            ("probability_bucket", resolved[pred_col].map(prob_bin)),
        ]:
            tmp = resolved.copy()
            tmp["_bucket"] = values
            for bucket, group in tmp.groupby("_bucket", dropna=False):
                if len(group) < 5:
                    continue
                row = metric_row(model_name, group["y_over"], group[pred_col])
                row.update({"breakdown": breakdown, "bucket": bucket})
                breakdown_rows.append(row)
    calibration_rows: list[dict[str, Any]] = []
    for model_name, pred_col in [
        ("production", "production_prob_over"),
        ("tb_rolling_balanced_shadow", "tb_rolling_balanced_shadow_prob_over"),
        ("tb_rolling_unweighted_shadow", "tb_rolling_unweighted_shadow_prob_over"),
    ]:
        tmp = resolved.copy()
        tmp["_bucket"] = tmp[pred_col].map(prob_bin)
        for bucket, group in tmp.groupby("_bucket", dropna=False):
            if len(group) < 5:
                continue
            calibration_rows.append(
                {
                    "model": model_name,
                    "probability_bucket": bucket,
                    "rows": int(len(group)),
                    "avg_prob": float(group[pred_col].mean()),
                    "actual_over_rate": float(group["y_over"].mean()),
                    "calibration_error": float(group["y_over"].mean() - group[pred_col].mean()),
                }
            )
    return overall, pd.DataFrame(breakdown_rows), pd.DataFrame(calibration_rows)


def coverage_monitor(train: pd.DataFrame, shadow: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in NUMERIC_FEATURES:
        train_values = pd.to_numeric(train[col], errors="coerce")
        shadow_values = pd.to_numeric(shadow[col], errors="coerce")
        train_std = float(train_values.std()) if train_values.notna().sum() > 1 else 0.0
        delta = float(shadow_values.mean() - train_values.mean()) if shadow_values.notna().any() and train_values.notna().any() else np.nan
        rows.append(
            {
                "feature": col,
                "train_null_rate": float(train_values.isna().mean()),
                "shadow_null_rate": float(shadow_values.isna().mean()),
                "train_mean": float(train_values.mean()) if train_values.notna().any() else None,
                "shadow_mean": float(shadow_values.mean()) if shadow_values.notna().any() else None,
                "mean_delta": delta if pd.notna(delta) else None,
                "mean_delta_z": float(delta / train_std) if train_std else None,
            }
        )
    return pd.DataFrame(rows)


def write_markdown(summary: dict[str, Any], path: Path) -> None:
    lines = [
        "# Total Bases Shadow Candidate",
        "",
        f"Generated at: `{summary['generated_at']}`",
        f"Slate date: `{summary['slate_date']}`",
        "",
        "## Summary",
        "",
        "```json",
        json.dumps(summary, indent=2),
        "```",
        "",
        "This is shadow-only. Production probabilities, selectors, uploads, grading, and matching are unchanged.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Shadow-score dedicated total_bases + rolling candidate.")
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--training-dataset", default=str(DEFAULT_TRAINING_DATASET))
    ap.add_argument("--slate-output-csv", default=str(DEFAULT_SLATE_OUTPUT))
    ap.add_argument("--train-through", default="2026-06-14")
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    ap.add_argument("--out-dir")
    ap.add_argument("--outcomes-csv", help="Optional resolved reconcile/outcome CSV for evaluation.")
    args = ap.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else Path(args.out_root) / args.slate_date
    out_dir.mkdir(parents=True, exist_ok=True)
    train = load_training(Path(args.training_dataset), args.train_through)
    if train.empty:
        raise ValueError("no resolved total_bases training rows available")
    slate = load_slate(Path(args.slate_output_csv), args.slate_date)
    if slate.empty:
        raise ValueError(f"no total_bases rows found in slate output for {args.slate_date}")
    balanced_pipe = make_pipeline(class_weight="balanced")
    unweighted_pipe = make_pipeline(class_weight=None)
    balanced_pipe.fit(train[NUMERIC_FEATURES + CATEGORICAL_FEATURES], train["y_over"])
    unweighted_pipe.fit(train[NUMERIC_FEATURES + CATEGORICAL_FEATURES], train["y_over"])
    shadow = score_slate(train, slate, balanced_pipe, unweighted_pipe)
    feature_imp = pd.concat(
        [
            feature_importance(balanced_pipe, model_name="tb_rolling_balanced_shadow"),
            feature_importance(unweighted_pipe, model_name="tb_rolling_unweighted_shadow"),
        ],
        ignore_index=True,
        sort=False,
    )
    drift = coverage_monitor(train, shadow)

    shadow_csv = out_dir / f"total_bases_shadow_scores_{args.slate_date}.csv"
    feature_csv = out_dir / f"total_bases_shadow_feature_importance_{args.slate_date}.csv"
    drift_csv = out_dir / f"total_bases_shadow_coverage_drift_{args.slate_date}.csv"
    shadow.to_csv(shadow_csv, index=False)
    feature_imp.to_csv(feature_csv, index=False)
    drift.to_csv(drift_csv, index=False)

    evaluation_outputs: dict[str, str] = {}
    evaluation_summary: dict[str, Any] = {"outcomes_supplied": bool(args.outcomes_csv)}
    if args.outcomes_csv:
        overall, breakdown, calibration = evaluation_from_outcomes(shadow.copy(), Path(args.outcomes_csv))
        overall_csv = out_dir / f"total_bases_shadow_evaluation_{args.slate_date}.csv"
        breakdown_csv = out_dir / f"total_bases_shadow_evaluation_breakdowns_{args.slate_date}.csv"
        calibration_csv = out_dir / f"total_bases_shadow_calibration_bins_{args.slate_date}.csv"
        overall.to_csv(overall_csv, index=False)
        breakdown.to_csv(breakdown_csv, index=False)
        calibration.to_csv(calibration_csv, index=False)
        evaluation_outputs = {
            "evaluation_csv": rel(overall_csv),
            "evaluation_breakdowns_csv": rel(breakdown_csv),
            "calibration_bins_csv": rel(calibration_csv),
        }
        evaluation_summary["resolved_rows"] = int(overall["rows"].max()) if not overall.empty else 0

    latest_dir = Path(args.out_root) / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    for path in [shadow_csv, feature_csv, drift_csv]:
        shutil.copy2(path, latest_dir / path.name.replace(args.slate_date, "latest"))

    summary = {
        "generated_at": now_iso(),
        "slate_date": args.slate_date,
        "shadow_model_name": "dedicated_total_bases_plus_rolling_dual_shadow",
        "shadow_candidates": [
            {
                "name": "tb_rolling_balanced_shadow",
                "class_weight": "balanced",
                "status": "research_only_not_promotion_ready",
            },
            {
                "name": "tb_rolling_unweighted_shadow",
                "class_weight": None,
                "status": "research_only_pending_larger_sample",
            },
        ],
        "training_dataset": rel(Path(args.training_dataset)),
        "training_train_through": args.train_through,
        "training_rows": int(len(train)),
        "training_date_min": str(train["date"].min()),
        "training_date_max": str(train["date"].max()),
        "slate_output_csv": rel(Path(args.slate_output_csv)),
        "shadow_rows": int(len(shadow)),
        "side_changed_rows": int(shadow["tb_rolling_balanced_side_changed"].sum()),
        "side_changed_rate": float(shadow["tb_rolling_balanced_side_changed"].mean()) if len(shadow) else None,
        "tb_rolling_balanced_side_changed_rows": int(shadow["tb_rolling_balanced_side_changed"].sum()),
        "tb_rolling_balanced_side_changed_rate": float(shadow["tb_rolling_balanced_side_changed"].mean()) if len(shadow) else None,
        "tb_rolling_unweighted_side_changed_rows": int(shadow["tb_rolling_unweighted_side_changed"].sum()),
        "tb_rolling_unweighted_side_changed_rate": float(shadow["tb_rolling_unweighted_side_changed"].mean()) if len(shadow) else None,
        "avg_production_prob_over": float(shadow["production_prob_over"].mean()) if len(shadow) else None,
        "avg_shadow_prob_over": float(shadow["tb_rolling_balanced_shadow_prob_over"].mean()) if len(shadow) else None,
        "avg_tb_rolling_balanced_prob_over": float(shadow["tb_rolling_balanced_shadow_prob_over"].mean()) if len(shadow) else None,
        "avg_tb_rolling_unweighted_prob_over": float(shadow["tb_rolling_unweighted_shadow_prob_over"].mean()) if len(shadow) else None,
        "avg_probability_delta_over": float(shadow["tb_rolling_balanced_probability_delta_over"].mean()) if len(shadow) else None,
        "avg_tb_rolling_balanced_probability_delta_over": float(shadow["tb_rolling_balanced_probability_delta_over"].mean()) if len(shadow) else None,
        "avg_tb_rolling_unweighted_probability_delta_over": float(shadow["tb_rolling_unweighted_probability_delta_over"].mean()) if len(shadow) else None,
        "rolling_context_present_rate": float(shadow["rolling_context_present"].mean()) if len(shadow) else None,
        "rolling_context_complete_rate": float(shadow["rolling_context_complete"].mean()) if len(shadow) else None,
        "production_outputs_changed": False,
        "final_upload_outputs_changed": False,
        "evaluation": evaluation_summary,
        "outputs": {
            "shadow_scores_csv": rel(shadow_csv),
            "feature_importance_csv": rel(feature_csv),
            "coverage_drift_csv": rel(drift_csv),
            **evaluation_outputs,
            "summary_json": rel(out_dir / f"total_bases_shadow_summary_{args.slate_date}.json"),
            "summary_md": rel(out_dir / f"total_bases_shadow_summary_{args.slate_date}.md"),
        },
    }
    summary_json = out_dir / f"total_bases_shadow_summary_{args.slate_date}.json"
    summary_md = out_dir / f"total_bases_shadow_summary_{args.slate_date}.md"
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_markdown(summary, summary_md)
    shutil.copy2(summary_json, latest_dir / "total_bases_shadow_summary_latest.json")
    shutil.copy2(summary_md, latest_dir / "total_bases_shadow_summary_latest.md")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
