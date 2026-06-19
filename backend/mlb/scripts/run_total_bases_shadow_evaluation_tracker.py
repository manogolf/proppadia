#!/usr/bin/env python3
"""Read-only cumulative tracker for total_bases shadow scores.

Scans existing shadow score files, joins completed-slate reconcile outcomes when
available, and writes analysis artifacts only.
"""

from __future__ import annotations

import argparse
import json
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
DEFAULT_SHADOW_ROOT = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow"
DEFAULT_RECONCILE_ROOT = ROOT / "artifacts/analysis/mlb/execution_vs_model"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation"
DEFAULT_TRAINING_DATASET = ROOT / (
    "artifacts/analysis/mlb/model_quality/total_bases_canonical_spine_rolling_hydrated/"
    "2026-04-01_2026-06-14/total_bases_canonical_spine_dry_run_dataset.csv"
)
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
MODEL_SPECS = [
    ("production", "production_prob_over", "production_pick_side"),
    ("tb_rolling_balanced_shadow", "tb_rolling_balanced_shadow_prob_over", "tb_rolling_balanced_shadow_pick_side"),
    ("tb_rolling_unweighted_shadow", "tb_rolling_unweighted_shadow_prob_over", "tb_rolling_unweighted_shadow_pick_side"),
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


def prob_bucket(value: Any) -> str:
    try:
        prob = float(value)
    except Exception:
        return "missing"
    for lo, hi in [(0, 0.4), (0.4, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0)]:
        if lo <= prob < hi or (hi == 1.0 and prob <= hi):
            return f"{lo:.1f}-{hi:.1f}"
    return "other"


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


def make_unweighted_pipeline() -> Pipeline:
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
    return Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=2000, solver="lbfgs", class_weight=None))])


def load_training(path: Path, train_through: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = df[df["prop_type"].astype(str).str.lower().eq(PROP_TYPE)].copy()
    df = add_model_features(df)
    df = df[df["date"].le(train_through)].copy()
    df = df[df["actual_over_outcome"].astype(str).str.lower().isin({"win", "loss"})].copy()
    df["y_over"] = df["actual_over_outcome"].astype(str).str.lower().eq("win").astype(int)
    return df


def fill_missing_unweighted_shadow(shadow: pd.DataFrame, training_dataset: Path, train_through: str) -> tuple[pd.DataFrame, int]:
    if shadow.empty:
        return shadow, 0
    missing = shadow["tb_rolling_unweighted_shadow_prob_over"].isna()
    if not bool(missing.any()):
        shadow["tb_rolling_unweighted_shadow_source"] = shadow.get("tb_rolling_unweighted_shadow_source", "daily_shadow_score")
        return shadow, 0
    train = load_training(training_dataset, train_through)
    if train.empty:
        return shadow, 0
    pipe = make_unweighted_pipeline()
    pipe.fit(train[NUMERIC_FEATURES + CATEGORICAL_FEATURES], train["y_over"])
    scored = add_model_features(shadow.loc[missing].copy())
    probs = pipe.predict_proba(scored[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[:, 1]
    shadow.loc[missing, "tb_rolling_unweighted_shadow_prob_over"] = probs
    shadow.loc[missing, "tb_rolling_unweighted_shadow_prob_under"] = 1.0 - probs
    shadow["tb_rolling_unweighted_shadow_source"] = np.where(
        missing,
        "evaluation_recomputed_from_score_rows",
        shadow.get("tb_rolling_unweighted_shadow_source", "daily_shadow_score"),
    )
    return shadow, int(missing.sum())


def safe_auc(y: pd.Series, p: pd.Series) -> float | None:
    try:
        if y.nunique() < 2:
            return None
        return float(roc_auc_score(y.astype(int), np.clip(p.astype(float), 1e-6, 1 - 1e-6)))
    except Exception:
        return None


def metric_row(model: str, rows: pd.DataFrame, pred_col: str, *, subset: str = "overall") -> dict[str, Any]:
    resolved = rows[rows["y_over"].notna() & rows[pred_col].notna()].copy()
    if resolved.empty:
        return {
            "model": model,
            "subset": subset,
            "rows": 0,
            "brier": None,
            "log_loss": None,
            "auc": None,
            "avg_prob": None,
            "actual_over_rate": None,
            "overconfidence_gap": None,
        }
    y = resolved["y_over"].astype(int)
    p = np.clip(resolved[pred_col].astype(float), 1e-6, 1 - 1e-6)
    actual_rate = float(y.mean())
    avg_prob = float(p.mean())
    return {
        "model": model,
        "subset": subset,
        "rows": int(len(resolved)),
        "brier": float(brier_score_loss(y, p)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "auc": safe_auc(y, p),
        "avg_prob": avg_prob,
        "actual_over_rate": actual_rate,
        "overconfidence_gap": float(avg_prob - actual_rate),
    }


def shadow_score_files(shadow_root: Path) -> list[Path]:
    return sorted(
        p
        for p in shadow_root.glob("20??-??-??/total_bases_shadow_scores_*.csv")
        if p.is_file()
    )


def load_shadow_scores(shadow_root: Path, training_dataset: Path, train_through: str) -> tuple[pd.DataFrame, int]:
    frames: list[pd.DataFrame] = []
    for path in shadow_score_files(shadow_root):
        df = pd.read_csv(path, low_memory=False)
        df["shadow_score_file"] = rel(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    if "slate_date" not in out.columns:
        out["slate_date"] = out.get("game_date")
    out["slate_date"] = pd.to_datetime(out["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "tb_rolling_balanced_shadow_prob_over" not in out.columns:
        out["tb_rolling_balanced_shadow_prob_over"] = np.nan
    if "tb_rolling_shadow_prob_over" in out.columns:
        out["tb_rolling_balanced_shadow_prob_over"] = out["tb_rolling_balanced_shadow_prob_over"].combine_first(
            out["tb_rolling_shadow_prob_over"]
        )
    if "tb_rolling_balanced_shadow_prob_under" not in out.columns:
        out["tb_rolling_balanced_shadow_prob_under"] = np.nan
    if "tb_rolling_shadow_prob_under" in out.columns:
        out["tb_rolling_balanced_shadow_prob_under"] = out["tb_rolling_balanced_shadow_prob_under"].combine_first(
            out["tb_rolling_shadow_prob_under"]
        )
    if "tb_rolling_unweighted_shadow_prob_over" not in out.columns:
        out["tb_rolling_unweighted_shadow_prob_over"] = np.nan
    if "tb_rolling_unweighted_shadow_prob_under" not in out.columns:
        out["tb_rolling_unweighted_shadow_prob_under"] = np.nan

    for col in [
        "game_id",
        "player_id",
        "line",
        "production_prob_over",
        "tb_rolling_balanced_shadow_prob_over",
        "tb_rolling_unweighted_shadow_prob_over",
    ]:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    out["prop_type"] = out["prop_type"].astype(str).str.lower()
    out = out[out["prop_type"].eq(PROP_TYPE)].copy()
    out["row_key"] = (
        out["slate_date"].astype(str)
        + "|"
        + out["game_id"].astype("Int64").astype(str)
        + "|"
        + out["player_id"].astype("Int64").astype(str)
        + "|"
        + out["prop_type"].astype(str)
        + "|"
        + out["line"].astype(str)
    )
    out = out.drop_duplicates("row_key", keep="last").copy()
    out, recomputed_rows = fill_missing_unweighted_shadow(out, training_dataset, train_through)
    return out, recomputed_rows


def load_reconcile_outcomes(reconcile_root: Path, dates: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for day in dates:
        path = reconcile_root / day / "reconcile_rows.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path, low_memory=False)
        df = df[df["prop_type"].astype(str).str.lower().eq(PROP_TYPE)].copy()
        if df.empty:
            continue
        date_col = "slate_date" if "slate_date" in df.columns else "game_date"
        df["slate_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        for col in ["game_id", "player_id", "line"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        cols = ["slate_date", "game_id", "player_id", "prop_type", "line"]
        keep = cols + [c for c in ["actual_over_outcome", "actual_value"] if c in df.columns]
        frames.append(df[keep].drop_duplicates(cols, keep="first"))
    if not frames:
        return pd.DataFrame(columns=["slate_date", "game_id", "player_id", "prop_type", "line", "actual_over_outcome", "actual_value"])
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out.drop_duplicates(["slate_date", "game_id", "player_id", "prop_type", "line"], keep="first")


def join_outcomes(shadow: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    if shadow.empty:
        return shadow
    cols = ["slate_date", "game_id", "player_id", "prop_type", "line"]
    joined = shadow.merge(outcomes, on=cols, how="left", validate="one_to_one")
    joined["resolved"] = joined["actual_over_outcome"].astype(str).str.lower().isin({"win", "loss"})
    joined["y_over"] = np.where(
        joined["resolved"],
        joined["actual_over_outcome"].astype(str).str.lower().eq("win").astype(int),
        np.nan,
    )
    joined["line_bucket"] = joined["line"].map(line_bucket)
    joined["production_prob_bucket"] = joined["production_prob_over"].map(prob_bucket)
    joined["production_pick_side"] = np.where(joined["production_prob_over"] >= 0.5, "over", "under")
    for model, pred_col, side_col in MODEL_SPECS:
        if model == "production":
            continue
        joined[f"{model}_prob_bucket"] = joined[pred_col].map(prob_bucket)
        joined[side_col] = pd.Series(pd.NA, index=joined.index, dtype="object")
        joined.loc[joined[pred_col].notna(), side_col] = np.where(
            joined.loc[joined[pred_col].notna(), pred_col] >= 0.5,
            "over",
            "under",
        )
        joined[f"{model}_side_changed"] = (
            joined[pred_col].notna()
            & joined["production_pick_side"].astype("object").fillna("__missing__").ne(
                joined[side_col].astype("object").fillna("__missing__")
            )
        )
        joined.loc[joined[pred_col].isna(), f"{model}_side_changed"] = False
        joined[f"{model}_side_change_direction"] = np.select(
            [
                joined["production_pick_side"].eq("under") & joined[side_col].eq("over"),
                joined["production_pick_side"].eq("over") & joined[side_col].eq("under"),
            ],
            ["production_under_to_shadow_over", "production_over_to_shadow_under"],
            default=pd.Series(
                np.where(joined[pred_col].notna(), "same_side", "probability_missing"),
                index=joined.index,
                dtype="object",
            ),
        )
    joined["shadow_prob_bucket"] = joined["tb_rolling_balanced_shadow_prob_over"].map(prob_bucket)
    joined["tb_rolling_shadow_pick_side"] = joined["tb_rolling_balanced_shadow_pick_side"]
    joined["side_changed"] = joined["tb_rolling_balanced_shadow_side_changed"]
    return joined


def build_summary_tables(joined: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    overall_rows = [metric_row(model, joined, pred_col) for model, pred_col, _ in MODEL_SPECS]
    overall = pd.DataFrame(overall_rows)

    breakdown_rows: list[dict[str, Any]] = []
    side_change_rows: list[dict[str, Any]] = []
    for model, pred_col, side_col in MODEL_SPECS:
        for breakdown, series in [
            ("line_bucket", joined["line_bucket"]),
            ("predicted_side", joined[side_col]),
            (
                "side_changed",
                joined.get(f"{model}_side_changed", pd.Series(False, index=joined.index)).map({True: "side_changed", False: "same_side"})
                if model != "production"
                else pd.Series("n/a", index=joined.index),
            ),
        ]:
            tmp = joined.copy()
            tmp["_bucket"] = series
            for bucket, group in tmp.groupby("_bucket", dropna=False):
                row = metric_row(model, group, pred_col, subset=f"{breakdown}:{bucket}")
                row.update({"breakdown": breakdown, "bucket": bucket})
                breakdown_rows.append(row)
        if model != "production":
            tmp = joined.copy()
            tmp["_direction"] = tmp[f"{model}_side_change_direction"]
            for direction, group in tmp.groupby("_direction", dropna=False):
                row = metric_row(model, group, pred_col, subset=f"side_change_direction:{direction}")
                prod_row = metric_row("production", group, "production_prob_over", subset=f"side_change_direction:{direction}")
                row.update(
                    {
                        "comparison_model": model,
                        "side_change_direction": direction,
                        "production_rows": prod_row["rows"],
                        "production_brier": prod_row["brier"],
                        "production_log_loss": prod_row["log_loss"],
                        "production_auc": prod_row["auc"],
                        "production_avg_prob": prod_row["avg_prob"],
                        "production_overconfidence_gap": prod_row["overconfidence_gap"],
                    }
                )
                side_change_rows.append(row)

    calibration_rows: list[dict[str, Any]] = []
    for model, pred_col, bucket_col in [
        ("production", "production_prob_over", "production_prob_bucket"),
        ("tb_rolling_balanced_shadow", "tb_rolling_balanced_shadow_prob_over", "tb_rolling_balanced_shadow_prob_bucket"),
        ("tb_rolling_unweighted_shadow", "tb_rolling_unweighted_shadow_prob_over", "tb_rolling_unweighted_shadow_prob_bucket"),
    ]:
        for bucket, group in joined.groupby(bucket_col, dropna=False):
            row = metric_row(model, group, pred_col, subset=f"probability_bucket:{bucket}")
            row.update({"probability_bucket": bucket})
            calibration_rows.append(row)

    by_date_rows: list[dict[str, Any]] = []
    for day, group in joined.groupby("slate_date", dropna=False):
        base = {
            "slate_date": day,
            "rows_scored": int(len(group)),
            "rows_with_outcomes": int(group["resolved"].sum()),
            "balanced_side_changed_rows": int(group["tb_rolling_balanced_shadow_side_changed"].sum()) if "tb_rolling_balanced_shadow_side_changed" in group else 0,
            "balanced_side_changed_rate": float(group["tb_rolling_balanced_shadow_side_changed"].mean()) if len(group) and "tb_rolling_balanced_shadow_side_changed" in group else None,
            "unweighted_side_changed_rows": int(group["tb_rolling_unweighted_shadow_side_changed"].sum()) if "tb_rolling_unweighted_shadow_side_changed" in group else 0,
            "unweighted_side_changed_rate": float(group["tb_rolling_unweighted_shadow_side_changed"].mean()) if len(group) and "tb_rolling_unweighted_shadow_side_changed" in group else None,
            "avg_balanced_probability_delta_over": float((group["tb_rolling_balanced_shadow_prob_over"] - group["production_prob_over"]).mean()) if len(group) else None,
            "avg_unweighted_probability_delta_over": float((group["tb_rolling_unweighted_shadow_prob_over"] - group["production_prob_over"]).mean()) if len(group) else None,
        }
        for model, pred_col, _ in MODEL_SPECS:
            m = metric_row(model, group, pred_col)
            for key in ["brier", "log_loss", "auc", "avg_prob", "actual_over_rate", "overconfidence_gap"]:
                base[f"{model}_{key}"] = m[key]
        by_date_rows.append(base)
    return overall, pd.DataFrame(breakdown_rows), pd.DataFrame(calibration_rows), pd.DataFrame(by_date_rows), pd.DataFrame(side_change_rows)


def write_report(summary: dict[str, Any], overall: pd.DataFrame, by_date: pd.DataFrame, path: Path) -> None:
    lines = [
        "# Total Bases Shadow Evaluation Tracker",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "## Status",
        "",
        f"- Shadow dates scanned: `{summary['shadow_dates_scanned']}`",
        f"- Rows scored: `{summary['rows_scored']}`",
        f"- Rows with outcomes: `{summary['rows_with_outcomes']}`",
        f"- Side changed rows: `{summary['side_changed_rows']}`",
        "",
        "Balanced shadow is research-only and not promotion-ready. Unweighted shadow is also research-only pending a larger live sample.",
        "",
        "## Overall",
        "",
        "```csv",
        overall.to_csv(index=False).strip(),
        "```",
        "",
        "## By Date",
        "",
        "```csv",
        by_date.to_csv(index=False).strip(),
        "```",
        "",
        "No production predictions, selectors, thresholds, uploads, grading, or matching were changed.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate existing total_bases shadow score files against resolved outcomes.")
    ap.add_argument("--shadow-root", default=str(DEFAULT_SHADOW_ROOT))
    ap.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--training-dataset", default=str(DEFAULT_TRAINING_DATASET))
    ap.add_argument("--train-through", default="2026-06-14")
    args = ap.parse_args()

    shadow_root = Path(args.shadow_root)
    reconcile_root = Path(args.reconcile_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    shadow, unweighted_recomputed_rows = load_shadow_scores(shadow_root, Path(args.training_dataset), args.train_through)
    dates = sorted(d for d in shadow.get("slate_date", pd.Series(dtype=str)).dropna().astype(str).unique() if d and d != "NaT")
    outcomes = load_reconcile_outcomes(reconcile_root, dates)
    joined = join_outcomes(shadow, outcomes)
    overall, breakdowns, calibration, by_date, side_change = (
        build_summary_tables(joined)
        if not joined.empty
        else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    )

    rows_scored = int(len(joined))
    rows_with_outcomes = int(joined["resolved"].sum()) if "resolved" in joined.columns else 0
    side_changed_rows = int(joined["tb_rolling_balanced_shadow_side_changed"].sum()) if "tb_rolling_balanced_shadow_side_changed" in joined.columns else 0
    unweighted_side_changed_rows = int(joined["tb_rolling_unweighted_shadow_side_changed"].sum()) if "tb_rolling_unweighted_shadow_side_changed" in joined.columns else 0
    summary = {
        "generated_at": now_iso(),
        "shadow_root": rel(shadow_root),
        "reconcile_root": rel(reconcile_root),
        "shadow_dates_scanned": dates,
        "rows_scored": rows_scored,
        "rows_with_outcomes": rows_with_outcomes,
        "outcome_coverage": float(rows_with_outcomes / rows_scored) if rows_scored else None,
        "side_changed_rows": side_changed_rows,
        "side_changed_rate": float(side_changed_rows / rows_scored) if rows_scored else None,
        "tb_rolling_balanced_side_changed_rows": side_changed_rows,
        "tb_rolling_balanced_side_changed_rate": float(side_changed_rows / rows_scored) if rows_scored else None,
        "tb_rolling_unweighted_side_changed_rows": unweighted_side_changed_rows,
        "tb_rolling_unweighted_side_changed_rate": float(unweighted_side_changed_rows / rows_scored) if rows_scored else None,
        "tb_rolling_unweighted_recomputed_rows": unweighted_recomputed_rows,
        "tb_rolling_unweighted_recomputed_source": "evaluation_recomputed_from_score_rows_when_missing",
        "cumulative_metrics": overall.to_dict(orient="records") if not overall.empty else [],
        "side_change_diagnostics": side_change.to_dict(orient="records") if not side_change.empty else [],
        "interpretation_note": "Balanced shadow is not promotion-ready; unweighted shadow is research-only pending larger live sample.",
        "production_outputs_changed": False,
        "final_upload_outputs_changed": False,
        "outputs": {
            "cumulative_rows_csv": rel(out_dir / "total_bases_shadow_evaluation_rows.csv"),
            "cumulative_summary_csv": rel(out_dir / "total_bases_shadow_evaluation_cumulative.csv"),
            "breakdowns_csv": rel(out_dir / "total_bases_shadow_evaluation_breakdowns.csv"),
            "calibration_bins_csv": rel(out_dir / "total_bases_shadow_evaluation_calibration_bins.csv"),
            "by_date_csv": rel(out_dir / "total_bases_shadow_evaluation_by_date.csv"),
            "side_change_diagnostics_csv": rel(out_dir / "total_bases_shadow_evaluation_side_change_diagnostics.csv"),
            "summary_json": rel(out_dir / "total_bases_shadow_evaluation_summary.json"),
            "report_md": rel(out_dir / "total_bases_shadow_evaluation_report.md"),
        },
    }

    joined.to_csv(out_dir / "total_bases_shadow_evaluation_rows.csv", index=False)
    overall.to_csv(out_dir / "total_bases_shadow_evaluation_cumulative.csv", index=False)
    breakdowns.to_csv(out_dir / "total_bases_shadow_evaluation_breakdowns.csv", index=False)
    calibration.to_csv(out_dir / "total_bases_shadow_evaluation_calibration_bins.csv", index=False)
    by_date.to_csv(out_dir / "total_bases_shadow_evaluation_by_date.csv", index=False)
    side_change.to_csv(out_dir / "total_bases_shadow_evaluation_side_change_diagnostics.csv", index=False)
    (out_dir / "total_bases_shadow_evaluation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_report(summary, overall, by_date, out_dir / "total_bases_shadow_evaluation_report.md")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
