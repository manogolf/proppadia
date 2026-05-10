#!/usr/bin/env python3
"""Run an isolated MLB over-threshold v2 line-feature experiment.

This is intentionally reporting-only:
- it does not write production model artifacts
- it does not mutate model metadata
- it does not deploy anything

The experiment keeps the legacy prediction-centered target:
    y_over = 1 if actual_value > line else 0

The v2 treatment is adding line/prop_value as fit-time features, then comparing
against the existing v1 probabilities already present in outcome-backed
reconcile rows.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from backend.mlb import model_trainer as trainer


DEFAULT_PROPS = "hits,total_bases,strikeouts_pitching"
DEFAULT_TRAIN_CSV = "tmp/mlb_base_vs_market_rows_anybook_full.csv"
DEFAULT_HOLDOUT_GLOB = "artifacts/analysis/mlb/execution_vs_model/20??-??-??/reconcile_rows.csv"
DEFAULT_OUT_DIR = "backend/mlb/exports/model_diagnostics"
PROB_BUCKETS = [0.0, 0.5, 0.55, 0.6, 0.65, 0.7, 1.000001]
PROB_LABELS = ["<0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70+"]


@dataclass(frozen=True)
class ExperimentPaths:
    out_dir: Path

    @property
    def report_md(self) -> Path:
        return self.out_dir / "model_v2_experiment_report.md"

    @property
    def line_coverage_csv(self) -> Path:
        return self.out_dir / "model_v2_line_coverage.csv"

    @property
    def summary_csv(self) -> Path:
        return self.out_dir / "model_v2_summary.csv"

    @property
    def calibration_bucket_csv(self) -> Path:
        return self.out_dir / "model_v2_calibration_by_probability_bucket.csv"

    @property
    def calibration_side_csv(self) -> Path:
        return self.out_dir / "model_v2_calibration_by_prop_side.csv"

    @property
    def side_roi_csv(self) -> Path:
        return self.out_dir / "model_v2_side_roi.csv"


def _props(raw: str) -> list[str]:
    return [p.strip().lower() for p in str(raw or "").split(",") if p.strip()]


def _find_line_column(df: pd.DataFrame) -> str | None:
    for col in ("line", "prop_value", "market_line"):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce")
            if vals.notna().any():
                return col
    return None


def _normalize_line_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    out = df.copy()
    canonical = _find_line_column(out)
    if canonical is None:
        out["line"] = np.nan
        out["prop_value"] = np.nan
        return out, None
    vals = pd.to_numeric(out[canonical], errors="coerce")
    out["line"] = vals
    out["prop_value"] = vals
    return out, canonical


def _actual_over_y(df: pd.DataFrame) -> pd.Series:
    if "actual_over_outcome" in df.columns:
        s = df["actual_over_outcome"].astype(str).str.strip().str.lower()
        y = pd.Series(np.nan, index=df.index, dtype="float64")
        y.loc[s.eq("win")] = 1.0
        y.loc[s.eq("loss")] = 0.0
        return y
    if {"actual_value", "line"}.issubset(df.columns):
        actual = pd.to_numeric(df["actual_value"], errors="coerce")
        line = pd.to_numeric(df["line"], errors="coerce")
        return (actual > line).astype("float64").where(actual.notna() & line.notna())
    return pd.Series(np.nan, index=df.index, dtype="float64")


def _american_profit(price: Any, won: bool) -> float:
    if not won:
        return -1.0
    try:
        px = float(price)
    except Exception:
        return 0.0
    if not math.isfinite(px) or px == 0:
        return 0.0
    return px / 100.0 if px > 0 else 100.0 / abs(px)


def _metric_safe_auc(y: pd.Series, p: pd.Series) -> float | None:
    mask = y.notna() & p.notna()
    if int(mask.sum()) < 2 or y.loc[mask].nunique() < 2:
        return None
    try:
        return float(roc_auc_score(y.loc[mask].astype(int), p.loc[mask].astype(float)))
    except Exception:
        return None


def _metric_brier(y: pd.Series, p: pd.Series) -> float | None:
    mask = y.notna() & p.notna()
    if int(mask.sum()) == 0:
        return None
    return float(brier_score_loss(y.loc[mask].astype(int), p.loc[mask].astype(float).clip(1e-6, 1 - 1e-6)))


def _metric_logloss(y: pd.Series, p: pd.Series) -> float | None:
    mask = y.notna() & p.notna()
    if int(mask.sum()) == 0 or y.loc[mask].nunique() < 2:
        return None
    return float(log_loss(y.loc[mask].astype(int), p.loc[mask].astype(float).clip(1e-6, 1 - 1e-6), labels=[0, 1]))


def _feature_list(prop: str) -> list[str]:
    spec_all = trainer._load_feature_spec()  # noqa: SLF001 - isolated experiment using established local loader.
    spec = spec_all.get(prop) or {}
    cols = (
        spec.get("random_forest")
        or spec.get("rf")
        or spec.get("logistic_regression")
        or spec.get("lr")
        or spec.get("features")
        or []
    )
    out = [str(c) for c in cols if str(c)]
    for required in ("line", "prop_value"):
        if required not in out:
            out.append(required)
    return out


def _load_training_rows(path: Path, props: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing train csv: {path}")
    df = pd.read_csv(path)
    df["prop_type"] = df.get("prop_type", "").astype(str).str.strip().str.lower()
    df = df[df["prop_type"].isin(props)].copy()
    df, _ = _normalize_line_columns(df)
    df["game_date"] = pd.to_datetime(df.get("game_date"), errors="coerce")
    df["y"] = _actual_over_y(df)
    df = df[df["y"].isin([0.0, 1.0]) & df["line"].notna()].copy()
    if {"player_id", "game_id", "line", "prop_type"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["prop_type", "player_id", "game_id", "line"], keep="first")
    return df


def _load_training_rows_raw_for_coverage(path: Path, props: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing train csv: {path}")
    df = pd.read_csv(path)
    df["prop_type"] = df.get("prop_type", "").astype(str).str.strip().str.lower()
    return df[df["prop_type"].isin(props)].copy()


def _load_holdout_rows(pattern: str, props: list[str], min_date: str | None, max_date: str | None) -> pd.DataFrame:
    frames = []
    for path in sorted(Path().glob(pattern)):
        try:
            part = pd.read_csv(path)
        except Exception:
            continue
        if part.empty or "prop_type" not in part.columns:
            continue
        part["source_file"] = str(path)
        frames.append(part)
    if not frames:
        raise FileNotFoundError(f"no holdout reconcile rows matched: {pattern}")
    df = pd.concat(frames, ignore_index=True)
    df["prop_type"] = df.get("prop_type", "").astype(str).str.strip().str.lower()
    df = df[df["prop_type"].isin(props)].copy()
    df, _ = _normalize_line_columns(df)
    df["game_date"] = pd.to_datetime(df.get("game_date"), errors="coerce")
    if min_date:
        df = df[df["game_date"] >= pd.Timestamp(min_date)]
    if max_date:
        df = df[df["game_date"] <= pd.Timestamp(max_date)]
    df["y"] = _actual_over_y(df)
    df = df[df["y"].isin([0.0, 1.0]) & df["line"].notna()].copy()
    if {"player_id", "game_id", "line", "prop_type"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["prop_type", "player_id", "game_id", "line"], keep="first")
    return df


def _load_holdout_rows_raw_for_coverage(
    pattern: str,
    props: list[str],
    min_date: str | None,
    max_date: str | None,
) -> pd.DataFrame:
    frames = []
    for path in sorted(Path().glob(pattern)):
        try:
            part = pd.read_csv(path)
        except Exception:
            continue
        if part.empty or "prop_type" not in part.columns:
            continue
        part["source_file"] = str(path)
        frames.append(part)
    if not frames:
        raise FileNotFoundError(f"no holdout reconcile rows matched: {pattern}")
    df = pd.concat(frames, ignore_index=True)
    df["prop_type"] = df.get("prop_type", "").astype(str).str.strip().str.lower()
    df = df[df["prop_type"].isin(props)].copy()
    df["game_date"] = pd.to_datetime(df.get("game_date"), errors="coerce")
    if min_date:
        df = df[df["game_date"] >= pd.Timestamp(min_date)]
    if max_date:
        df = df[df["game_date"] <= pd.Timestamp(max_date)]
    return df


def _line_coverage(df: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for prop, g in df.groupby("prop_type", dropna=False):
        cols_present = [c for c in ("line", "prop_value", "market_line") if c in g.columns]
        canonical = _find_line_column(g)
        rows.append(
            {
                "source": source_name,
                "prop_type": prop,
                "rows": int(len(g)),
                "line_columns_present": ",".join(cols_present),
                "canonical_line_column": canonical or "",
                "line_null_rate": float(pd.to_numeric(g.get("line"), errors="coerce").isna().mean()),
                "prop_value_null_rate": (
                    float(pd.to_numeric(g.get("prop_value"), errors="coerce").isna().mean())
                    if "prop_value" in g.columns
                    else None
                ),
                "market_line_null_rate": (
                    float(pd.to_numeric(g.get("market_line"), errors="coerce").isna().mean())
                    if "market_line" in g.columns
                    else None
                ),
                "prop_value_normalized_from_line_for_v2": "prop_value" not in g.columns and "line" in g.columns,
                "min_line": float(pd.to_numeric(g.get("line"), errors="coerce").min()),
                "max_line": float(pd.to_numeric(g.get("line"), errors="coerce").max()),
            }
        )
    return pd.DataFrame(rows)


def _hydrate(sb: Any, df: pd.DataFrame, feat_cols: list[str], prop: str) -> pd.DataFrame:
    out = trainer._add_time_features(df)  # noqa: SLF001
    out = trainer._merge_derived_features(sb, out, feat_cols)  # noqa: SLF001
    if prop == "total_bases":
        out = trainer._add_total_bases_component_features(out, quiet=True)  # noqa: SLF001
    out, _ = _normalize_line_columns(out)
    for col in ("line", "prop_value"):
        if col not in out.columns:
            out[col] = np.nan
    return out


def _prep_xy(df: pd.DataFrame, feat_cols: list[str]) -> tuple[pd.DataFrame, pd.Series, np.ndarray, list[str], list[str]]:
    work = df.copy()
    for col in feat_cols:
        if col not in work.columns:
            work[col] = np.nan
    numeric_candidates = [c for c in feat_cols if c not in trainer.ALWAYS_CATEGORICAL_FEATURES and c in work.columns]
    for col in numeric_candidates:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    feat_cols = [c for c in feat_cols if c in work.columns and not (c in numeric_candidates and work[c].isna().all())]
    num_used = [c for c in feat_cols if c in work.columns and pd.api.types.is_numeric_dtype(work[c]) and c not in trainer.ALWAYS_CATEGORICAL_FEATURES]
    cat_used = [c for c in feat_cols if c in work.columns and (not pd.api.types.is_numeric_dtype(work[c]) or c in trainer.ALWAYS_CATEGORICAL_FEATURES)]
    cat_used = [
        c
        for c in cat_used
        if not work[c].replace("", np.nan).isna().all()
    ]
    for col in list(num_used):
        if work[col].isna().any():
            mcol = f"isna__{col}"
            work[mcol] = work[col].isna().astype(int)
            num_used.append(mcol)
    cols_used = num_used + cat_used
    y = pd.to_numeric(work["y"], errors="coerce").astype(int)
    weights = np.ones(len(work), dtype="float64")
    return work[cols_used], y, weights, num_used, cat_used


def _fit_v2(train: pd.DataFrame, prop: str, feat_cols: list[str]) -> tuple[Any, Any, dict[str, Any]]:
    train = train.sort_values("game_date").copy()
    split = int(len(train) * 0.8)
    tr = train.iloc[:split].copy()
    val = train.iloc[split:].copy()
    if len(tr) < 100 or len(val) < 50:
        raise RuntimeError(f"{prop}: insufficient split rows train={len(tr)} val={len(val)}")
    x_tr, y_tr, w_tr, num_used, cat_used = _prep_xy(tr, feat_cols)
    x_val, y_val, _w_val, _, _ = _prep_xy(val, feat_cols)
    for col in x_tr.columns:
        if col not in x_val.columns:
            x_val[col] = np.nan
    x_val = x_val[x_tr.columns]
    lr, rf = trainer.build_pipeline(num_used, cat_used)
    lr.fit(x_tr, y_tr, clf__sample_weight=w_tr)
    rf.fit(x_tr, y_tr, clf__sample_weight=w_tr)
    p_lr = pd.Series(lr.predict_proba(x_val)[:, 1], index=x_val.index)
    p_rf = pd.Series(rf.predict_proba(x_val)[:, 1], index=x_val.index)
    auc_lr = _metric_safe_auc(y_val, p_lr)
    auc_rf = _metric_safe_auc(y_val, p_rf)
    w_lr = max((auc_lr or 0.5) - 0.5, 0.0)
    w_rf = max((auc_rf or 0.5) - 0.5, 0.0)
    if (w_lr + w_rf) > 0:
        blend = ((p_lr * w_lr) + (p_rf * w_rf)) / (w_lr + w_rf)
    else:
        blend = (p_lr + p_rf) / 2.0
    best_thr = 0.5
    best_acc = -1.0
    for thr in [round(x, 2) for x in np.arange(0.35, 0.66, 0.01)]:
        pred = (blend >= thr).astype(int)
        acc = float((pred.to_numpy() == y_val.to_numpy()).mean())
        if acc > best_acc or (abs(acc - best_acc) < 1e-12 and abs(thr - 0.5) < abs(best_thr - 0.5)):
            best_thr = thr
            best_acc = acc
    meta = {
        "prop_type": prop,
        "train_rows": int(len(tr)),
        "validation_rows": int(len(val)),
        "input_columns": list(x_tr.columns),
        "numeric_features": num_used,
        "categorical_features": cat_used,
        "auc_lr_internal": auc_lr,
        "auc_rf_internal": auc_rf,
        "blend_weight_lr": w_lr,
        "blend_weight_rf": w_rf,
        "decision_threshold": best_thr,
        "validation_accuracy_at_threshold": best_acc,
    }
    return lr, rf, meta


def _predict_v2(lr: Any, rf: Any, meta: dict[str, Any], holdout: pd.DataFrame, feat_cols: list[str]) -> pd.Series:
    x, _y, _w, _num, _cat = _prep_xy(holdout, feat_cols)
    for col in meta["input_columns"]:
        if col not in x.columns:
            x[col] = np.nan
    x = x[meta["input_columns"]]
    p_lr = pd.Series(lr.predict_proba(x)[:, 1], index=holdout.index)
    p_rf = pd.Series(rf.predict_proba(x)[:, 1], index=holdout.index)
    w_lr = float(meta.get("blend_weight_lr") or 0.0)
    w_rf = float(meta.get("blend_weight_rf") or 0.0)
    if (w_lr + w_rf) > 0:
        return ((p_lr * w_lr) + (p_rf * w_rf)) / (w_lr + w_rf)
    return (p_lr + p_rf) / 2.0


def _model_eval_rows(df: pd.DataFrame, *, prop: str, version: str, p_over_col: str, threshold: float | None) -> pd.DataFrame:
    out = df.copy()
    out["model_version"] = version
    out["prob_over_eval"] = pd.to_numeric(out[p_over_col], errors="coerce").clip(1e-6, 1 - 1e-6)
    if version == "v1":
        side = out.get("model_pick_side", "").astype(str).str.lower()
        side = side.where(side.isin(["over", "under"]), np.where(out["prob_over_eval"] >= 0.5, "over", "under"))
    else:
        side = np.where(out["prob_over_eval"] >= float(threshold or 0.5), "over", "under")
    out["side_norm"] = side
    out["side_prob"] = np.where(out["side_norm"].eq("over"), out["prob_over_eval"], 1.0 - out["prob_over_eval"])
    out["actual_win"] = np.where(out["side_norm"].eq("over"), out["y"].astype(int), 1 - out["y"].astype(int))
    out["price_taken"] = np.where(
        out["side_norm"].eq("over"),
        pd.to_numeric(out.get("price_over_american"), errors="coerce"),
        pd.to_numeric(out.get("price_under_american"), errors="coerce"),
    )
    out["pnl_1u"] = [
        _american_profit(price, bool(won)) for price, won in zip(out["price_taken"], out["actual_win"])
    ]
    out["prop_type"] = prop
    return out


def _summary_metrics(rows: pd.DataFrame, *, prob_col: str = "prob_over_eval", y_col: str = "y") -> dict[str, Any]:
    y = pd.to_numeric(rows[y_col], errors="coerce")
    p = pd.to_numeric(rows[prob_col], errors="coerce")
    return {
        "rows": int((y.notna() & p.notna()).sum()),
        "auc": _metric_safe_auc(y, p),
        "brier": _metric_brier(y, p),
        "log_loss": _metric_logloss(y, p),
    }


def _calibration_by_bucket(eval_rows: pd.DataFrame) -> pd.DataFrame:
    work = eval_rows.copy()
    work["prob_bucket"] = pd.cut(work["side_prob"], bins=PROB_BUCKETS, labels=PROB_LABELS, right=False)
    rows = []
    for keys, g in work.groupby(["model_version", "prop_type", "side_norm", "prob_bucket"], dropna=False, observed=False):
        version, prop, side, bucket = keys
        if g.empty:
            continue
        rows.append(
            {
                "model_version": version,
                "prop_type": prop,
                "side": side,
                "prob_bucket": bucket,
                "bets": int(len(g)),
                "actual_win_rate": float(pd.to_numeric(g["actual_win"], errors="coerce").mean()),
                "expected_win_rate": float(pd.to_numeric(g["side_prob"], errors="coerce").mean()),
                "actual_minus_expected": float(pd.to_numeric(g["actual_win"], errors="coerce").mean() - pd.to_numeric(g["side_prob"], errors="coerce").mean()),
                "profit_units": float(pd.to_numeric(g["pnl_1u"], errors="coerce").sum()),
                "roi": float(pd.to_numeric(g["pnl_1u"], errors="coerce").sum() / max(1, len(g))),
            }
        )
    return pd.DataFrame(rows)


def _calibration_by_side(eval_rows: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, g in eval_rows.groupby(["model_version", "prop_type", "side_norm"], dropna=False):
        version, prop, side = keys
        rows.append(
            {
                "model_version": version,
                "prop_type": prop,
                "side": side,
                "bets": int(len(g)),
                "actual_win_rate": float(pd.to_numeric(g["actual_win"], errors="coerce").mean()),
                "expected_win_rate": float(pd.to_numeric(g["side_prob"], errors="coerce").mean()),
                "actual_minus_expected": float(pd.to_numeric(g["actual_win"], errors="coerce").mean() - pd.to_numeric(g["side_prob"], errors="coerce").mean()),
                "profit_units": float(pd.to_numeric(g["pnl_1u"], errors="coerce").sum()),
                "roi": float(pd.to_numeric(g["pnl_1u"], errors="coerce").sum() / max(1, len(g))),
            }
        )
    return pd.DataFrame(rows)


def _side_roi(eval_rows: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, g in eval_rows.groupby(["model_version", "side_norm"], dropna=False):
        version, side = keys
        wins = int(pd.to_numeric(g["actual_win"], errors="coerce").sum())
        rows.append(
            {
                "model_version": version,
                "side": side,
                "bets": int(len(g)),
                "wins": wins,
                "losses": int(len(g) - wins),
                "win_rate": float(wins / max(1, len(g))),
                "profit_units": float(pd.to_numeric(g["pnl_1u"], errors="coerce").sum()),
                "roi": float(pd.to_numeric(g["pnl_1u"], errors="coerce").sum() / max(1, len(g))),
            }
        )
    return pd.DataFrame(rows)


def _fmt_pct(v: Any) -> str:
    try:
        if pd.isna(v):
            return ""
        return f"{float(v) * 100:.2f}%"
    except Exception:
        return ""


def _md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    clean = df.fillna("").copy()
    cols = list(clean.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in clean.iterrows():
        lines.append("| " + " | ".join(str(row[c]).replace("|", "\\|") for c in cols) + " |")
    return "\n".join(lines)


def _write_report(paths: ExperimentPaths, *, props: list[str], train_csv: Path, holdout_pattern: str, line_cov: pd.DataFrame, summary: pd.DataFrame, side_roi: pd.DataFrame, audit_only: bool) -> None:
    lines = [
        "# MLB Model V2 Line-Feature Experiment",
        "",
        "Reporting-only experiment. No production model artifacts were written and nothing was deployed.",
        "",
        "## Design",
        "",
        "- Objective: keep the prediction-centered target `P(actual > line)`.",
        "- Required v2 treatment: include both `line` and `prop_value` as fit-time features.",
        "- Training source: `" + str(train_csv) + "`.",
        "- Holdout source: `" + str(holdout_pattern) + "`.",
        "- Props: `" + ",".join(props) + "`.",
        "- V1 baseline: existing `model_prob_over` and `model_pick_side` in outcome-backed reconcile rows.",
        "- V2 candidate: isolated LR/RF blend trained from the legacy feature set plus `line` and `prop_value`.",
        "",
        "## Canonical Line Column",
        "",
        "`line` is treated as canonical when present. `prop_value` is normalized to the same numeric value for fit-time compatibility; `market_line` is only a fallback if neither is present.",
        "",
        "## Line Coverage",
        "",
        _md_table(line_cov),
        "",
    ]
    if audit_only:
        lines.extend(
            [
                "## Training Status",
                "",
                "Audit-only mode was used, so v2 models were not trained.",
                "",
                "Run the experiment with DB access to hydrate legacy derived features:",
                "",
                "```bash",
                "source backend/.env",
                ".venv/bin/python backend/mlb/scripts/run_mlb_model_v2_line_feature_experiment.py",
                "```",
                "",
            ]
        )
    else:
        show_summary = summary.copy()
        for col in ("auc", "brier", "log_loss", "roi", "over_roi", "under_roi", "threshold"):
            if col in show_summary.columns:
                show_summary[col] = show_summary[col].map(lambda x: "" if pd.isna(x) else f"{float(x):.4f}")
        lines.extend(
            [
                "## Holdout Summary",
                "",
                _md_table(show_summary),
                "",
                "## Over vs Under ROI",
                "",
            ]
        )
        show_side = side_roi.copy()
        for col in ("win_rate", "roi"):
            if col in show_side.columns:
                show_side[col] = show_side[col].map(_fmt_pct)
        if "profit_units" in show_side.columns:
            show_side["profit_units"] = show_side["profit_units"].map(lambda x: f"{float(x):.2f}" if not pd.isna(x) else "")
        lines.append(_md_table(show_side))
        lines.append("")
    lines.extend(
        [
            "## Output CSVs",
            "",
            f"- `{paths.line_coverage_csv}`",
            f"- `{paths.summary_csv}`",
            f"- `{paths.calibration_bucket_csv}`",
            f"- `{paths.calibration_side_csv}`",
            f"- `{paths.side_roi_csv}`",
        ]
    )
    paths.report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run MLB v2 line-feature over-threshold experiment.")
    ap.add_argument("--props", default=DEFAULT_PROPS)
    ap.add_argument("--train-csv", default=DEFAULT_TRAIN_CSV)
    ap.add_argument("--holdout-glob", default=DEFAULT_HOLDOUT_GLOB)
    ap.add_argument("--holdout-from-date", default="2026-04-29")
    ap.add_argument("--holdout-to-date", default="")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--audit-only", action="store_true")
    args = ap.parse_args()

    props = _props(args.props)
    paths = ExperimentPaths(Path(args.out_dir))
    paths.out_dir.mkdir(parents=True, exist_ok=True)
    train_csv = Path(args.train_csv)

    train_raw_for_coverage = _load_training_rows_raw_for_coverage(train_csv, props)
    holdout_raw_for_coverage = _load_holdout_rows_raw_for_coverage(
        str(args.holdout_glob),
        props,
        args.holdout_from_date or None,
        args.holdout_to_date or None,
    )
    train_all = _load_training_rows(train_csv, props)
    holdout_all = _load_holdout_rows(
        str(args.holdout_glob),
        props,
        args.holdout_from_date or None,
        args.holdout_to_date or None,
    )
    line_cov = pd.concat(
        [
            _line_coverage(train_raw_for_coverage, source_name="train_csv"),
            _line_coverage(holdout_raw_for_coverage, source_name="holdout_reconcile_rows"),
        ],
        ignore_index=True,
    )
    line_cov.to_csv(paths.line_coverage_csv, index=False)

    if args.audit_only:
        pd.DataFrame().to_csv(paths.summary_csv, index=False)
        pd.DataFrame().to_csv(paths.calibration_bucket_csv, index=False)
        pd.DataFrame().to_csv(paths.calibration_side_csv, index=False)
        pd.DataFrame().to_csv(paths.side_roi_csv, index=False)
        _write_report(
            paths,
            props=props,
            train_csv=train_csv,
            holdout_pattern=str(args.holdout_glob),
            line_cov=line_cov,
            summary=pd.DataFrame(),
            side_roi=pd.DataFrame(),
            audit_only=True,
        )
        print(f"[v2-line-experiment] audit_only=1 report={paths.report_md}")
        return 0

    sb = trainer._supabase_client()  # noqa: SLF001
    if sb is None and not (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")):
        raise SystemExit(
            "DB credentials are required to hydrate legacy derived features. "
            "Run `source backend/.env` first, or rerun with --audit-only."
        )

    all_eval_rows: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for prop in props:
        feat_cols = _feature_list(prop)
        train_prop = train_all[train_all["prop_type"].eq(prop)].copy()
        holdout_prop = holdout_all[holdout_all["prop_type"].eq(prop)].copy()
        if train_prop.empty or holdout_prop.empty:
            summary_rows.append({"prop_type": prop, "model_version": "v2", "status": "skipped_missing_rows"})
            continue
        train_h = _hydrate(sb, train_prop, feat_cols, prop)
        holdout_h = _hydrate(sb, holdout_prop, feat_cols, prop)
        lr, rf, meta = _fit_v2(train_h, prop, feat_cols)
        holdout_h["v2_prob_over"] = _predict_v2(lr, rf, meta, holdout_h, feat_cols)

        v1 = _model_eval_rows(holdout_h, prop=prop, version="v1", p_over_col="model_prob_over", threshold=None)
        v2 = _model_eval_rows(
            holdout_h,
            prop=prop,
            version="v2_line_feature",
            p_over_col="v2_prob_over",
            threshold=float(meta["decision_threshold"]),
        )
        all_eval_rows.extend([v1, v2])

        for version, rows, pcol, thr in [
            ("v1", v1, "prob_over_eval", None),
            ("v2_line_feature", v2, "prob_over_eval", float(meta["decision_threshold"])),
        ]:
            m = _summary_metrics(rows, prob_col=pcol, y_col="y")
            over = rows[rows["side_norm"].eq("over")]
            under = rows[rows["side_norm"].eq("under")]
            summary_rows.append(
                {
                    "prop_type": prop,
                    "model_version": version,
                    "status": "ok",
                    "train_rows": int(len(train_h)),
                    "holdout_rows": int(len(rows)),
                    "threshold": thr,
                    "auc": m["auc"],
                    "brier": m["brier"],
                    "log_loss": m["log_loss"],
                    "profit_units": float(rows["pnl_1u"].sum()),
                    "roi": float(rows["pnl_1u"].sum() / max(1, len(rows))),
                    "over_bets": int(len(over)),
                    "over_roi": float(over["pnl_1u"].sum() / max(1, len(over))) if len(over) else None,
                    "under_bets": int(len(under)),
                    "under_roi": float(under["pnl_1u"].sum() / max(1, len(under))) if len(under) else None,
                    "internal_auc_lr": meta.get("auc_lr_internal") if version == "v2_line_feature" else None,
                    "internal_auc_rf": meta.get("auc_rf_internal") if version == "v2_line_feature" else None,
                    "input_columns_count": len(meta.get("input_columns") or []) if version == "v2_line_feature" else None,
                    "line_in_input_columns": ("line" in (meta.get("input_columns") or [])) if version == "v2_line_feature" else None,
                    "prop_value_in_input_columns": ("prop_value" in (meta.get("input_columns") or [])) if version == "v2_line_feature" else None,
                }
            )

    summary = pd.DataFrame(summary_rows)
    eval_rows = pd.concat(all_eval_rows, ignore_index=True) if all_eval_rows else pd.DataFrame()
    bucket = _calibration_by_bucket(eval_rows) if not eval_rows.empty else pd.DataFrame()
    side = _calibration_by_side(eval_rows) if not eval_rows.empty else pd.DataFrame()
    roi = _side_roi(eval_rows) if not eval_rows.empty else pd.DataFrame()

    summary.to_csv(paths.summary_csv, index=False)
    bucket.to_csv(paths.calibration_bucket_csv, index=False)
    side.to_csv(paths.calibration_side_csv, index=False)
    roi.to_csv(paths.side_roi_csv, index=False)
    _write_report(
        paths,
        props=props,
        train_csv=train_csv,
        holdout_pattern=str(args.holdout_glob),
        line_cov=line_cov,
        summary=summary,
        side_roi=roi,
        audit_only=False,
    )
    print(f"[v2-line-experiment] report={paths.report_md}")
    print(f"[v2-line-experiment] summary={paths.summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
