#!/usr/bin/env python3
"""Report MLB model calibration by probability bucket, prop, and side.

CSV-only reporting from outcome-backed full-slate reconcile rows.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_performance/calibration_surface.csv")
BUCKETS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, np.inf]
BUCKET_LABELS = ["0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _discover_reconcile_files(root: Path, from_date: str = "", to_date: str = "") -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        if pd.isna(pd.to_datetime(date, errors="coerce")):
            continue
        if from_date and date < from_date:
            continue
        if to_date and date > to_date:
            continue
        files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_rows(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
        "prop_type",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[calibration-surface] skip {path}: missing {missing}")
            continue
        df = df.copy()
        df["source_date"] = path.parent.name
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible reconcile_rows.csv files found.")
    return pd.concat(frames, ignore_index=True)


def _side_rows(rows: pd.DataFrame) -> pd.DataFrame:
    work = rows.copy()
    work["prop_type"] = work["prop_type"].map(lambda v: _clean(v).lower())
    pieces = []
    for side in ("over", "under"):
        prob_col = f"model_prob_{side}"
        outcome_col = f"actual_{side}_outcome"
        side_df = pd.DataFrame(
            {
                "prop_type": work["prop_type"],
                "side": side,
                "model_prob": pd.to_numeric(work[prob_col], errors="coerce"),
                "outcome": work[outcome_col].map(lambda v: _clean(v).lower()),
            }
        )
        side_df = side_df[side_df["outcome"].isin({"win", "loss"}) & side_df["model_prob"].notna()].copy()
        side_df["actual_win"] = side_df["outcome"].eq("win").astype(float)
        pieces.append(side_df)
    out = pd.concat(pieces, ignore_index=True)
    out = out[out["model_prob"].ge(0.50)].copy()
    out["prob_bucket"] = pd.cut(
        out["model_prob"],
        bins=BUCKETS,
        labels=BUCKET_LABELS,
        right=False,
        include_lowest=True,
    )
    out = out[out["prob_bucket"].notna()].copy()
    return out


def _log_loss(actual: pd.Series, prob: pd.Series) -> float:
    p = pd.to_numeric(prob, errors="coerce").clip(1e-6, 1 - 1e-6)
    y = pd.to_numeric(actual, errors="coerce")
    return float((-(y * np.log(p) + (1 - y) * np.log(1 - p))).mean())


def build_report(rows: pd.DataFrame) -> pd.DataFrame:
    sides = _side_rows(rows)
    if sides.empty:
        raise SystemExit("No resolved model_prob >= 0.50 side rows found.")

    records = []
    for (bucket, prop_type, side), group in sides.groupby(["prob_bucket", "prop_type", "side"], observed=True):
        bets = int(len(group))
        actual = float(group["actual_win"].mean())
        avg_model = float(group["model_prob"].mean())
        brier = float(((group["model_prob"] - group["actual_win"]) ** 2).mean())
        records.append(
            {
                "prob_bucket": str(bucket),
                "prop_type": prop_type,
                "side": side,
                "bets": bets,
                "actual_win_rate": actual,
                "avg_model_prob": avg_model,
                "calibration_error": actual - avg_model,
                "brier_score": brier,
                "log_loss": _log_loss(group["actual_win"], group["model_prob"]),
            }
        )

    out = pd.DataFrame(records).sort_values(["prop_type", "side", "prob_bucket"]).reset_index(drop=True)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report calibration surface by bucket, prop, and side.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    rows = _load_rows(paths)
    out = build_report(rows)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[calibration-surface] files={len(paths)} rows={len(out)} out_csv={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
