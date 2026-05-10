#!/usr/bin/env python3
"""Diagnose hits probability calibration failures.

This is a diagnostics-only report. It does not compute ROI or betting rules.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_probability_failure_surface.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_probability_failure_summary.md")

PROB_BINS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, np.inf]
PROB_LABELS = ["0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]


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


def _load_hits(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
        "prop_type",
        "line",
        "price_over_american",
        "price_under_american",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-prob-failure] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        if df.empty:
            continue
        df["source_date"] = path.parent.name
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible hits rows found.")
    return pd.concat(frames, ignore_index=True)


def _side_rows(rows: pd.DataFrame) -> pd.DataFrame:
    pieces = []
    for side in ("over", "under"):
        side_df = pd.DataFrame(
            {
                "source_date": rows["source_date"],
                "side": side,
                "line": pd.to_numeric(rows["line"], errors="coerce"),
                "price": pd.to_numeric(rows[f"price_{side}_american"], errors="coerce"),
                "model_prob": pd.to_numeric(rows[f"model_prob_{side}"], errors="coerce"),
                "outcome": rows[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
            }
        )
        pieces.append(side_df)
    out = pd.concat(pieces, ignore_index=True)
    out = out[
        out["outcome"].isin({"win", "loss"})
        & out["line"].notna()
        & out["price"].notna()
        & out["model_prob"].notna()
        & out["model_prob"].ge(0.50)
    ].copy()
    out["actual_win"] = out["outcome"].eq("win").astype(float)
    out["line_bucket"] = out["line"].map(lambda v: f"{float(v):g}")
    out["line_side"] = out["side"] + " " + out["line_bucket"]
    out["price_class"] = np.where(out["price"].lt(0), "favorite", "plus_money")
    out["prob_bucket"] = pd.cut(
        out["model_prob"],
        bins=PROB_BINS,
        labels=PROB_LABELS,
        right=False,
        include_lowest=True,
    )
    return out[out["prob_bucket"].notna()].copy()


def _log_loss(actual: pd.Series, prob: pd.Series) -> float:
    p = pd.to_numeric(prob, errors="coerce").clip(1e-6, 1 - 1e-6)
    y = pd.to_numeric(actual, errors="coerce")
    return float((-(y * np.log(p) + (1 - y) * np.log(1 - p))).mean())


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    actual = float(group["actual_win"].mean()) if bets else np.nan
    model = float(group["model_prob"].mean()) if bets else np.nan
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": actual - model if bets else np.nan,
        "brier_score": float(((group["model_prob"] - group["actual_win"]) ** 2).mean()) if bets else np.nan,
        "log_loss": _log_loss(group["actual_win"], group["model_prob"]) if bets else np.nan,
    }


def _add_group(rows: list[dict[str, Any]], sides: pd.DataFrame, level: str, cols: list[str]) -> None:
    for keys, group in sides.groupby(cols, observed=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {
            "group_level": level,
            "side": "ALL",
            "line_bucket": "ALL",
            "prob_bucket": "ALL",
            "price_class": "ALL",
            "line_side": "ALL",
        }
        row.update(dict(zip(cols, [str(k) for k in keys])))
        row.update(_metrics(group))
        rows.append(row)


def build_surface(sides: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    group_defs = [
        ("overall", []),
        ("side", ["side"]),
        ("line", ["line_bucket"]),
        ("prob_bucket", ["prob_bucket"]),
        ("price_class", ["price_class"]),
        ("line_side", ["line_side"]),
        ("side_prob_bucket", ["side", "prob_bucket"]),
        ("line_prob_bucket", ["line_bucket", "prob_bucket"]),
        ("price_prob_bucket", ["price_class", "prob_bucket"]),
        ("line_side_prob_bucket", ["line_side", "prob_bucket"]),
        ("line_side_price_class", ["line_side", "price_class"]),
        ("line_side_price_prob_bucket", ["line_side", "price_class", "prob_bucket"]),
    ]
    for level, cols in group_defs:
        if cols:
            _add_group(rows, sides, level, cols)
        else:
            row = {
                "group_level": level,
                "side": "ALL",
                "line_bucket": "ALL",
                "prob_bucket": "ALL",
                "price_class": "ALL",
                "line_side": "ALL",
            }
            row.update(_metrics(sides))
            rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["group_level", "side", "line_bucket", "line_side", "price_class", "prob_bucket"]
    )


def _fmt_pct(value: Any) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value) * 100:.1f}%"


def _md_table(df: pd.DataFrame, cols: list[str], max_rows: int = 20) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].head(max_rows).copy()
    for col in ["avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"]:
        if col in work.columns:
            if col in {"avg_model_prob", "actual_win_rate", "calibration_error"}:
                work[col] = work[col].map(_fmt_pct)
            else:
                work[col] = work[col].map(lambda v: "NA" if pd.isna(v) else f"{float(v):.3f}")
    work = work.fillna("")
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_summary(surface: pd.DataFrame, out_md: Path, from_date: str, to_date: str) -> None:
    overall = surface[surface["group_level"].eq("overall")].iloc[0]
    side = surface[surface["group_level"].eq("side")].sort_values("calibration_error")
    line = surface[surface["group_level"].eq("line")].sort_values("calibration_error")
    prob = surface[surface["group_level"].eq("prob_bucket")].sort_values("prob_bucket")
    price = surface[surface["group_level"].eq("price_class")].sort_values("calibration_error")
    line_side = surface[surface["group_level"].eq("line_side")].sort_values("calibration_error")
    worst = surface[
        surface["group_level"].isin(
            {"side_prob_bucket", "line_prob_bucket", "price_prob_bucket", "line_side_prob_bucket", "line_side_price_prob_bucket"}
        )
        & surface["bets"].ge(50)
    ].sort_values("calibration_error")

    lines = [
        "# Hits Probability Failure Summary",
        "",
        f"Date range: `{from_date or 'first available'}` to `{to_date or 'last available'}`",
        "",
        "This report is calibration-only. It does not optimize ROI or derive betting rules.",
        "",
        "## Overall",
        "",
        f"- Bets: `{int(overall['bets'])}`",
        f"- Avg model probability: `{_fmt_pct(overall['avg_model_prob'])}`",
        f"- Actual win rate: `{_fmt_pct(overall['actual_win_rate'])}`",
        f"- Calibration error: `{_fmt_pct(overall['calibration_error'])}`",
        f"- Brier score: `{overall['brier_score']:.3f}`",
        f"- Log loss: `{overall['log_loss']:.3f}`",
        "",
        "## By Side",
        "",
        _md_table(side, ["side", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"]),
        "",
        "## By Line",
        "",
        _md_table(line, ["line_bucket", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"]),
        "",
        "## By Probability Bucket",
        "",
        _md_table(prob, ["prob_bucket", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"]),
        "",
        "## By Price Class",
        "",
        _md_table(price, ["price_class", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"]),
        "",
        "## By Line/Side",
        "",
        _md_table(line_side, ["line_side", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"]),
        "",
        "## Worst Overconfidence Pockets",
        "",
        _md_table(
            worst,
            ["group_level", "side", "line_bucket", "line_side", "price_class", "prob_bucket", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "brier_score", "log_loss"],
            max_rows=25,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose hits probability overconfidence.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    hits = _load_hits(paths)
    sides = _side_rows(hits)
    if sides.empty:
        raise SystemExit("No resolved hits side rows with model_prob >= 0.50 found.")
    surface = build_surface(sides)
    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    surface.to_csv(out_csv, index=False)
    write_summary(surface, out_md, args.from_date, args.to_date)
    print(f"[hits-prob-failure] files={len(paths)} side_rows={len(sides)} out_csv={out_csv} out_md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
