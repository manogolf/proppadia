#!/usr/bin/env python3
"""Build a residual-probability SOG market CSV for live arm testing.

This script:
  1) Fits per-line logistic models on historical rows using only
     market/base logits:
       y_over ~ logit(p_mkt) + logit(p_base)
  2) Applies those models to an input `sog_with_market.csv`.
  3) Writes an output market CSV with:
       - p_over_base (original model probability)
       - p_over_residual_raw (pure residual model output)
       - p_over (final probability used by selector; optionally blended)
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression


def _to_float(v: Any) -> float | None:
    try:
        if v is None or str(v).strip() == "":
            return None
        return float(v)
    except Exception:
        return None


def _logit_arr(p: np.ndarray) -> np.ndarray:
    q = np.clip(p.astype(float), 1e-6, 1.0 - 1e-6)
    return np.log(q / (1.0 - q))


def _prob_to_fair_american(p: float) -> int | None:
    if not math.isfinite(float(p)):
        return None
    p = float(p)
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def _extract_y_over(df: pd.DataFrame) -> pd.Series:
    # Preferred path: explicit actual_result column.
    if "actual_result" in df.columns:
        s = df["actual_result"].astype(str).str.lower().str.strip()
        y = s.map({"over": 1.0, "under": 0.0})
        if y.notna().any():
            return y

    # Fallback: model_pick/model_wl encoding.
    if {"model_pick", "model_wl"}.issubset(set(df.columns)):
        pick = df["model_pick"].astype(str).str.lower().str.strip()
        wl = df["model_wl"].astype(str).str.upper().str.strip()
        y = pd.Series(np.nan, index=df.index, dtype=float)
        y[(pick == "over") & (wl == "W")] = 1.0
        y[(pick == "over") & (wl == "L")] = 0.0
        y[(pick == "under") & (wl == "W")] = 0.0
        y[(pick == "under") & (wl == "L")] = 1.0
        if y.notna().any():
            return y

    # Last fallback: actual_sog compared to line.
    if {"actual_sog", "line"}.issubset(set(df.columns)):
        sog = pd.to_numeric(df["actual_sog"], errors="coerce")
        line = pd.to_numeric(df["line"], errors="coerce")
        return (sog > line).astype(float)

    return pd.Series(np.nan, index=df.index, dtype=float)


@dataclass
class LineModel:
    line: float
    model: LogisticRegression | None
    train_rows: int
    train_pos_rate: float | None
    intercept: float | None
    coef_market_logit: float | None
    coef_base_logit: float | None
    fallback_identity: bool
    reason: str


def _fit_line_model(
    train: pd.DataFrame,
    *,
    line: float,
    min_train_rows: int,
) -> LineModel:
    sub = train[train["line"] == float(line)].copy()
    if sub.empty:
        return LineModel(
            line=float(line),
            model=None,
            train_rows=0,
            train_pos_rate=None,
            intercept=None,
            coef_market_logit=None,
            coef_base_logit=None,
            fallback_identity=True,
            reason="no_train_rows_for_line",
        )

    y = pd.to_numeric(sub["y_over"], errors="coerce")
    p_mkt = pd.to_numeric(sub["p_mkt"], errors="coerce")
    p_base = pd.to_numeric(sub["p_base"], errors="coerce")
    mask = y.notna() & p_mkt.notna() & p_base.notna()
    y = y[mask].astype(int)
    p_mkt = p_mkt[mask].astype(float).clip(1e-6, 1 - 1e-6)
    p_base = p_base[mask].astype(float).clip(1e-6, 1 - 1e-6)

    n = int(len(y))
    if n < int(min_train_rows):
        return LineModel(
            line=float(line),
            model=None,
            train_rows=n,
            train_pos_rate=(float(y.mean()) if n else None),
            intercept=None,
            coef_market_logit=None,
            coef_base_logit=None,
            fallback_identity=True,
            reason=f"train_rows<{int(min_train_rows)}",
        )
    if y.nunique(dropna=True) < 2:
        return LineModel(
            line=float(line),
            model=None,
            train_rows=n,
            train_pos_rate=(float(y.mean()) if n else None),
            intercept=None,
            coef_market_logit=None,
            coef_base_logit=None,
            fallback_identity=True,
            reason="single_class_target",
        )

    X = np.column_stack([_logit_arr(p_mkt.to_numpy()), _logit_arr(p_base.to_numpy())])
    clf = LogisticRegression(max_iter=2000, solver="lbfgs")
    clf.fit(X, y.to_numpy(dtype=int))

    return LineModel(
        line=float(line),
        model=clf,
        train_rows=n,
        train_pos_rate=float(y.mean()),
        intercept=float(clf.intercept_[0]),
        coef_market_logit=float(clf.coef_[0][0]),
        coef_base_logit=float(clf.coef_[0][1]),
        fallback_identity=False,
        reason="ok",
    )


def _apply_line_model(
    df: pd.DataFrame,
    lm: LineModel,
    *,
    blend_alpha: float,
) -> tuple[pd.Series, pd.Series]:
    sub = df[df["line"] == float(lm.line)].copy()
    if sub.empty:
        return (
            pd.Series([], dtype=float),
            pd.Series([], dtype=float),
        )

    base = pd.to_numeric(sub["p_over_base"], errors="coerce").astype(float).clip(1e-6, 1 - 1e-6)
    mkt = pd.to_numeric(sub["p_over_mkt"], errors="coerce").astype(float).clip(1e-6, 1 - 1e-6)

    if lm.model is None:
        raw = base.copy()
    else:
        X = np.column_stack([_logit_arr(mkt.to_numpy()), _logit_arr(base.to_numpy())])
        raw = pd.Series(lm.model.predict_proba(X)[:, 1], index=sub.index, dtype=float)

    a = float(np.clip(float(blend_alpha), 0.0, 1.0))
    final = (a * raw) + ((1.0 - a) * base)
    final = final.clip(lower=1e-6, upper=1 - 1e-6)
    return raw, final


def main() -> None:
    ap = argparse.ArgumentParser(description="Build residual live market CSV from historical rows + base market CSV.")
    ap.add_argument("--history-rows-csv", default="tmp/nhl_sog_base_vs_betonline_rows.csv")
    ap.add_argument("--market-csv-in", required=True)
    ap.add_argument("--market-csv-out", required=True)
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--game-date", default="", help="Optional YYYY-MM-DD. If set, train only on rows before this date.")
    ap.add_argument("--train-from-date", default="", help="Optional inclusive train lower date.")
    ap.add_argument("--train-to-date", default="", help="Optional inclusive train upper date (applied before game-date cutoff).")
    ap.add_argument("--min-train-rows-per-line", type=int, default=400)
    ap.add_argument("--blend-alpha", type=float, default=1.0, help="0..1; 1 uses pure residual model output.")
    args = ap.parse_args()

    history_fp = Path(args.history_rows_csv)
    in_fp = Path(args.market_csv_in)
    out_fp = Path(args.market_csv_out)

    if not history_fp.exists():
        raise SystemExit(f"history rows csv not found: {history_fp}")
    if not in_fp.exists():
        raise SystemExit(f"market csv in not found: {in_fp}")

    hist = pd.read_csv(history_fp)
    need_hist = {"game_date", "line", "p_base", "p_mkt"}
    missing_hist = [c for c in need_hist if c not in hist.columns]
    if missing_hist:
        raise SystemExit(f"history rows missing required columns: {missing_hist}")
    hist = hist.copy()
    hist["game_date"] = hist["game_date"].astype(str)
    hist["line"] = pd.to_numeric(hist["line"], errors="coerce").round(1)
    hist["p_base"] = pd.to_numeric(hist["p_base"], errors="coerce")
    hist["p_mkt"] = pd.to_numeric(hist["p_mkt"], errors="coerce")
    hist["y_over"] = _extract_y_over(hist)

    if str(args.train_from_date).strip():
        hist = hist[hist["game_date"] >= str(args.train_from_date).strip()].copy()
    if str(args.train_to_date).strip():
        hist = hist[hist["game_date"] <= str(args.train_to_date).strip()].copy()
    if str(args.game_date).strip():
        hist = hist[hist["game_date"] < str(args.game_date).strip()].copy()

    hist = hist.dropna(subset=["line", "p_base", "p_mkt", "y_over"]).copy()
    if hist.empty:
        raise SystemExit("no training rows after filtering")

    market = pd.read_csv(in_fp).copy()
    need_mkt = {"line", "p_over", "p_over_mkt"}
    missing_mkt = [c for c in need_mkt if c not in market.columns]
    if missing_mkt:
        raise SystemExit(f"input market csv missing required columns: {missing_mkt}")
    market["line"] = pd.to_numeric(market["line"], errors="coerce").round(1)
    market["p_over"] = pd.to_numeric(market["p_over"], errors="coerce")
    market["p_over_mkt"] = pd.to_numeric(market["p_over_mkt"], errors="coerce")
    market = market.dropna(subset=["line", "p_over", "p_over_mkt"]).copy()
    if market.empty:
        raise SystemExit("input market csv has no usable rows")

    market["p_over_base"] = market["p_over"].astype(float).clip(1e-6, 1 - 1e-6)
    market["p_over_residual_raw"] = market["p_over_base"]
    market["p_over"] = market["p_over_base"]

    model_lines: list[dict[str, Any]] = []
    for line in (1.5, 2.5, 3.5):
        lm = _fit_line_model(
            hist,
            line=float(line),
            min_train_rows=int(args.min_train_rows_per_line),
        )
        raw, final = _apply_line_model(
            market,
            lm,
            blend_alpha=float(args.blend_alpha),
        )
        if not raw.empty:
            idx = raw.index
            market.loc[idx, "p_over_residual_raw"] = raw.astype(float)
            market.loc[idx, "p_over"] = final.astype(float)
        model_lines.append(
            {
                "line": float(line),
                "train_rows": int(lm.train_rows),
                "train_pos_rate": lm.train_pos_rate,
                "fallback_identity": bool(lm.fallback_identity),
                "reason": lm.reason,
                "intercept": lm.intercept,
                "coef_market_logit": lm.coef_market_logit,
                "coef_base_logit": lm.coef_base_logit,
            }
        )

    market["p_over"] = market["p_over"].clip(1e-6, 1 - 1e-6)
    market["p_under"] = 1.0 - market["p_over"]
    if "p_under_mkt" in market.columns:
        market["edge_under"] = market["p_under"] - pd.to_numeric(market["p_under_mkt"], errors="coerce")
    if "p_over_mkt" in market.columns:
        market["edge_over"] = market["p_over"] - pd.to_numeric(market["p_over_mkt"], errors="coerce")
    market["fair_over"] = market["p_over"].map(lambda p: _prob_to_fair_american(float(p)))
    market["fair_under"] = market["p_under"].map(lambda p: _prob_to_fair_american(float(p)))

    out_fp.parent.mkdir(parents=True, exist_ok=True)
    market.to_csv(out_fp, index=False)

    summary = {
        "ok": True,
        "history_rows_csv": str(history_fp),
        "market_csv_in": str(in_fp),
        "market_csv_out": str(out_fp),
        "game_date_cutoff": (str(args.game_date).strip() or None),
        "train_from_date": (str(args.train_from_date).strip() or None),
        "train_to_date": (str(args.train_to_date).strip() or None),
        "blend_alpha": float(args.blend_alpha),
        "min_train_rows_per_line": int(args.min_train_rows_per_line),
        "train_rows_after_filters": int(len(hist)),
        "market_rows_written": int(len(market)),
        "lines": model_lines,
    }
    if str(args.summary_json).strip():
        sj = Path(args.summary_json)
        sj.parent.mkdir(parents=True, exist_ok=True)
        sj.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
