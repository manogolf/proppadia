#!/usr/bin/env python3
"""Build a full-refit SOG market CSV arm for shadow bakeoff.

Model form (per line):
  y_over ~ market_logit + base_logit + shared context features

Training source:
  - historical feature dataset (season CSV)
  - archived odds history (primary over price)

Live apply source:
  - input market CSV (sog_with_market style)
  - live slate feature CSV (daily sog_features export)
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

from backend.nhl.scripts.experiment_sog_market_residual_logit import _prepare_matched


def _prob_to_fair_american(p: float) -> int | None:
    if not math.isfinite(float(p)):
        return None
    p = float(p)
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def _logit(p: pd.Series) -> pd.Series:
    q = pd.to_numeric(p, errors="coerce").clip(1e-6, 1.0 - 1e-6)
    return np.log(q / (1.0 - q))


@dataclass
class LineModel:
    line: float
    model: LogisticRegression | None
    train_rows: int
    train_pos_rate: float | None
    intercept: float | None
    coefs: dict[str, float]
    fallback_identity: bool
    reason: str


def _fit_line_model(
    train: pd.DataFrame,
    *,
    line: float,
    feature_cols: list[str],
    min_train_rows: int,
) -> tuple[LineModel, dict[str, float]]:
    sub = train[train["line"] == float(line)].copy()
    if sub.empty:
        return (
            LineModel(
                line=float(line),
                model=None,
                train_rows=0,
                train_pos_rate=None,
                intercept=None,
                coefs={},
                fallback_identity=True,
                reason="no_train_rows_for_line",
            ),
            {},
        )

    y = pd.to_numeric(sub["y_over"], errors="coerce")
    ok = y.notna()
    for c in feature_cols:
        ok = ok & pd.to_numeric(sub[c], errors="coerce").notna()
    sub = sub[ok].copy()
    y = pd.to_numeric(sub["y_over"], errors="coerce").astype(int)
    n = int(len(sub))

    if n < int(min_train_rows):
        return (
            LineModel(
                line=float(line),
                model=None,
                train_rows=n,
                train_pos_rate=(float(y.mean()) if n else None),
                intercept=None,
                coefs={},
                fallback_identity=True,
                reason=f"train_rows<{int(min_train_rows)}",
            ),
            {},
        )
    if y.nunique(dropna=True) < 2:
        return (
            LineModel(
                line=float(line),
                model=None,
                train_rows=n,
                train_pos_rate=(float(y.mean()) if n else None),
                intercept=None,
                coefs={},
                fallback_identity=True,
                reason="single_class_target",
            ),
            {},
        )

    medians: dict[str, float] = {}
    for c in feature_cols:
        vals = pd.to_numeric(sub[c], errors="coerce")
        medians[c] = float(vals.median()) if vals.notna().any() else 0.0
        sub[c] = vals.fillna(medians[c])

    X = sub[feature_cols].to_numpy(dtype=float)
    clf = LogisticRegression(max_iter=2000, solver="lbfgs")
    clf.fit(X, y.to_numpy(dtype=int))

    return (
        LineModel(
            line=float(line),
            model=clf,
            train_rows=n,
            train_pos_rate=float(y.mean()),
            intercept=float(clf.intercept_[0]),
            coefs={k: float(v) for k, v in zip(feature_cols, clf.coef_[0].tolist())},
            fallback_identity=False,
            reason="ok",
        ),
        medians,
    )


def _apply_line_model(
    live: pd.DataFrame,
    lm: LineModel,
    *,
    feature_cols: list[str],
    medians: dict[str, float],
    blend_alpha: float,
) -> tuple[pd.Series, pd.Series]:
    sub = live[live["line"] == float(lm.line)].copy()
    if sub.empty:
        return (pd.Series([], dtype=float), pd.Series([], dtype=float))

    base = pd.to_numeric(sub["p_over_base"], errors="coerce").astype(float).clip(1e-6, 1 - 1e-6)
    if lm.model is None:
        raw = base.copy()
    else:
        for c in feature_cols:
            sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(float(medians.get(c, 0.0)))
        X = sub[feature_cols].to_numpy(dtype=float)
        raw = pd.Series(lm.model.predict_proba(X)[:, 1], index=sub.index, dtype=float)

    a = float(np.clip(float(blend_alpha), 0.0, 1.0))
    final = (a * raw) + ((1.0 - a) * base)
    final = final.clip(lower=1e-6, upper=1 - 1e-6)
    return raw, final


def main() -> None:
    ap = argparse.ArgumentParser(description="Build full-refit live market CSV arm from historical rows + live features.")
    ap.add_argument("--dataset-csv", default="backend/nhl/data/analysis/sog_poisson_residual_dataset_season_2025.csv")
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--market-csv-in", required=True)
    ap.add_argument("--features-csv", required=True)
    ap.add_argument("--market-csv-out", required=True)
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--game-date", required=True, help="YYYY-MM-DD. Train uses rows strictly before this date.")
    ap.add_argument("--train-from-date", default="", help="Optional inclusive train lower bound.")
    ap.add_argument("--train-to-date", default="", help="Optional inclusive train upper bound.")
    ap.add_argument("--min-train-rows-per-line", type=int, default=400)
    ap.add_argument("--blend-alpha", type=float, default=1.0, help="0..1; 1 uses pure full-refit model output.")
    args = ap.parse_args()

    dataset_fp = Path(args.dataset_csv)
    odds_root = Path(args.odds_root)
    in_fp = Path(args.market_csv_in)
    feats_fp = Path(args.features_csv)
    out_fp = Path(args.market_csv_out)
    if not dataset_fp.exists():
        raise SystemExit(f"dataset csv not found: {dataset_fp}")
    if not odds_root.exists():
        raise SystemExit(f"odds root not found: {odds_root}")
    if not in_fp.exists():
        raise SystemExit(f"market csv in not found: {in_fp}")
    if not feats_fp.exists():
        raise SystemExit(f"features csv not found: {feats_fp}")

    train_to_date = str(args.train_to_date).strip() or str(args.game_date).strip()
    train = _prepare_matched(
        dataset_csv=dataset_fp,
        odds_root=odds_root,
        bookmaker_key=str(args.bookmaker),
        from_date=(str(args.train_from_date).strip() or None),
        to_date=(train_to_date or None),
    )
    train["game_date"] = train["game_date"].astype(str)
    train["line"] = pd.to_numeric(train["line"], errors="coerce").round(1)
    train = train[train["game_date"] < str(args.game_date).strip()].copy()
    if train.empty:
        raise SystemExit("no training rows after filtering")

    market = pd.read_csv(in_fp).copy()
    need_market = {"player_id", "game_id", "line", "p_over", "p_over_mkt"}
    miss_market = [c for c in need_market if c not in market.columns]
    if miss_market:
        raise SystemExit(f"input market csv missing required columns: {miss_market}")
    market["player_id"] = pd.to_numeric(market["player_id"], errors="coerce")
    market["game_id"] = pd.to_numeric(market["game_id"], errors="coerce")
    market["line"] = pd.to_numeric(market["line"], errors="coerce").round(1)
    market["p_over"] = pd.to_numeric(market["p_over"], errors="coerce")
    market["p_over_mkt"] = pd.to_numeric(market["p_over_mkt"], errors="coerce")
    market = market.dropna(subset=["player_id", "game_id", "line", "p_over", "p_over_mkt"]).copy()
    if market.empty:
        raise SystemExit("input market csv has no usable rows")

    feats = pd.read_csv(feats_fp).copy()
    need_feats = {
        "player_id",
        "game_id",
        "d10_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "pace_matchup_index",
        "is_home",
    }
    miss_feats = [c for c in need_feats if c not in feats.columns]
    if miss_feats:
        raise SystemExit(f"features csv missing required columns: {miss_feats}")
    feats["player_id"] = pd.to_numeric(feats["player_id"], errors="coerce")
    feats["game_id"] = pd.to_numeric(feats["game_id"], errors="coerce")
    feats = (
        feats.sort_values(["player_id", "game_id"])
        .drop_duplicates(["player_id", "game_id"], keep="last")
        .reset_index(drop=True)
    )

    feature_cols = [
        "market_logit",
        "base_logit",
        "d10_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "pace_matchup_index",
        "is_home",
    ]

    # Train features are from matched historical rows.
    train["market_logit"] = _logit(train["p_mkt"])
    train["base_logit"] = _logit(train["p_base"])
    for c in [
        "d10_sog_per60",
        "attempts_d10_per60",
        "d10_toi_min_avg",
        "role_pp_share",
        "pace_matchup_index",
        "is_home",
    ]:
        train[c] = pd.to_numeric(train[c], errors="coerce")

    live = market.merge(
        feats[
            [
                "player_id",
                "game_id",
                "d10_sog_per60",
                "attempts_d10_per60",
                "d10_toi_min_avg",
                "role_pp_share",
                "pace_matchup_index",
                "is_home",
            ]
        ],
        on=["player_id", "game_id"],
        how="left",
    )
    live["p_over_base"] = live["p_over"].astype(float).clip(1e-6, 1 - 1e-6)
    live["market_logit"] = _logit(live["p_over_mkt"])
    live["base_logit"] = _logit(live["p_over_base"])
    live["p_over_full_refit_raw"] = live["p_over_base"]
    live["p_over"] = live["p_over_base"]

    model_lines: list[dict[str, Any]] = []
    for line in (1.5, 2.5, 3.5):
        lm, medians = _fit_line_model(
            train=train,
            line=float(line),
            feature_cols=feature_cols,
            min_train_rows=int(args.min_train_rows_per_line),
        )
        raw, final = _apply_line_model(
            live=live,
            lm=lm,
            feature_cols=feature_cols,
            medians=medians,
            blend_alpha=float(args.blend_alpha),
        )
        if not raw.empty:
            idx = raw.index
            live.loc[idx, "p_over_full_refit_raw"] = raw.astype(float)
            live.loc[idx, "p_over"] = final.astype(float)
        model_lines.append(
            {
                "line": float(line),
                "train_rows": int(lm.train_rows),
                "train_pos_rate": lm.train_pos_rate,
                "fallback_identity": bool(lm.fallback_identity),
                "reason": lm.reason,
                "intercept": lm.intercept,
                "coefs": lm.coefs,
            }
        )

    live["p_over"] = pd.to_numeric(live["p_over"], errors="coerce").clip(1e-6, 1 - 1e-6)
    live["p_under"] = 1.0 - live["p_over"]
    if "p_under_mkt" in live.columns:
        live["edge_under"] = live["p_under"] - pd.to_numeric(live["p_under_mkt"], errors="coerce")
    live["edge_over"] = live["p_over"] - pd.to_numeric(live["p_over_mkt"], errors="coerce")
    live["fair_over"] = live["p_over"].map(lambda p: _prob_to_fair_american(float(p)))
    live["fair_under"] = live["p_under"].map(lambda p: _prob_to_fair_american(float(p)))

    out_fp.parent.mkdir(parents=True, exist_ok=True)
    live.to_csv(out_fp, index=False)

    summary = {
        "ok": True,
        "dataset_csv": str(dataset_fp),
        "odds_root": str(odds_root),
        "bookmaker": str(args.bookmaker),
        "features_csv": str(feats_fp),
        "market_csv_in": str(in_fp),
        "market_csv_out": str(out_fp),
        "game_date_cutoff": str(args.game_date).strip(),
        "train_from_date": (str(args.train_from_date).strip() or None),
        "train_to_date": (str(args.train_to_date).strip() or None),
        "blend_alpha": float(args.blend_alpha),
        "min_train_rows_per_line": int(args.min_train_rows_per_line),
        "train_rows_after_filters": int(len(train)),
        "market_rows_written": int(len(live)),
        "market_rows_missing_live_features": int(
            live[
                [
                    "d10_sog_per60",
                    "attempts_d10_per60",
                    "d10_toi_min_avg",
                    "role_pp_share",
                    "pace_matchup_index",
                    "is_home",
                ]
            ]
            .isna()
            .any(axis=1)
            .sum()
        ),
        "feature_cols": feature_cols,
        "lines": model_lines,
    }
    if str(args.summary_json).strip():
        sj = Path(args.summary_json)
        sj.parent.mkdir(parents=True, exist_ok=True)
        sj.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

