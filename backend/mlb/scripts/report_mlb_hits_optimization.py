#!/usr/bin/env python3
"""Build a hits-only decision surface from full-slate outcomes.

CSV-only reporting. This does not change model logic or production selection.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_SURFACE_CSV = Path("backend/mlb/exports/model_performance/hits_decision_surface.csv")
DEFAULT_RULES_CSV = Path("backend/mlb/exports/model_performance/hits_rules.csv")

PROB_BINS = [0.0, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, np.inf]
PROB_LABELS = ["<0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]
PRICE_BINS = [-np.inf, -200, -150, -110, 100, 150, np.inf]
PRICE_LABELS = ["<=-200", "-200_to_-150", "-150_to_-110", "-110_to_+100", "+100_to_+150", "+150+"]


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
        "pnl_over_1u",
        "pnl_under_1u",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-optimization] skip {path}: missing {missing}")
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
        out = pd.DataFrame(
            {
                "source_date": rows["source_date"],
                "prop_type": "hits",
                "side": side,
                "line": pd.to_numeric(rows["line"], errors="coerce"),
                "price": pd.to_numeric(rows[f"price_{side}_american"], errors="coerce"),
                "model_prob": pd.to_numeric(rows[f"model_prob_{side}"], errors="coerce"),
                "outcome": rows[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
                "pnl": pd.to_numeric(rows[f"pnl_{side}_1u"], errors="coerce"),
            }
        )
        pieces.append(out)
    sides = pd.concat(pieces, ignore_index=True)
    sides = sides[
        sides["outcome"].isin({"win", "loss"})
        & sides["model_prob"].notna()
        & sides["price"].notna()
        & sides["line"].notna()
    ].copy()
    sides["win"] = sides["outcome"].eq("win").astype(int)
    sides["model_prob_bucket"] = pd.cut(
        sides["model_prob"],
        bins=PROB_BINS,
        labels=PROB_LABELS,
        right=False,
        include_lowest=True,
    )
    sides["price_bucket"] = pd.cut(
        sides["price"],
        bins=PRICE_BINS,
        labels=PRICE_LABELS,
        right=False,
        include_lowest=True,
    )
    sides["line_bucket"] = sides["line"].map(lambda v: f"{float(v):g}")
    return sides.dropna(subset=["model_prob_bucket", "price_bucket"]).copy()


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    wins = int(group["win"].sum()) if bets else 0
    losses = int(bets - wins)
    profit = float(pd.to_numeric(group["pnl"], errors="coerce").fillna(0.0).sum()) if bets else 0.0
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / bets if bets else np.nan,
        "profit_units": profit,
        "roi": profit / bets if bets else np.nan,
        "avg_price": float(group["price"].mean()) if bets else np.nan,
        "avg_model_prob": float(group["model_prob"].mean()) if bets else np.nan,
    }


def _zone(row: pd.Series, min_bets: int) -> str:
    if int(row["bets"]) < min_bets:
        return "low_sample"
    roi = float(row["roi"])
    if roi > 0.05:
        return "positive_roi"
    if roi < -0.05:
        return "negative_roi"
    return "neutral_roi"


def build_surface(sides: pd.DataFrame, min_bets: int) -> pd.DataFrame:
    levels = [
        ("prob_price", ["model_prob_bucket", "price_bucket"]),
        ("prob_price_side", ["model_prob_bucket", "price_bucket", "side"]),
        ("prob_price_side_line", ["model_prob_bucket", "price_bucket", "side", "line_bucket"]),
    ]
    rows = []
    for group_level, cols in levels:
        for keys, group in sides.groupby(cols, observed=True, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {
                "group_level": group_level,
                "model_prob_bucket": "",
                "price_bucket": "",
                "side": "ALL",
                "line_bucket": "ALL",
            }
            row.update(dict(zip(cols, [str(k) for k in keys])))
            row.update(_metrics(group))
            rows.append(row)
    out = pd.DataFrame(rows)
    out["zone"] = out.apply(lambda row: _zone(row, min_bets), axis=1)
    return out.sort_values(["group_level", "model_prob_bucket", "price_bucket", "side", "line_bucket"]).reset_index(drop=True)


def build_rules(surface: pd.DataFrame, min_bets: int) -> pd.DataFrame:
    detailed = surface[
        surface["group_level"].eq("prob_price_side_line")
        & surface["bets"].ge(min_bets)
        & surface["zone"].isin({"positive_roi", "neutral_roi", "negative_roi"})
    ].copy()
    if detailed.empty:
        return pd.DataFrame(
            columns=[
                "rule_type",
                "model_prob_range",
                "price_range",
                "side_preference",
                "line_preference",
                "bets",
                "win_rate",
                "roi",
                "suggested_action",
            ]
        )

    def action(zone: str) -> str:
        if zone == "positive_roi":
            return "allow_candidate"
        if zone == "neutral_roi":
            return "watch_or_tighten"
        return "avoid_bucket"

    rules = detailed.rename(
        columns={
            "model_prob_bucket": "model_prob_range",
            "price_bucket": "price_range",
            "side": "side_preference",
            "line_bucket": "line_preference",
            "zone": "rule_type",
        }
    ).copy()
    rules["suggested_action"] = rules["rule_type"].map(action)
    return rules[
        [
            "rule_type",
            "model_prob_range",
            "price_range",
            "side_preference",
            "line_preference",
            "bets",
            "win_rate",
            "roi",
            "profit_units",
            "avg_price",
            "avg_model_prob",
            "suggested_action",
        ]
    ].sort_values(["rule_type", "roi"], ascending=[True, False])


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build hits-only model probability x price decision surface.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--min-bets", type=int, default=50)
    ap.add_argument("--out-csv", default=str(DEFAULT_SURFACE_CSV))
    ap.add_argument("--rules-csv", default=str(DEFAULT_RULES_CSV))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    hits = _load_hits(paths)
    sides = _side_rows(hits)
    surface = build_surface(sides, args.min_bets)
    rules = build_rules(surface, args.min_bets)

    out_path = Path(args.out_csv)
    rules_path = Path(args.rules_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    surface.to_csv(out_path, index=False)
    rules.to_csv(rules_path, index=False)

    positive = int(rules["rule_type"].eq("positive_roi").sum()) if not rules.empty else 0
    neutral = int(rules["rule_type"].eq("neutral_roi").sum()) if not rules.empty else 0
    negative = int(rules["rule_type"].eq("negative_roi").sum()) if not rules.empty else 0
    print(
        "[hits-optimization] "
        f"files={len(paths)} side_rows={len(sides)} rules={len(rules)} "
        f"positive={positive} neutral={neutral} negative={negative} "
        f"out_csv={out_path} rules_csv={rules_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
