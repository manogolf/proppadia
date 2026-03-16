#!/usr/bin/env python3
"""Convert holdout probability CSV into walkforward/backtest row schema.

Input is expected to contain over-side probabilities:
  - p_mkt (market over probability)
  - one model probability column (e.g., p_base or p_model) used as p_base output
  - realized binary over outcome y_over (1=over wins, 0=under wins)

Output duplicates each row into over + under picks so downstream scripts can
evaluate both sides with two-sided executable prices.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _to_bool_series(n: int) -> list[bool]:
    return [True] * int(max(0, n))


def _need(df: pd.DataFrame, cols: list[str]) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise SystemExit(f"input csv missing required columns: {miss}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build backtest rows from holdout probability CSV.")
    ap.add_argument("--in-csv", required=True)
    ap.add_argument(
        "--model-prob-col",
        default="p_model",
        help="Input over-probability column to use as output p_base.",
    )
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    fp = Path(args.in_csv)
    if not fp.exists():
        raise SystemExit(f"input csv not found: {fp}")
    df = pd.read_csv(fp)
    _need(
        df,
        [
            "game_date",
            "line",
            "player_id",
            "player_name",
            "game_id",
            "p_mkt",
            str(args.model_prob_col),
            "y_over",
            "shots_on_goal",
        ],
    )

    work = df.copy()
    work["game_date"] = work["game_date"].astype(str)
    work["line"] = pd.to_numeric(work["line"], errors="coerce").round(1)
    work["player_id"] = pd.to_numeric(work["player_id"], errors="coerce")
    work["game_id"] = pd.to_numeric(work["game_id"], errors="coerce")
    work["p_mkt"] = pd.to_numeric(work["p_mkt"], errors="coerce")
    work["p_model"] = pd.to_numeric(work[str(args.model_prob_col)], errors="coerce")
    work["y_over"] = pd.to_numeric(work["y_over"], errors="coerce")
    work["actual_sog"] = pd.to_numeric(work["shots_on_goal"], errors="coerce")
    work = work.dropna(subset=["game_date", "line", "player_id", "game_id", "p_mkt", "p_model", "y_over", "actual_sog"]).copy()

    # Clamp to avoid invalid 0/1 endpoints in downstream EV math.
    work["p_mkt"] = work["p_mkt"].clip(lower=1e-6, upper=1 - 1e-6)
    work["p_model"] = work["p_model"].clip(lower=1e-6, upper=1 - 1e-6)
    work["player_key"] = (
        work["player_name"]
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9\s]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    work["player_key"] = work["player_key"].map(lambda s: "" if not s else f"{s.split()[0][0]} {s.split()[-1]}")
    work = work[work["player_key"].ne("")].copy()

    over = work.copy()
    over["model_pick"] = "over"
    over["model_wl"] = over["y_over"].map(lambda y: "W" if int(y) == 1 else "L")

    under = work.copy()
    under["model_pick"] = "under"
    under["model_wl"] = under["y_over"].map(lambda y: "W" if int(y) == 0 else "L")

    out = pd.concat([over, under], ignore_index=True)
    out["publishable"] = _to_bool_series(len(out))
    out["p_base"] = out["p_model"]
    out["line_target"] = out["line"]
    out["actual_result"] = out["y_over"].map(lambda y: "over" if int(y) == 1 else "under")
    out["market_pick"] = out["p_mkt"].map(lambda p: "over" if float(p) >= 0.5 else "under")
    out["market_wl"] = out.apply(
        lambda r: ("W" if r["market_pick"] == r["actual_result"] else "L"),
        axis=1,
    )
    out["edge_base_vs_market"] = out["p_base"] - out["p_mkt"]

    cols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "player_key",
        "line",
        "line_target",
        "actual_sog",
        "actual_result",
        "p_mkt",
        "market_pick",
        "market_wl",
        "p_base",
        "model_pick",
        "model_wl",
        "edge_base_vs_market",
        "publishable",
    ]
    out_final = out[cols].sort_values(["game_date", "game_id", "player_id", "line", "model_pick"]).reset_index(drop=True)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_final.to_csv(out_path, index=False)
    print(
        f"[holdout_rows] wrote={out_path} rows={len(out_final)} dates={out_final['game_date'].nunique()} "
        f"min_date={out_final['game_date'].min()} max_date={out_final['game_date'].max()} model_prob_col={args.model_prob_col}"
    )


if __name__ == "__main__":
    main()
