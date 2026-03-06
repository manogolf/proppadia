#!/usr/bin/env python3
"""Select NHL SOG wager candidates from row-level reconciliation output.

This script is intentionally policy-focused:
  - side-aware EV and gap (over/under transformed correctly)
  - favorite/dog gap gates (default: 6% favorite, 3% dog)
  - one pick per player per slate
  - optional per-game and per-slate caps

Input is expected to be the row CSV produced by:
  backend/nhl/scripts/reconcile_sog_base_vs_betonline_by_month.py
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def prob_to_fair_american(p: float) -> int | None:
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


@dataclass(frozen=True)
class Thresholds:
    min_ev: float
    min_gap_favorite: float
    min_gap_dog: float


def _to_bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y"}


def _norm_line(v: Any) -> str:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(x):
        return str(v)
    return f"{float(x):.1f}"


def _load_threshold_overrides(path: Path | None) -> dict[str, Thresholds]:
    if path is None:
        return {}
    data = json.loads(path.read_text())
    out: dict[str, Thresholds] = {}
    for key, val in data.items():
        out[str(key)] = Thresholds(
            min_ev=float(val["min_ev"]),
            min_gap_favorite=float(val["min_gap_favorite"]),
            min_gap_dog=float(val["min_gap_dog"]),
        )
    return out


def _pick_thresholds(
    row: pd.Series,
    default: Thresholds,
    overrides: dict[str, Thresholds],
) -> Thresholds:
    line = _norm_line(row["line"])
    side = str(row["model_pick"]).strip().lower()
    seg_key = f"{side}:{line}"
    return overrides.get(seg_key, default)


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["publishable"] = out["publishable"].map(_to_bool)
    out["game_date"] = out["game_date"].astype(str)
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["p_base"] = pd.to_numeric(out["p_base"], errors="coerce")
    out["p_mkt"] = pd.to_numeric(out["p_mkt"], errors="coerce")
    out["actual_sog"] = pd.to_numeric(out["actual_sog"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["model_pick"] = out["model_pick"].astype(str).str.lower().str.strip()
    out["model_wl"] = out["model_wl"].astype(str).str.upper().str.strip()

    # Side-aware probability transform for the model's selected side.
    over_mask = out["model_pick"] == "over"
    under_mask = out["model_pick"] == "under"

    out["model_side_prob"] = pd.NA
    out["market_side_prob"] = pd.NA
    out.loc[over_mask, "model_side_prob"] = out.loc[over_mask, "p_base"]
    out.loc[over_mask, "market_side_prob"] = out.loc[over_mask, "p_mkt"]
    out.loc[under_mask, "model_side_prob"] = 1.0 - out.loc[under_mask, "p_base"]
    out.loc[under_mask, "market_side_prob"] = 1.0 - out.loc[under_mask, "p_mkt"]

    out["model_side_prob"] = pd.to_numeric(out["model_side_prob"], errors="coerce")
    out["market_side_prob"] = pd.to_numeric(out["market_side_prob"], errors="coerce")
    out["edge_side"] = out["model_side_prob"] - out["market_side_prob"]
    out["ev_side"] = (out["model_side_prob"] / out["market_side_prob"]) - 1.0
    out["market_is_favorite"] = out["market_side_prob"] >= 0.5
    out["market_side"] = out["market_is_favorite"].map({True: "favorite", False: "dog"})
    return out


def _summarize(selected: pd.DataFrame) -> dict[str, Any]:
    def _wl_counts(frame: pd.DataFrame) -> dict[str, Any]:
        w = int((frame["model_wl"] == "W").sum())
        l = int((frame["model_wl"] == "L").sum())
        p = int((frame["model_wl"] == "P").sum())
        actioned = w + l
        return {
            "rows": int(len(frame)),
            "wins": w,
            "losses": l,
            "pushes": p,
            "win_pct_no_push": (w / actioned) if actioned else None,
        }

    out: dict[str, Any] = {"overall": _wl_counts(selected)}

    by_side: dict[str, Any] = {}
    for side, sub in selected.groupby("model_pick", dropna=False):
        by_side[str(side)] = _wl_counts(sub)
    out["by_side"] = by_side

    by_line: dict[str, Any] = {}
    for line, sub in selected.groupby("line", dropna=False):
        by_line[_norm_line(line)] = _wl_counts(sub)
    out["by_line"] = by_line

    by_month: dict[str, Any] = {}
    tmp = selected.copy()
    tmp["month"] = tmp["game_date"].str.slice(0, 7)
    for month, sub in tmp.groupby("month", dropna=False):
        by_month[str(month)] = _wl_counts(sub)
    out["by_month"] = by_month

    return out


def _select(
    df: pd.DataFrame,
    default_thr: Thresholds,
    overrides: dict[str, Thresholds],
    max_per_game: int,
    max_per_slate: int,
    max_fair_favorite: int,
    min_ev_floor: float,
    min_gap_floor_favorite: float,
    min_gap_floor_dog: float,
) -> pd.DataFrame:
    out = df.copy()
    out = out[out["publishable"]].copy()
    out = out[out["model_pick"].isin(["over", "under"])].copy()
    out = out[
        out["model_side_prob"].between(0.0, 1.0, inclusive="neither")
        & out["market_side_prob"].between(0.0, 1.0, inclusive="neither")
    ].copy()

    # Apply segment-specific thresholds if provided.
    min_evs = []
    min_gaps = []
    for _, row in out.iterrows():
        t = _pick_thresholds(row, default_thr, overrides)
        min_evs.append(float(t.min_ev))
        min_gap = float(t.min_gap_favorite if bool(row["market_is_favorite"]) else t.min_gap_dog)
        min_gaps.append(min_gap)
    out["min_ev_required"] = min_evs
    out["min_gap_required"] = min_gaps
    out["effective_min_ev"] = pd.to_numeric(out["min_ev_required"], errors="coerce").clip(lower=float(min_ev_floor))
    out["effective_min_gap"] = pd.to_numeric(out["min_gap_required"], errors="coerce")
    out.loc[out["market_is_favorite"], "effective_min_gap"] = out.loc[out["market_is_favorite"], "effective_min_gap"].clip(
        lower=float(min_gap_floor_favorite)
    )
    out.loc[~out["market_is_favorite"], "effective_min_gap"] = out.loc[~out["market_is_favorite"], "effective_min_gap"].clip(
        lower=float(min_gap_floor_dog)
    )

    out = out[(out["ev_side"] >= out["effective_min_ev"]) & (out["edge_side"] >= out["effective_min_gap"])].copy()
    if out.empty:
        return out

    out["model_side_fair_american"] = out["model_side_prob"].map(prob_to_fair_american)
    out = out.dropna(subset=["model_side_fair_american"]).copy()
    out["model_side_fair_american"] = out["model_side_fair_american"].astype(int)
    out = out[
        (out["model_side_fair_american"] > 0)
        | (out["model_side_fair_american"] >= int(max_fair_favorite))
    ].copy()
    if out.empty:
        return out

    # Rank strongest candidates first.
    out = out.sort_values(
        ["game_date", "ev_side", "edge_side", "model_side_prob"],
        ascending=[True, False, False, False],
    ).copy()

    # One pick per player per slate.
    player_key = out["player_id"].fillna(-1).astype(int).astype(str) + "|" + out["player_name"].astype(str)
    out["_player_slate_key"] = out["game_date"] + "|" + player_key
    out = out.drop_duplicates("_player_slate_key", keep="first").copy()

    # Optional per-game cap.
    if max_per_game > 0:
        keep_idx: list[int] = []
        for _, sub in out.groupby(["game_date", "game_id"], dropna=False):
            keep_idx.extend(sub.head(max_per_game).index.tolist())
        out = out.loc[keep_idx].copy()

    # Optional per-slate cap.
    if max_per_slate > 0:
        keep_idx = []
        for _, sub in out.groupby("game_date", dropna=False):
            keep_idx.extend(sub.head(max_per_slate).index.tolist())
        out = out.loc[keep_idx].copy()

    out = out.drop(columns=["_player_slate_key"], errors="ignore")
    out = out.sort_values(["game_date", "game_id", "ev_side"], ascending=[True, True, False]).reset_index(drop=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Select NHL SOG candidates from reconciliation row CSV.")
    ap.add_argument("--rows-csv", required=True, help="Path to nhl_sog_base_vs_betonline_rows.csv")
    ap.add_argument("--from-date", default="", help="Optional inclusive lower bound YYYY-MM-DD")
    ap.add_argument("--to-date", default="", help="Optional inclusive upper bound YYYY-MM-DD")
    ap.add_argument("--min-ev", type=float, default=0.05, help="Default minimum EV for picked side.")
    ap.add_argument(
        "--min-gap-favorite",
        type=float,
        default=0.06,
        help="Default minimum model-market gap when market-side prob >= 0.5.",
    )
    ap.add_argument(
        "--min-gap-dog",
        type=float,
        default=0.03,
        help="Default minimum model-market gap when market-side prob < 0.5.",
    )
    ap.add_argument(
        "--thresholds-json",
        default="",
        help="Optional JSON map of segment overrides keyed by '<side>:<line>' (e.g. over:2.5).",
    )
    ap.add_argument("--max-per-game", type=int, default=0, help="Cap picks per game (0 disables).")
    ap.add_argument("--max-per-slate", type=int, default=0, help="Cap picks per slate/day (0 disables).")
    ap.add_argument("--min-ev-floor", type=float, default=0.0, help="Hard EV floor after threshold policy.")
    ap.add_argument(
        "--min-gap-floor-favorite",
        type=float,
        default=0.0,
        help="Hard gap floor for favorite-side picks (market_side_prob >= 0.5).",
    )
    ap.add_argument(
        "--min-gap-floor-dog",
        type=float,
        default=0.0,
        help="Hard gap floor for dog-side picks (market_side_prob < 0.5).",
    )
    ap.add_argument(
        "--max-fair-favorite",
        type=int,
        default=-300,
        help=(
            "Drop rows whose fair odds are more juiced than this value "
            "(e.g. -300 drops -301, -500; dogs are unaffected)."
        ),
    )
    ap.add_argument("--out-csv", default="tmp/nhl_sog_candidate_policy_picks.csv")
    ap.add_argument("--out-json", default="tmp/nhl_sog_candidate_policy_summary.json")
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv)
    if not rows_csv.exists():
        raise SystemExit(f"rows csv not found: {rows_csv}")

    df = pd.read_csv(rows_csv)
    df = _prepare(df)
    if args.from_date:
        df = df[df["game_date"] >= str(args.from_date)].copy()
    if args.to_date:
        df = df[df["game_date"] <= str(args.to_date)].copy()

    default_thr = Thresholds(
        min_ev=float(args.min_ev),
        min_gap_favorite=float(args.min_gap_favorite),
        min_gap_dog=float(args.min_gap_dog),
    )
    overrides = _load_threshold_overrides(Path(args.thresholds_json)) if args.thresholds_json else {}

    selected = _select(
        df=df,
        default_thr=default_thr,
        overrides=overrides,
        max_per_game=int(args.max_per_game),
        max_per_slate=int(args.max_per_slate),
        max_fair_favorite=int(args.max_fair_favorite),
        min_ev_floor=float(args.min_ev_floor),
        min_gap_floor_favorite=float(args.min_gap_floor_favorite),
        min_gap_floor_dog=float(args.min_gap_floor_dog),
    )

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    cols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "line",
        "actual_sog",
        "actual_result",
        "model_pick",
        "model_wl",
        "p_base",
        "p_mkt",
        "model_side_prob",
        "market_side_prob",
        "market_side",
        "edge_side",
        "ev_side",
        "min_gap_required",
        "min_ev_required",
        "effective_min_gap",
        "effective_min_ev",
        "model_side_fair_american",
        "price_over",
        "publishable",
    ]
    keep = [c for c in cols if c in selected.columns]
    selected[keep].to_csv(out_csv, index=False)

    summary = {
        "config": {
            "rows_csv": str(rows_csv),
            "from_date": args.from_date or None,
            "to_date": args.to_date or None,
            "default_thresholds": {
                "min_ev": default_thr.min_ev,
                "min_gap_favorite": default_thr.min_gap_favorite,
                "min_gap_dog": default_thr.min_gap_dog,
            },
            "override_segments": sorted(overrides.keys()),
            "max_per_game": int(args.max_per_game),
            "max_per_slate": int(args.max_per_slate),
            "min_ev_floor": float(args.min_ev_floor),
            "min_gap_floor_favorite": float(args.min_gap_floor_favorite),
            "min_gap_floor_dog": float(args.min_gap_floor_dog),
            "max_fair_favorite": int(args.max_fair_favorite),
        },
        "selected": _summarize(selected),
        "row_count_selected": int(len(selected)),
        "outputs": {"csv": str(out_csv), "json": str(Path(args.out_json))},
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
