#!/usr/bin/env python3
"""Select today's NHL SOG wager candidates using walk-forward threshold policy.

Policy input can be:
1) Full walk-forward summary JSON containing `thresholds_for_next_slate`
2) Direct segment map:
   {
     "over:1.5": {"min_ev": 0.03, "min_gap": 0.04},
     "under:2.5": {"min_ev": 0.03, "min_gap": 0.08}
   }
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def prob_to_fair_american(p: float) -> int | None:
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


@dataclass(frozen=True)
class SegmentPolicy:
    min_ev: float
    min_gap: float
    train_wilson_lb: float | None = None


def _to_line(v: Any) -> str:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(x):
        return str(v)
    return f"{float(x):.1f}"


def _load_policy(path: Path) -> dict[str, SegmentPolicy]:
    data = json.loads(path.read_text())
    if "thresholds_for_next_slate" in data:
        data = data["thresholds_for_next_slate"]

    out: dict[str, SegmentPolicy] = {}
    for seg, vals in data.items():
        out[str(seg)] = SegmentPolicy(
            min_ev=float(vals["min_ev"]),
            min_gap=float(vals["min_gap"]),
            train_wilson_lb=(
                None if vals.get("train_wilson_lb") is None else float(vals["train_wilson_lb"])
            ),
        )
    return out


def _build_side_rows(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    base["line_key"] = base["line"].map(_to_line)

    over = base.copy()
    over["model_pick"] = "over"
    over["model_side_prob"] = over["p_over"]
    over["market_side_prob"] = over["p_over_mkt"]
    over["segment"] = "over:" + over["line_key"]

    under = base.copy()
    under["model_pick"] = "under"
    under["model_side_prob"] = 1.0 - under["p_over"]
    under["market_side_prob"] = 1.0 - under["p_over_mkt"]
    under["segment"] = "under:" + under["line_key"]

    out = pd.concat([over, under], ignore_index=True)
    out["edge_side"] = out["model_side_prob"] - out["market_side_prob"]
    out["ev_side"] = (out["model_side_prob"] / out["market_side_prob"]) - 1.0
    out["market_side"] = (out["market_side_prob"] >= 0.5).map({True: "favorite", False: "dog"})
    return out


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "rows": int(len(df)),
        "segments": {},
        "by_side": {},
        "by_line": {},
    }
    if df.empty:
        return out

    for seg, sub in df.groupby("segment", dropna=False):
        out["segments"][str(seg)] = int(len(sub))

    for side, sub in df.groupby("model_pick", dropna=False):
        out["by_side"][str(side)] = int(len(sub))

    for line, sub in df.groupby("line_key", dropna=False):
        out["by_line"][str(line)] = int(len(sub))
    return out


def _write_summary_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _emit_book_upload(
    candidates_csv: Path,
    out_csv: Path,
    *,
    max_fair_favorite: int,
    availability_csv: str,
    skip_availability_filter: bool,
    exclude_player_ids: list[int],
) -> dict[str, Any]:
    exporter = Path(__file__).resolve().parent / "export_sog_candidate_book_upload.py"
    cmd = [
        sys.executable,
        str(exporter),
        "--candidates-csv",
        str(candidates_csv),
        "--out-csv",
        str(out_csv),
        "--max-fair-favorite",
        str(int(max_fair_favorite)),
    ]
    if skip_availability_filter:
        cmd.append("--skip-availability-filter")
    else:
        cmd.extend(["--availability-csv", str(availability_csv)])
    for pid in exclude_player_ids:
        cmd.extend(["--exclude-player-id", str(int(pid))])

    try:
        cp = subprocess.run(cmd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        raise SystemExit(
            "book upload export failed.\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout:\n{e.stdout}\n"
            f"stderr:\n{e.stderr}"
        ) from e

    out_rows = None
    if out_csv.exists():
        try:
            out_rows = int(len(pd.read_csv(out_csv)))
        except Exception:
            out_rows = None
    return {
        "csv": str(out_csv),
        "rows": out_rows,
        "cmd": cmd,
        "stdout_tail": (cp.stdout or "").strip().splitlines()[-5:],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Select live NHL SOG candidates from sog_with_market.csv")
    ap.add_argument("--market-csv", default="nhl/site/data/sog_with_market.csv")
    ap.add_argument("--policy-json", default="tmp/nhl_sog_walkforward_summary.json")
    ap.add_argument("--game-date", default="", help="YYYY-MM-DD (defaults to latest in market CSV)")
    ap.add_argument(
        "--min-train-wilson-lb",
        type=float,
        default=0.0,
        help="Disable segments with train_wilson_lb below this threshold (0 disables).",
    )
    ap.add_argument(
        "--min-ev-floor",
        type=float,
        default=0.0,
        help="Hard EV floor applied after walk-forward policy (0 disables).",
    )
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
    ap.add_argument("--max-per-player", type=int, default=1, help="0 disables.")
    ap.add_argument("--max-per-game", type=int, default=0, help="0 disables.")
    ap.add_argument("--max-per-slate", type=int, default=0, help="0 disables.")
    ap.add_argument(
        "--max-fair-favorite",
        type=int,
        default=-300,
        help=(
            "Drop selected sides whose fair odds are more juiced than this value "
            "(e.g. -300 drops -301, -500; dogs are unaffected)."
        ),
    )
    ap.add_argument("--out-csv", default="tmp/nhl_sog_live_candidates.csv")
    ap.add_argument("--out-json", default="tmp/nhl_sog_live_candidates_summary.json")
    ap.add_argument(
        "--emit-book-upload",
        action="store_true",
        help="Also write final upload CSV by calling export_sog_candidate_book_upload.py",
    )
    ap.add_argument(
        "--book-upload-out-csv",
        default="backend/nhl/data/processed/sog_candidate_book_upload.csv",
        help="Output CSV path used when --emit-book-upload is set.",
    )
    ap.add_argument(
        "--book-upload-max-fair-favorite",
        type=int,
        default=-300,
        help="Favorite fair-odds cap for generated upload CSV.",
    )
    ap.add_argument(
        "--book-upload-availability-csv",
        default="nhl/site/data/sog_with_market.csv",
        help="Availability CSV path passed to exporter.",
    )
    ap.add_argument(
        "--book-upload-skip-availability-filter",
        action="store_true",
        help="Pass through to exporter to skip availability filtering.",
    )
    ap.add_argument(
        "--book-upload-exclude-player-id",
        action="append",
        default=[],
        help="Repeatable player ID exclude passed to exporter.",
    )
    args = ap.parse_args()

    market_csv = Path(args.market_csv)
    policy_json = Path(args.policy_json)
    if not market_csv.exists():
        raise SystemExit(f"market csv not found: {market_csv}")
    if not policy_json.exists():
        raise SystemExit(f"policy json not found: {policy_json}")

    policy = _load_policy(policy_json)
    if not policy:
        raise SystemExit("policy is empty")

    df = pd.read_csv(market_csv)
    required = ["full_name", "player_id", "game_id", "line", "p_over", "p_over_mkt", "game_date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"market csv missing required columns: {missing}")

    for c in ["player_id", "game_id", "line", "p_over", "p_over_mkt"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["game_date"] = df["game_date"].astype(str)
    df = df.dropna(subset=["player_id", "game_id", "line", "p_over", "p_over_mkt", "game_date"]).copy()
    if df.empty:
        raise SystemExit("no valid rows in market csv")

    target_date = str(args.game_date).strip() or str(df["game_date"].max())
    df = df[df["game_date"] == target_date].copy()
    if df.empty:
        raise SystemExit(f"no rows found for game_date={target_date}")

    sides = _build_side_rows(df)
    sides = sides[
        sides["model_side_prob"].between(0.0, 1.0, inclusive="neither")
        & sides["market_side_prob"].between(0.0, 1.0, inclusive="neither")
    ].copy()

    # Attach policy thresholds.
    sides["policy_min_ev"] = sides["segment"].map(lambda s: policy[s].min_ev if s in policy else pd.NA)
    sides["policy_min_gap"] = sides["segment"].map(lambda s: policy[s].min_gap if s in policy else pd.NA)
    sides["policy_train_wilson_lb"] = sides["segment"].map(
        lambda s: (policy[s].train_wilson_lb if s in policy else None)
    )
    sides = sides.dropna(subset=["policy_min_ev", "policy_min_gap"]).copy()
    sides["policy_min_ev"] = pd.to_numeric(sides["policy_min_ev"], errors="coerce")
    sides["policy_min_gap"] = pd.to_numeric(sides["policy_min_gap"], errors="coerce")

    if float(args.min_train_wilson_lb) > 0.0:
        sides = sides[
            pd.to_numeric(sides["policy_train_wilson_lb"], errors="coerce").fillna(0.0) >= float(args.min_train_wilson_lb)
        ].copy()

    sides["effective_min_ev"] = sides["policy_min_ev"].clip(lower=float(args.min_ev_floor))
    sides["effective_min_gap"] = np.where(
        pd.to_numeric(sides["market_side_prob"], errors="coerce") >= 0.5,
        np.maximum(sides["policy_min_gap"], float(args.min_gap_floor_favorite)),
        np.maximum(sides["policy_min_gap"], float(args.min_gap_floor_dog)),
    )

    # Policy selection.
    selected = sides[
        (sides["ev_side"] >= sides["effective_min_ev"])
        & (sides["edge_side"] >= sides["effective_min_gap"])
    ].copy()

    dropped_fair_odds = 0
    if not selected.empty:
        selected["model_side_fair_american"] = selected["model_side_prob"].map(prob_to_fair_american)
        before = len(selected)
        selected = selected.dropna(subset=["model_side_fair_american"]).copy()
        selected["model_side_fair_american"] = selected["model_side_fair_american"].astype(int)
        selected = selected[
            (selected["model_side_fair_american"] > 0)
            | (selected["model_side_fair_american"] >= int(args.max_fair_favorite))
        ].copy()
        dropped_fair_odds = before - len(selected)

    # Rank strongest first for any capping.
    selected = selected.sort_values(
        ["ev_side", "edge_side", "model_side_prob"],
        ascending=[False, False, False],
    ).copy()

    # One pick per player.
    if int(args.max_per_player) > 0:
        keep_idx: list[int] = []
        for _, sub in selected.groupby(["game_date", "player_id"], dropna=False):
            keep_idx.extend(sub.head(int(args.max_per_player)).index.tolist())
        selected = selected.loc[keep_idx].copy()

    # Optional caps.
    if int(args.max_per_game) > 0:
        keep_idx = []
        for _, sub in selected.groupby(["game_date", "game_id"], dropna=False):
            keep_idx.extend(sub.head(int(args.max_per_game)).index.tolist())
        selected = selected.loc[keep_idx].copy()

    if int(args.max_per_slate) > 0:
        selected = selected.head(int(args.max_per_slate)).copy()

    selected = selected.sort_values(["game_id", "ev_side", "edge_side"], ascending=[True, False, False]).reset_index(drop=True)

    out_cols = [
        "game_date",
        "game_id",
        "player_id",
        "full_name",
        "line",
        "segment",
        "model_pick",
        "model_side_prob",
        "market_side_prob",
        "edge_side",
        "ev_side",
        "policy_min_ev",
        "policy_min_gap",
        "effective_min_ev",
        "effective_min_gap",
        "policy_train_wilson_lb",
        "market_side",
        "price_over",
        "p_over",
        "p_over_mkt",
        "model_side_fair_american",
    ]
    out_cols = [c for c in out_cols if c in selected.columns]

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    selected[out_cols].to_csv(out_csv, index=False)

    summary: dict[str, Any] = {
        "config": {
            "market_csv": str(market_csv),
            "policy_json": str(policy_json),
            "game_date": target_date,
            "min_train_wilson_lb": float(args.min_train_wilson_lb),
            "min_ev_floor": float(args.min_ev_floor),
            "min_gap_floor_favorite": float(args.min_gap_floor_favorite),
            "min_gap_floor_dog": float(args.min_gap_floor_dog),
            "max_per_player": int(args.max_per_player),
            "max_per_game": int(args.max_per_game),
            "max_per_slate": int(args.max_per_slate),
            "max_fair_favorite": int(args.max_fair_favorite),
            "emit_book_upload": bool(args.emit_book_upload),
        },
        "policy_segments_loaded": sorted(policy.keys()),
        "source_rows_for_date": int(len(df)),
        "source_side_rows_for_date": int(len(sides)),
        "dropped_by_fair_odds_cap": int(dropped_fair_odds),
        "selected": _summarize(selected),
        "outputs": {"csv": str(out_csv), "json": str(Path(args.out_json))},
    }

    out_json = Path(args.out_json)
    _write_summary_json(out_json, summary)

    if args.emit_book_upload:
        exclude_ids = [int(x) for x in (args.book_upload_exclude_player_id or [])]
        book_out = Path(args.book_upload_out_csv)
        book_out.parent.mkdir(parents=True, exist_ok=True)
        try:
            emitted = _emit_book_upload(
                candidates_csv=out_csv,
                out_csv=book_out,
                max_fair_favorite=int(args.book_upload_max_fair_favorite),
                availability_csv=str(args.book_upload_availability_csv),
                skip_availability_filter=bool(args.book_upload_skip_availability_filter),
                exclude_player_ids=exclude_ids,
            )
            summary["outputs"]["book_upload"] = emitted
        except SystemExit as e:
            summary["outputs"]["book_upload_error"] = str(e)
            _write_summary_json(out_json, summary)
            raise

    _write_summary_json(out_json, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
