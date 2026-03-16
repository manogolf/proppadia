#!/usr/bin/env python3
"""Explain stage-by-stage probability/odds waterfall for one NHL SOG pick."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_BAKEOFF_ROOT = Path("tmp/analysis/arm_bakeoff")


def _json_default(obj: Any):
    try:
        import numpy as np  # local import keeps dependency optional for static checks

        if isinstance(obj, np.generic):
            return obj.item()
    except Exception:
        pass
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _prob_to_fair_american(p: float) -> int | None:
    try:
        x = float(p)
    except Exception:
        return None
    if not (0.0 < x < 1.0):
        return None
    if x >= 0.5:
        return int(-round(100.0 * x / (1.0 - x)))
    return int(round(100.0 * (1.0 - x) / x))


def _line_tag(line: float) -> str:
    return f"{float(line):.1f}".replace(".", "_")


def _side_prob_from_over(p_over: float, side: str) -> float:
    s = str(side).strip().lower()
    if s == "over":
        return float(p_over)
    if s == "under":
        return 1.0 - float(p_over)
    raise ValueError(f"invalid side: {side}")


def _read_row(
    path: Path,
    *,
    player_id: int,
    game_id: int | None = None,
) -> pd.Series:
    if not path.exists():
        raise FileNotFoundError(f"file not found: {path}")
    df = pd.read_csv(path)
    need = {"player_id"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"{path} missing required columns: {missing}")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    m = df[df["player_id"] == float(player_id)]
    if game_id is not None:
        if "game_id" not in df.columns:
            raise RuntimeError(f"{path} missing game_id column required for disambiguation.")
        df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce")
        m = m[m["game_id"] == float(game_id)]
    if m.empty:
        raise RuntimeError(
            f"no row found in {path} for player_id={player_id}"
            + (f" game_id={game_id}" if game_id is not None else "")
        )
    if len(m) > 1 and game_id is None and "game_id" in m.columns:
        gids = sorted(set(pd.to_numeric(m["game_id"], errors="coerce").dropna().astype(int).tolist()))
        raise RuntimeError(
            f"multiple rows in {path} for player_id={player_id}; pass --game-id. game_ids={gids}"
        )
    return m.iloc[0]


def _read_card_row(
    card_csv: Path,
    *,
    player_id: int,
    line: float,
    side: str,
    game_id: int | None = None,
) -> pd.Series:
    if not card_csv.exists():
        raise FileNotFoundError(f"card csv not found: {card_csv}")
    df = pd.read_csv(card_csv)
    need = {"player_id", "line", "model_pick"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise RuntimeError(f"{card_csv} missing required columns: {missing}")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["model_pick"] = df["model_pick"].astype(str).str.lower().str.strip()
    m = df[
        (df["player_id"] == float(player_id))
        & (df["line"] == float(line))
        & (df["model_pick"] == str(side).lower().strip())
    ]
    if game_id is not None:
        if "game_id" not in df.columns:
            raise RuntimeError(f"{card_csv} missing game_id column required for disambiguation.")
        df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce")
        m = m[m["game_id"] == float(game_id)]
    if m.empty:
        raise RuntimeError(
            f"no card row found in {card_csv} for player_id={player_id} line={line} side={side}"
            + (f" game_id={game_id}" if game_id is not None else "")
        )
    if len(m) > 1 and game_id is None and "game_id" in m.columns:
        gids = sorted(set(pd.to_numeric(m["game_id"], errors="coerce").dropna().astype(int).tolist()))
        raise RuntimeError(
            f"multiple card rows found for player_id={player_id} line={line} side={side}; "
            f"pass --game-id. game_ids={gids}"
        )
    return m.iloc[0]


def _as_float(x: Any) -> float | None:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Explain one SOG pick waterfall from bakeoff artifacts.")
    ap.add_argument("--slate-date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--arm",
        default="defense_blend_calibrated",
        choices=["base", "calibrated", "defense_blend_calibrated"],
    )
    ap.add_argument("--player-id", required=True, type=int)
    ap.add_argument("--line", required=True, type=float)
    ap.add_argument("--side", required=True, choices=["over", "under"])
    ap.add_argument("--game-id", type=int, default=None, help="Optional disambiguation key.")
    ap.add_argument("--bakeoff-root", default=str(DEFAULT_BAKEOFF_ROOT))
    ap.add_argument("--out-json", default="", help="Optional path to write JSON result.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    slate_date = str(args.slate_date).strip()
    bakeoff_dir = Path(args.bakeoff_root) / slate_date
    summary_path = bakeoff_dir / f"nhl_sog_arm_bakeoff_{slate_date}.json"
    if not summary_path.exists():
        raise SystemExit(f"bakeoff summary not found: {summary_path}")
    summary = json.loads(summary_path.read_text())

    arm_info = summary.get("arms", {}).get(args.arm)
    if not isinstance(arm_info, dict):
        raise SystemExit(f"arm not found in bakeoff summary: {args.arm}")
    if not bool(arm_info.get("ok", False)):
        raise SystemExit(f"arm is not ok in bakeoff summary: {args.arm}")

    card_csv = Path(arm_info.get("select", {}).get("card_csv", ""))
    pred_src_csv = Path(arm_info.get("pred_src_csv", ""))
    pred_post_csv = Path(arm_info.get("pred_csv", ""))
    if not card_csv:
        raise SystemExit(f"missing card csv path for arm: {args.arm}")
    if not pred_src_csv:
        raise SystemExit(f"missing pred_src_csv for arm: {args.arm}")
    if not pred_post_csv:
        raise SystemExit(f"missing pred_csv for arm: {args.arm}")

    card_row = _read_card_row(
        card_csv,
        player_id=args.player_id,
        line=args.line,
        side=args.side,
        game_id=args.game_id,
    )
    game_id = args.game_id if args.game_id is not None else int(float(card_row["game_id"]))
    side = str(args.side).strip().lower()
    line_tag = _line_tag(float(args.line))
    p_col = f"p_over_{line_tag}"

    pre_row = _read_row(pred_src_csv, player_id=args.player_id, game_id=game_id)
    post_row = _read_row(pred_post_csv, player_id=args.player_id, game_id=game_id)
    if p_col not in pre_row.index:
        raise SystemExit(f"pre prediction row missing {p_col} in {pred_src_csv}")
    if p_col not in post_row.index:
        raise SystemExit(f"post prediction row missing {p_col} in {pred_post_csv}")

    p_over_pre = float(pre_row[p_col])
    p_over_post = float(post_row[p_col])
    p_side_pre = _side_prob_from_over(p_over_pre, side)
    p_side_post = _side_prob_from_over(p_over_post, side)

    p_side_market = _as_float(card_row.get("market_side_prob"))
    fair_pre = _prob_to_fair_american(p_side_pre)
    fair_post = _prob_to_fair_american(p_side_post)
    fair_market = _prob_to_fair_american(p_side_market) if p_side_market is not None else None

    result: dict[str, Any] = {
        "ok": True,
        "slate_date": slate_date,
        "arm": args.arm,
        "pick": {
            "player_id": int(args.player_id),
            "player_name": str(card_row.get("full_name", "")).strip(),
            "game_id": int(game_id),
            "line": float(args.line),
            "side": side,
        },
        "paths": {
            "summary_json": str(summary_path),
            "card_csv": str(card_csv),
            "pred_src_csv": str(pred_src_csv),
            "pred_post_csv": str(pred_post_csv),
        },
        "waterfall": {
            "p_over_precal": p_over_pre,
            "p_over_postcal": p_over_post,
            "p_side_precal": p_side_pre,
            "p_side_postcal": p_side_post,
            "p_side_market": p_side_market,
            "delta_side_prob_calibration": (p_side_post - p_side_pre),
            "fair_precal_american": fair_pre,
            "fair_postcal_american": fair_post,
            "fair_market_american": fair_market,
        },
        "card_row": {
            "segment": card_row.get("segment"),
            "model_side_prob": _as_float(card_row.get("model_side_prob")),
            "market_side_prob": _as_float(card_row.get("market_side_prob")),
            "edge_side": _as_float(card_row.get("edge_side")),
            "ev_side": _as_float(card_row.get("ev_side")),
            "model_side_fair_american": _as_float(card_row.get("model_side_fair_american")),
            "price_over": _as_float(card_row.get("price_over")),
            "p_over_mkt": _as_float(card_row.get("p_over_mkt")),
            "policy_min_ev": _as_float(card_row.get("policy_min_ev")),
            "policy_min_gap": _as_float(card_row.get("policy_min_gap")),
        },
    }

    if args.arm == "defense_blend_calibrated":
        shadow_csv = Path(summary.get("inputs", {}).get("shadow_pred_csv", ""))
        shadow_prefix = str(summary.get("shadow_conversion", {}).get("prefix", "")).strip()
        if shadow_csv.exists() and shadow_prefix:
            shadow_row = _read_row(shadow_csv, player_id=args.player_id, game_id=game_id)
            p_off_col = f"p_offense_over_{line_tag}"
            p_proj_col = f"{shadow_prefix}_over_{line_tag}"
            p_over_offense = _as_float(shadow_row.get(p_off_col))
            p_over_projected = _as_float(shadow_row.get(p_proj_col))
            if p_over_offense is not None and p_over_projected is not None:
                p_side_offense = _side_prob_from_over(p_over_offense, side)
                p_side_projected = _side_prob_from_over(p_over_projected, side)
                result["waterfall"]["p_over_offense"] = p_over_offense
                result["waterfall"]["p_side_offense"] = p_side_offense
                result["waterfall"]["delta_side_prob_defense_adjustment"] = (
                    p_side_projected - p_side_offense
                )
                result["waterfall"]["delta_side_prob_calibration"] = (
                    p_side_post - p_side_projected
                )
                result["waterfall"]["fair_offense_american"] = _prob_to_fair_american(p_side_offense)
                result["waterfall"]["fair_projected_precal_american"] = _prob_to_fair_american(p_side_projected)

            result["defense_features"] = {
                "shadow_prefix": shadow_prefix,
                "expected_sog_bucket": shadow_row.get("expected_sog_bucket"),
                "lambda_offense": _as_float(shadow_row.get("lambda_offense")),
                "projected_signature_rate_per60": _as_float(shadow_row.get("projected_signature_rate_per60")),
                "faced_projected_rate_last10": _as_float(shadow_row.get("faced_projected_rate_last10")),
                "defense_surprise_ratio": _as_float(shadow_row.get("defense_surprise_ratio")),
                "defense_surprise_applied": shadow_row.get("defense_surprise_applied"),
                "lambda_projected_a0_5": _as_float(shadow_row.get("lambda_projected_a0_5")),
            }

    if args.out_json:
        out_path = Path(args.out_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, default=_json_default))

    print(json.dumps(result, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
