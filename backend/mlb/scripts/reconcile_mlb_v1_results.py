#!/usr/bin/env python3
"""Join MLB V1 candidate wagers to resolved reconcile outcomes.

CSV only. No DB reads/writes and no pipeline side effects.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd


OUT_COLUMNS = ["date", "player_name", "market_key", "side", "line", "price", "result", "pnl"]


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _norm_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", _clean(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace(",", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _num(value: Any) -> float:
    try:
        text = _clean(value).replace(",", "")
        if not text:
            return np.nan
        return float(text)
    except Exception:
        return np.nan


def _line_key(value: Any) -> str:
    v = _num(value)
    if pd.isna(v):
        return ""
    return f"{v:.3f}".rstrip("0").rstrip(".")


def _market_key(value: Any) -> str:
    text = _clean(value).lower().strip().replace(" ", "_")
    aliases = {
        "outs_recorded": "pitcher_outs",
        "pitcher_outs": "pitcher_outs",
        "pitching_outs": "pitcher_outs",
        "strikeouts_pitching": "pitcher_strikeouts",
        "pitcher_strikeouts": "pitcher_strikeouts",
    }
    return aliases.get(text, text)


def _side(value: Any) -> str:
    text = _clean(value).lower().strip()
    return text if text in {"over", "under"} else text


def _first_existing(df: pd.DataFrame, names: Sequence[str]) -> str:
    lookup = {str(c).lower(): str(c) for c in df.columns}
    for name in names:
        found = lookup.get(name.lower())
        if found:
            return found
    return ""


def _price_for_side(row: pd.Series) -> float:
    side = row.get("side_key")
    if side == "over":
        return _num(row.get("price_over_american"))
    if side == "under":
        return _num(row.get("price_under_american"))
    return np.nan


def _result_for_side(row: pd.Series) -> str:
    side = row.get("side_key")
    col = "actual_over_outcome" if side == "over" else "actual_under_outcome" if side == "under" else ""
    result = _clean(row.get(col)).lower() if col else ""
    return result if result in {"win", "loss", "push"} else result


def _pnl_for_side(row: pd.Series) -> float:
    side = row.get("side_key")
    col = "pnl_over_1u" if side == "over" else "pnl_under_1u" if side == "under" else ""
    return _num(row.get(col)) if col else np.nan


def _profit_1u(result: Any, price: Any) -> float:
    outcome = _clean(result).lower()
    if outcome == "push":
        return 0.0
    if outcome == "loss":
        return -1.0
    if outcome != "win":
        return np.nan

    p = _num(price)
    if pd.isna(p) or p == 0:
        return np.nan
    if p > 0:
        return p / 100.0
    return 100.0 / abs(p)


def _prep_candidates(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    required = ["date", "player_name", "market_key", "side", "line"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"{path} missing required candidate columns: {', '.join(missing)}")

    price_col = _first_existing(df, ["price", "current_price"])
    if not price_col:
        raise SystemExit(f"{path} missing price/current_price column")

    out = df.copy()
    out["date_key"] = out["date"].map(_date_key)
    out["player_key"] = out["player_name"].map(_norm_name)
    out["market_key_norm"] = out["market_key"].map(_market_key)
    out["side_key"] = out["side"].map(_side)
    out["line_key"] = out["line"].map(_line_key)
    out["candidate_price"] = pd.to_numeric(out[price_col], errors="coerce")
    return out


def _prep_reconcile(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    required = [
        "game_date",
        "player_name",
        "market_key",
        "line",
        "actual_over_outcome",
        "actual_under_outcome",
        "pnl_over_1u",
        "pnl_under_1u",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"{path} missing required reconcile columns: {', '.join(missing)}")

    out = df.copy()
    out["date_key"] = out["game_date"].map(_date_key)
    out["player_key"] = out["player_name"].map(_norm_name)
    out["market_key_norm"] = out["market_key"].map(_market_key)
    out["line_key"] = out["line"].map(_line_key)

    rows = []
    for side in ("over", "under"):
        side_df = out.copy()
        side_df["side_key"] = side
        side_df["result"] = side_df.apply(_result_for_side, axis=1)
        side_df["pnl"] = side_df.apply(_pnl_for_side, axis=1)
        side_df["reconcile_price"] = side_df.apply(_price_for_side, axis=1)
        rows.append(side_df)

    sides = pd.concat(rows, ignore_index=True)
    sides = sides[sides["result"].isin({"win", "loss", "push"})].copy()
    if sides.empty:
        raise SystemExit(
            f"{path} has no resolved side outcomes. Rebuild or pass an outcome-backed full-slate "
            "reconcile CSV before running V1 results reconciliation."
        )
    sort_cols = ["date_key", "player_key", "market_key_norm", "side_key", "line_key", "bookmaker_key"]
    present_sort = [c for c in sort_cols if c in sides.columns]
    sides = sides.sort_values(present_sort).drop_duplicates(
        ["date_key", "player_key", "market_key_norm", "side_key", "line_key"],
        keep="first",
    )
    return sides


def reconcile(candidates_csv: Path, reconcile_csv: Path) -> pd.DataFrame:
    candidates = _prep_candidates(candidates_csv)
    rec = _prep_reconcile(reconcile_csv)
    merged = candidates.merge(
        rec[
            [
                "date_key",
                "player_key",
                "market_key_norm",
                "side_key",
                "line_key",
                "result",
                "pnl",
            ]
        ],
        how="left",
        on=["date_key", "player_key", "market_key_norm", "side_key", "line_key"],
    )

    out = pd.DataFrame(
        {
            "date": merged["date_key"],
            "player_name": merged["player_name"],
            "market_key": merged["market_key_norm"],
            "side": merged["side_key"],
            "line": pd.to_numeric(merged["line"], errors="coerce"),
            "price": merged["candidate_price"],
            "result": merged["result"].fillna("unmatched"),
        }
    )
    out["pnl"] = out.apply(lambda row: _profit_1u(row["result"], row["price"]), axis=1)
    return out[OUT_COLUMNS]


def _summary(df: pd.DataFrame) -> dict[str, Any]:
    resolved = df[df["result"].isin({"win", "loss", "push"})].copy()
    wins = int((resolved["result"] == "win").sum())
    losses = int((resolved["result"] == "loss").sum())
    profit = float(pd.to_numeric(resolved["pnl"], errors="coerce").fillna(0.0).sum())
    bets = int(len(resolved))
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "profit": profit,
        "roi": (profit / bets) if bets else np.nan,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Join MLB V1 candidates to actual reconcile outcomes.")
    ap.add_argument("--candidates-csv", required=True)
    ap.add_argument("--reconcile-csv", required=True)
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args(list(argv) if argv is not None else None)

    out = reconcile(Path(args.candidates_csv), Path(args.reconcile_csv))
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    summary = _summary(out)
    print(
        "[mlb-v1-results] "
        f"bets={summary['bets']} wins={summary['wins']} losses={summary['losses']} "
        f"profit={summary['profit']:.3f} roi={summary['roi']:.3f} out_csv={out_path}"
    )
    unmatched = int((out["result"] == "unmatched").sum())
    if unmatched:
        print(f"[mlb-v1-results] unmatched_candidates={unmatched}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
