"""Passive market/pricing audit context helpers for MLB artifacts."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


MARKET_AUDIT_CONTEXT_COLUMNS = [
    "market_price_over",
    "market_price_under",
    "market_no_vig_implied_over",
    "market_no_vig_implied_under",
    "market_hold",
    "market_book_count_two_sided",
    "market_bookmaker_key",
    "market_odds_snapshot_file",
    "market_snapshot_time_utc",
    "market_snapshot_run_tag",
    "selected_side_price",
    "selected_side_no_vig_implied",
    "model_vs_market_gap",
]


_SOURCE_ALIASES = {
    "market_price_over": ("market_price_over", "price_over_american", "odds_over"),
    "market_price_under": ("market_price_under", "price_under_american", "odds_under"),
    "market_no_vig_implied_over": ("market_no_vig_implied_over", "implied_over_novig"),
    "market_no_vig_implied_under": ("market_no_vig_implied_under", "implied_under_novig"),
    "market_hold": ("market_hold",),
    "market_book_count_two_sided": ("market_book_count_two_sided", "book_count_two_sided", "books_two_sided"),
    "market_bookmaker_key": ("market_bookmaker_key", "bookmaker_key"),
    "market_odds_snapshot_file": ("market_odds_snapshot_file", "odds_snapshot_file"),
    "market_snapshot_time_utc": ("market_snapshot_time_utc", "snapshot_time_utc"),
    "market_snapshot_run_tag": ("market_snapshot_run_tag", "snapshot_run_tag"),
}


def _blank_series(index: pd.Index) -> pd.Series:
    return pd.Series(pd.NA, index=index)


def _first_available(df: pd.DataFrame, names: tuple[str, ...]) -> pd.Series:
    out = _blank_series(df.index)
    for name in names:
        if name not in df.columns:
            continue
        values = df[name]
        out = out.where(out.notna(), values)
    return out


def _clean_side(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in {"over", "under"} else ""


def add_market_audit_context(
    df: pd.DataFrame,
    *,
    side_col: str | None = None,
    probability_col: str | None = None,
) -> pd.DataFrame:
    """Add passive market audit columns without changing selection/scoring fields."""
    out = df.copy()
    for target, aliases in _SOURCE_ALIASES.items():
        values = _first_available(out, aliases)
        if target in out.columns:
            out[target] = out[target].where(out[target].notna(), values)
        else:
            out[target] = values

    numeric_cols = [
        "market_price_over",
        "market_price_under",
        "market_no_vig_implied_over",
        "market_no_vig_implied_under",
        "market_hold",
        "market_book_count_two_sided",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "selected_side_price" not in out.columns:
        out["selected_side_price"] = pd.NA
    if "selected_side_no_vig_implied" not in out.columns:
        out["selected_side_no_vig_implied"] = pd.NA
    if "model_vs_market_gap" not in out.columns:
        out["model_vs_market_gap"] = pd.NA

    if side_col and side_col in out.columns:
        side = out[side_col].map(_clean_side)
        over = side.eq("over")
        under = side.eq("under")
        out["selected_side_price"] = np.where(
            over,
            out["market_price_over"],
            np.where(under, out["market_price_under"], pd.NA),
        )
        out["selected_side_no_vig_implied"] = np.where(
            over,
            out["market_no_vig_implied_over"],
            np.where(under, out["market_no_vig_implied_under"], pd.NA),
        )
        out["selected_side_price"] = pd.to_numeric(out["selected_side_price"], errors="coerce")
        out["selected_side_no_vig_implied"] = pd.to_numeric(
            out["selected_side_no_vig_implied"],
            errors="coerce",
        )

    if probability_col and probability_col in out.columns:
        prob = pd.to_numeric(out[probability_col], errors="coerce")
        market_prob = pd.to_numeric(out["selected_side_no_vig_implied"], errors="coerce")
        out["model_vs_market_gap"] = prob - market_prob
    else:
        out["model_vs_market_gap"] = pd.to_numeric(out["model_vs_market_gap"], errors="coerce")

    return out
