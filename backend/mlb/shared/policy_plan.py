from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


_REQUIRED_PLAN_COLS = ("prop_type", "bookmaker_key", "side")


def _clean_text(value: object) -> str:
    text = str(value or "").strip().lower()
    return text


def american_to_implied_probability(price: object) -> Optional[float]:
    if price is None or (isinstance(price, float) and np.isnan(price)):
        return None
    try:
        p = float(price)
    except Exception:
        return None
    if p == 0:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def add_novig_probs_from_prices(
    df: pd.DataFrame,
    *,
    price_over_col: str = "price_over_american",
    price_under_col: str = "price_under_american",
    implied_over_col: str = "implied_over_novig",
    implied_under_col: str = "implied_under_novig",
) -> pd.DataFrame:
    out = df.copy()
    if implied_over_col in out.columns and implied_under_col in out.columns:
        over_ok = out[implied_over_col].notna()
        under_ok = out[implied_under_col].notna()
        if bool((over_ok & under_ok).all()):
            return out

    over_raw = out[price_over_col].map(american_to_implied_probability)
    under_raw = out[price_under_col].map(american_to_implied_probability)
    denom = over_raw + under_raw
    with np.errstate(invalid="ignore", divide="ignore"):
        out[implied_over_col] = np.where(denom > 0, over_raw / denom, np.nan)
        out[implied_under_col] = np.where(denom > 0, under_raw / denom, np.nan)
    return out


def load_policy_plan(path: Path | str, *, include_actions: Iterable[str] = ("enable",)) -> pd.DataFrame:
    plan_path = Path(path).expanduser()
    if not plan_path.exists():
        raise FileNotFoundError(f"policy plan not found: {plan_path}")

    plan = pd.read_csv(plan_path)
    missing = [c for c in _REQUIRED_PLAN_COLS if c not in plan.columns]
    if missing:
        raise ValueError(f"policy plan missing required columns: {missing}")

    out = plan.copy()
    out["prop_type"] = out["prop_type"].map(_clean_text)
    out["bookmaker_key"] = out["bookmaker_key"].map(_clean_text)
    out["side"] = out["side"].map(_clean_text)
    out = out[out["prop_type"].astype(str).str.len() > 0]
    out = out[out["bookmaker_key"].astype(str).str.len() > 0]
    out = out[out["side"].isin(["over", "under"])]

    for col in ("min_gap", "min_ev", "min_model_prob", "min_price", "max_price"):
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")

    action_set = {str(x).strip().lower() for x in include_actions if str(x).strip()}
    if "action" not in out.columns:
        out["action"] = "enable"
    out["action"] = out["action"].map(_clean_text)
    if action_set:
        out = out[out["action"].isin(action_set)]
    out = out[out["action"].astype(str).str.len() > 0]

    # Keep distinct lanes by prop/book/side so policy can evaluate fallback books.
    out = out.drop_duplicates(subset=["prop_type", "bookmaker_key", "side"], keep="first").reset_index(drop=True)
    return out


def score_policy_plan_rows(
    rows: pd.DataFrame,
    plan: pd.DataFrame,
    *,
    prop_col: str = "prop_type",
    bookmaker_col: str = "bookmaker_key",
    model_prob_over_col: str = "model_prob_over",
    model_prob_under_col: str = "model_prob_under",
    price_over_col: str = "price_over_american",
    price_under_col: str = "price_under_american",
    implied_over_col: str = "implied_over_novig",
    implied_under_col: str = "implied_under_novig",
    require_two_sided: bool = True,
) -> pd.DataFrame:
    if rows.empty:
        out = rows.copy()
        out["pass_policy"] = False
        return out

    need_cols = [
        prop_col,
        bookmaker_col,
        model_prob_over_col,
        model_prob_under_col,
        price_over_col,
        price_under_col,
    ]
    missing = [c for c in need_cols if c not in rows.columns]
    if missing:
        raise ValueError(f"rows missing required columns: {missing}")

    r = rows.copy()
    r[prop_col] = r[prop_col].map(_clean_text)
    r[bookmaker_col] = r[bookmaker_col].map(_clean_text)
    r = add_novig_probs_from_prices(
        r,
        price_over_col=price_over_col,
        price_under_col=price_under_col,
        implied_over_col=implied_over_col,
        implied_under_col=implied_under_col,
    )

    p = plan.copy()
    p["prop_type"] = p["prop_type"].map(_clean_text)
    p["bookmaker_key"] = p["bookmaker_key"].map(_clean_text)
    p["side"] = p["side"].map(_clean_text)
    if "action" not in p.columns:
        p["action"] = "enable"
    p["action"] = p["action"].map(_clean_text)

    merged = r.merge(
        p[
            [
                "prop_type",
                "bookmaker_key",
                "side",
                "action",
                "min_gap",
                "min_ev",
                "min_model_prob",
                "min_price",
                "max_price",
            ]
        ].rename(
            columns={
                "side": "plan_side",
                "action": "plan_action",
                "min_gap": "plan_min_gap",
                "min_ev": "plan_min_ev",
                "min_model_prob": "plan_min_model_prob",
                "min_price": "plan_min_price",
                "max_price": "plan_max_price",
            }
        ),
        left_on=[prop_col, bookmaker_col],
        right_on=["prop_type", "bookmaker_key"],
        how="inner",
        suffixes=("", "_plan"),
    )
    if merged.empty:
        merged["pass_policy"] = False
        return merged

    side_is_over = merged["plan_side"].eq("over")
    merged["side_model_prob"] = np.where(
        side_is_over,
        pd.to_numeric(merged[model_prob_over_col], errors="coerce"),
        pd.to_numeric(merged[model_prob_under_col], errors="coerce"),
    )
    merged["side_price_american"] = np.where(
        side_is_over,
        pd.to_numeric(merged[price_over_col], errors="coerce"),
        pd.to_numeric(merged[price_under_col], errors="coerce"),
    )
    merged["side_market_prob"] = np.where(
        side_is_over,
        pd.to_numeric(merged[implied_over_col], errors="coerce"),
        pd.to_numeric(merged[implied_under_col], errors="coerce"),
    )
    merged["gap"] = merged["side_model_prob"] - merged["side_market_prob"]
    with np.errstate(invalid="ignore", divide="ignore"):
        merged["ev"] = (merged["side_model_prob"] / merged["side_market_prob"]) - 1.0

    base_ok = (
        merged["side_model_prob"].notna()
        & merged["side_market_prob"].notna()
        & merged["side_price_american"].notna()
    )
    if require_two_sided:
        base_ok = (
            base_ok
            & pd.to_numeric(merged[price_over_col], errors="coerce").notna()
            & pd.to_numeric(merged[price_under_col], errors="coerce").notna()
        )

    cond_model_prob = merged["plan_min_model_prob"].isna() | (
        merged["side_model_prob"] >= merged["plan_min_model_prob"]
    )
    cond_gap = merged["plan_min_gap"].isna() | (merged["gap"] >= merged["plan_min_gap"])
    cond_ev = merged["plan_min_ev"].isna() | (merged["ev"] >= merged["plan_min_ev"])
    cond_min_price = merged["plan_min_price"].isna() | (
        merged["side_price_american"] >= merged["plan_min_price"]
    )
    cond_max_price = merged["plan_max_price"].isna() | (
        merged["side_price_american"] <= merged["plan_max_price"]
    )

    merged["pass_policy"] = base_ok & cond_model_prob & cond_gap & cond_ev & cond_min_price & cond_max_price
    return merged
