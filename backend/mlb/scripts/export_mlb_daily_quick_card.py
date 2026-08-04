#!/usr/bin/env python3
"""Export a daily quick card from strong historical bucket signals."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_SLATE_CSV = Path("backend/mlb/data/processed/mlb_slate_output.csv")
DEFAULT_BOOK_UPLOAD_CSV = Path("backend/mlb/data/processed/mlb_book_upload.csv")
DEFAULT_ACTION_REPORT_CSV = Path("backend/mlb/exports/model_performance/bucket_action_report.csv")
DEFAULT_BUCKET_SUMMARY_CSV = Path("backend/mlb/exports/model_performance/bucket_performance_summary.csv")
DEFAULT_ODDS_HISTORY_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_OUT_ROOT = Path("backend/mlb/exports/quick_card")

OUTPUT_COLUMNS = [
    "date",
    "player_name",
    "prop_type",
    "side",
    "line",
    "price",
    "model_fair_price",
    "price_edge",
    "price_edge_class",
    "model_prob",
    "matched_bucket_count",
    "matched_buckets",
    "tier_before_caps",
    "tier",
    "tier_cap_reason",
]


def _norm_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _norm_line(value: Any) -> float:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return np.nan
    return float(number)


def _bucket_price(price: Any) -> str:
    p = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(p):
        return "unknown"
    if p <= -200:
        return "<= -200"
    if p <= -150:
        return "-200 to -150"
    if p <= -110:
        return "-150 to -110"
    if p <= 100:
        return "-110 to +100"
    if p <= 150:
        return "+100 to +150"
    return "+150+"


def _bucket_prob(prob: Any) -> str:
    p = pd.to_numeric(pd.Series([prob]), errors="coerce").iloc[0]
    if pd.isna(p):
        return "unknown"
    if p < 0.50:
        return "<0.50"
    if p < 0.55:
        return "0.50-0.55"
    if p < 0.60:
        return "0.55-0.60"
    if p < 0.65:
        return "0.60-0.65"
    return "0.65+"


def _bucket_edge(edge: Any) -> str:
    e = pd.to_numeric(pd.Series([edge]), errors="coerce").iloc[0]
    if pd.isna(e):
        return "unknown"
    if e < 0:
        return "<0"
    if e < 0.02:
        return "0-0.02"
    if e < 0.05:
        return "0.02-0.05"
    if e < 0.10:
        return "0.05-0.10"
    return "0.10+"


def _bucket_line(line: Any) -> str:
    n = pd.to_numeric(pd.Series([line]), errors="coerce").iloc[0]
    if pd.isna(n):
        return "unknown"
    if float(n).is_integer():
        return f"line={int(n)}"
    return f"line={float(n):g}"


def _american_to_implied(price: Any) -> float:
    p = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(p) or p == 0:
        return np.nan
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def _prob_to_american(prob: Any) -> float:
    p = pd.to_numeric(pd.Series([prob]), errors="coerce").iloc[0]
    if pd.isna(p) or p <= 0 or p >= 1:
        return np.nan
    if p >= 0.5:
        return -100.0 * p / (1.0 - p)
    return 100.0 * (1.0 - p) / p


def _price_edge_class(edge: Any) -> str:
    e = pd.to_numeric(pd.Series([edge]), errors="coerce").iloc[0]
    if pd.isna(e):
        return "unknown_edge"
    # Treat a few cents of American-odds rounding as neutral.
    if e > 5:
        return "positive_edge"
    if e < -5:
        return "negative_edge"
    return "neutral_edge"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="Slate date YYYY-MM-DD. Defaults to the slate file date.")
    parser.add_argument("--slate-csv", type=Path, default=DEFAULT_SLATE_CSV)
    parser.add_argument("--book-upload-csv", type=Path, default=DEFAULT_BOOK_UPLOAD_CSV)
    parser.add_argument(
        "--odds-snapshot-json",
        type=Path,
        help="Optional OddsAPI snapshot. Defaults to backend/mlb/exports/odds_history/<date>/odds_latest_compatible.json",
    )
    parser.add_argument("--bookmaker-key", default="betonlineag", help="Preferred bookmaker for bet prices. Blank uses best price.")
    parser.add_argument("--action-report-csv", type=Path, default=DEFAULT_ACTION_REPORT_CSV)
    parser.add_argument("--bucket-summary-csv", type=Path, default=DEFAULT_BUCKET_SUMMARY_CSV)
    parser.add_argument("--out-csv", type=Path, help="Defaults to backend/mlb/exports/quick_card/<date>/quick_card.csv")
    return parser.parse_args()


def _load_strong_buckets(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"missing bucket action report: {path}")
    actions = pd.read_csv(path)
    signal_col = "signal" if "signal" in actions.columns else "signal_type"
    required = {"prop_type", "bucket_dimension", "bucket_value", signal_col, "suggested_action"}
    missing = required - set(actions.columns)
    if missing:
        raise SystemExit(f"{path} missing required columns: {sorted(missing)}")

    mask = (
        actions[signal_col].astype(str).str.lower().eq("strong")
        & actions["suggested_action"].astype(str).str.lower().eq("keep_normal")
    )
    if "sample_size_flag" in actions.columns:
        mask &= actions["sample_size_flag"].astype(str).str.lower().isin({"usable", "strong_sample"})
    strong = actions.loc[mask].copy()
    strong["prop_type"] = strong["prop_type"].map(_norm_text)
    strong["bucket_dimension"] = strong["bucket_dimension"].map(_norm_text)
    strong["bucket_value"] = strong["bucket_value"].astype(str).str.strip()
    return strong


def _derive_date(slate: pd.DataFrame, requested: str | None) -> str:
    if requested:
        return str(pd.to_datetime(requested).date())
    for column in ("slate_date", "game_date", "date"):
        if column in slate.columns:
            dates = pd.to_datetime(slate[column], errors="coerce").dropna()
            if not dates.empty:
                return str(dates.dt.date.mode().iloc[0])
    raise SystemExit("could not derive slate date; pass --date YYYY-MM-DD")


def _selected_fair_price(slate: pd.DataFrame) -> pd.Series:
    side = slate["side"].astype(str).str.lower()
    over = pd.to_numeric(slate.get("fair_odds_over_american"), errors="coerce")
    under = pd.to_numeric(slate.get("fair_odds_under_american"), errors="coerce")
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=slate.index)


def _load_slate(path: Path, slate_date: str | None) -> tuple[pd.DataFrame, str]:
    if not path.exists():
        raise SystemExit(f"missing slate CSV: {path}")
    slate = pd.read_csv(path)
    required = {"player_name", "prop_type", "line", "model_pick_side", "model_pick_prob"}
    missing = required - set(slate.columns)
    if missing:
        raise SystemExit(f"{path} missing required columns: {sorted(missing)}")

    date_value = _derive_date(slate, slate_date)
    slate = slate.copy()
    if "slate_date" in slate.columns:
        slate = slate[pd.to_datetime(slate["slate_date"], errors="coerce").dt.date.astype(str).eq(date_value)].copy()
    elif "game_date" in slate.columns:
        slate = slate[pd.to_datetime(slate["game_date"], errors="coerce").dt.date.astype(str).eq(date_value)].copy()

    slate["date"] = date_value
    slate["prop_type"] = slate["prop_type"].map(_norm_text)
    slate["side"] = slate["model_pick_side"].map(_norm_text)
    slate["line"] = slate["line"].map(_norm_line)
    slate["model_prob"] = pd.to_numeric(slate["model_pick_prob"], errors="coerce")
    slate["market_key_norm"] = slate.get("market_key", slate["prop_type"]).map(_norm_text)
    slate["player_id_norm"] = pd.to_numeric(slate.get("player_id"), errors="coerce").astype("Int64").astype(str)
    slate["player_name_norm"] = slate["player_name"].map(_norm_text)
    slate["price"] = _selected_fair_price(slate)
    return slate, date_value


def _merge_upload_prices(slate: pd.DataFrame, upload_path: Path, date_value: str) -> pd.DataFrame:
    if not upload_path.exists():
        return slate
    upload = pd.read_csv(upload_path)
    required = {"MARKET", "SELECTOR", "POINT", "SIDE", "WIN %"}
    if not required.issubset(upload.columns):
        return slate

    work = upload.copy()
    if "DATE" in work.columns:
        date_compact = date_value.replace("-", "")
        work = work[work["DATE"].astype(str).str.replace(r"\.0$", "", regex=True).eq(date_compact)].copy()
    if work.empty:
        return slate

    work["market_key_norm"] = work["MARKET"].map(_norm_text)
    work["side"] = work["SIDE"].map(_norm_text)
    work["line"] = work["POINT"].map(_norm_line)
    work["player_id_norm"] = pd.to_numeric(work["SELECTOR"], errors="coerce").astype("Int64").astype(str)
    work["upload_price"] = pd.to_numeric(work["WIN %"], errors="coerce")
    work = work[work["upload_price"].notna()].copy()
    work = work.sort_values("upload_price", ascending=False).drop_duplicates(
        ["player_id_norm", "market_key_norm", "side", "line"], keep="first"
    )

    merged = slate.merge(
        work[["player_id_norm", "market_key_norm", "side", "line", "upload_price"]],
        on=["player_id_norm", "market_key_norm", "side", "line"],
        how="left",
    )
    merged["price"] = merged["upload_price"].fillna(merged["price"])
    return merged.drop(columns=["upload_price"])


def _load_odds_price_rows(path: Path, bookmaker_key: str) -> pd.DataFrame:
    import json

    if not path.exists():
        return pd.DataFrame()
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    events = payload.get("events") if isinstance(payload, dict) else payload
    if not isinstance(events, list):
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    preferred_book = _norm_text(bookmaker_key)
    for event in events:
        if not isinstance(event, dict):
            continue
        for book in event.get("bookmakers") or []:
            book_key = _norm_text(book.get("key"))
            if preferred_book and book_key != preferred_book:
                continue
            for market in book.get("markets") or []:
                market_key = _norm_text(market.get("key"))
                if not market_key:
                    continue
                for outcome in market.get("outcomes") or []:
                    side = _norm_text(outcome.get("name"))
                    if side not in {"over", "under"}:
                        continue
                    player_name = outcome.get("description") or outcome.get("name")
                    line = _norm_line(outcome.get("point"))
                    price = pd.to_numeric(pd.Series([outcome.get("price")]), errors="coerce").iloc[0]
                    if not _norm_text(player_name) or pd.isna(line) or pd.isna(price):
                        continue
                    rows.append(
                        {
                            "player_name_norm": _norm_text(player_name),
                            "market_key_norm": market_key,
                            "side": side,
                            "line": line,
                            "odds_price": float(price),
                            "odds_bookmaker_key": book_key,
                        }
                    )
    return pd.DataFrame(rows)


def _merge_odds_prices(slate: pd.DataFrame, odds_path: Path, bookmaker_key: str) -> pd.DataFrame:
    odds = _load_odds_price_rows(odds_path, bookmaker_key)
    if odds.empty:
        return slate
    odds = odds.sort_values("odds_price", ascending=False).drop_duplicates(
        ["player_name_norm", "market_key_norm", "side", "line"], keep="first"
    )
    merged = slate.merge(
        odds[["player_name_norm", "market_key_norm", "side", "line", "odds_price", "odds_bookmaker_key"]],
        on=["player_name_norm", "market_key_norm", "side", "line"],
        how="left",
    )
    merged["price"] = merged["odds_price"].fillna(merged["price"])
    return merged.drop(columns=["odds_price", "odds_bookmaker_key"])


def _add_buckets(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    rows["model_fair_price"] = rows["model_prob"].map(_prob_to_american)
    rows["price_edge"] = pd.to_numeric(rows["price"], errors="coerce") - pd.to_numeric(
        rows["model_fair_price"], errors="coerce"
    )
    rows["price_edge_class"] = rows["price_edge"].map(_price_edge_class)
    rows["price_bucket"] = rows["price"].map(_bucket_price)
    rows["model_prob_bucket"] = rows["model_prob"].map(_bucket_prob)
    rows["line_bucket"] = rows["line"].map(_bucket_line)
    rows["implied_probability"] = rows["price"].map(_american_to_implied)
    rows["implied_edge"] = rows["model_prob"] - rows["implied_probability"]
    rows["implied_edge_bucket"] = rows["implied_edge"].map(_bucket_edge)
    return rows


def _match_row(row: pd.Series, strong_by_prop: dict[str, pd.DataFrame]) -> tuple[int, str]:
    buckets = strong_by_prop.get(str(row["prop_type"]), pd.DataFrame())
    if buckets.empty:
        return 0, ""

    row_values = {
        "prop_type": "all",
        "model_pick_side": row["side"],
        "price_bucket": row["price_bucket"],
        "model_prob_bucket": row["model_prob_bucket"],
        "line_bucket": row["line_bucket"],
        "implied_edge_bucket": row["implied_edge_bucket"],
    }
    matched: list[str] = []
    display_names = {
        "prop_type": "prop",
        "model_pick_side": "side",
        "price_bucket": "price",
        "model_prob_bucket": "prob",
        "line_bucket": "line",
        "implied_edge_bucket": "edge",
    }
    for _, bucket in buckets.iterrows():
        dimension = str(bucket["bucket_dimension"])
        expected = str(bucket["bucket_value"]).strip()
        actual = str(row_values.get(dimension, "")).strip()
        if actual and actual == expected:
            matched.append(f"{display_names.get(dimension, dimension)}={expected}")
    matched = sorted(set(matched))
    return len(matched), "; ".join(matched)


def _apply_tier_caps(row: pd.Series) -> tuple[str, str]:
    before = str(row["tier_before_caps"])
    price = pd.to_numeric(pd.Series([row["price"]]), errors="coerce").iloc[0]
    prob = pd.to_numeric(pd.Series([row["model_prob"]]), errors="coerce").iloc[0]
    edge_class = str(row.get("price_edge_class", "unknown_edge"))
    matched = int(row["matched_bucket_count"])
    reasons: list[str] = []

    if pd.isna(price):
        reasons.append("missing_price")
    if pd.isna(prob):
        reasons.append("missing_model_prob")
    if edge_class == "unknown_edge":
        reasons.append("unknown_price_edge")

    if pd.notna(price) and price <= -300:
        return "watch_low_value", "price<=-300"
    if pd.notna(price) and price <= -200:
        return "watch", "price<=-200"
    if edge_class == "negative_edge":
        return "watch", "negative_edge"
    if pd.notna(prob) and prob >= 0.75:
        return "watch", "model_prob>=0.75"
    if pd.notna(prob) and prob >= 0.65:
        if pd.notna(price) and float(price) > -200 and edge_class in {"positive_edge", "neutral_edge"} and matched >= 2:
            return "lean", "model_prob>=0.65"

    strong_price_ok = pd.notna(price) and -120 <= float(price) <= 150
    strong_prob_ok = pd.notna(prob) and 0.54 <= float(prob) <= 0.62
    lean_price_ok = pd.notna(price) and float(price) > -200
    lean_prob_ok = pd.notna(prob) and 0.54 <= float(prob) <= 0.65

    if before == "strong" and strong_price_ok and strong_prob_ok and edge_class == "positive_edge" and matched >= 2:
        return "strong", "none"

    if before == "strong":
        if edge_class != "positive_edge":
            reasons.append(f"strong_requires_positive_edge:{edge_class}")
        if not strong_price_ok:
            reasons.append("outside_strong_price_window")
        if not strong_prob_ok:
            reasons.append("outside_strong_probability_window")
        if pd.notna(prob) and prob >= 0.65:
            reasons.append("model_prob>=0.65")

    if lean_price_ok and lean_prob_ok and edge_class in {"positive_edge", "neutral_edge"} and matched >= 2:
        return "lean", "; ".join(reasons) if reasons else "none"

    if edge_class not in {"positive_edge", "neutral_edge"}:
        reasons.append(edge_class)
    if not lean_price_ok:
        reasons.append("outside_lean_price_window")
    if not lean_prob_ok:
        reasons.append("outside_lean_probability_window")
    return "watch", "; ".join(dict.fromkeys(reasons)) if reasons else "tier_cap"


def main() -> None:
    from backend.mlb.shared.model_authority import assert_predictive_model_qualified

    assert_predictive_model_qualified("production_quick_card_generation")
    args = _parse_args()
    strong = _load_strong_buckets(args.action_report_csv)

    if not args.bucket_summary_csv.exists():
        raise SystemExit(f"missing bucket performance summary: {args.bucket_summary_csv}")
    # Loaded deliberately so the export fails early if the paired action inputs are incomplete.
    pd.read_csv(args.bucket_summary_csv, nrows=1)

    slate, date_value = _load_slate(args.slate_csv, args.date)
    slate = _merge_upload_prices(slate, args.book_upload_csv, date_value)
    odds_path = args.odds_snapshot_json or DEFAULT_ODDS_HISTORY_ROOT / date_value / "odds_latest_compatible.json"
    slate = _merge_odds_prices(slate, odds_path, args.bookmaker_key)
    slate = _add_buckets(slate)

    strong_by_prop = {prop: group.copy() for prop, group in strong.groupby("prop_type")}
    matches = slate.apply(lambda row: _match_row(row, strong_by_prop), axis=1)
    slate["matched_bucket_count"] = [item[0] for item in matches]
    slate["matched_buckets"] = [item[1] for item in matches]
    card = slate[slate["matched_bucket_count"] >= 2].copy()
    card["tier_before_caps"] = np.where(card["matched_bucket_count"] >= 4, "strong", "lean")
    capped = card.apply(_apply_tier_caps, axis=1)
    card["tier"] = [item[0] for item in capped]
    card["tier_cap_reason"] = [item[1] for item in capped]

    tier_order = pd.CategoricalDtype(["strong", "lean", "watch", "watch_low_value"], ordered=True)
    card["tier_sort"] = card["tier"].astype(tier_order)
    card = card.sort_values(
        ["tier_sort", "matched_bucket_count", "model_prob", "prop_type", "player_name"],
        ascending=[True, False, False, True, True],
    )
    card["price"] = pd.to_numeric(card["price"], errors="coerce").round(0).astype("Int64")
    card["model_fair_price"] = pd.to_numeric(card["model_fair_price"], errors="coerce").round(0).astype("Int64")
    card["price_edge"] = pd.to_numeric(card["price_edge"], errors="coerce").round(0).astype("Int64")
    card["model_prob"] = pd.to_numeric(card["model_prob"], errors="coerce").round(4)
    card = card[OUTPUT_COLUMNS]

    out_csv = args.out_csv or DEFAULT_OUT_ROOT / date_value / "quick_card.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    card.to_csv(out_csv, index=False, quoting=csv.QUOTE_NONNUMERIC)

    total = int(len(card))
    strong_count = int(card["tier"].eq("strong").sum()) if total else 0
    lean_count = int(card["tier"].eq("lean").sum()) if total else 0
    print(f"wrote {out_csv}")
    print(f"total candidates: {total}")
    print(f"strong count: {strong_count}")
    print(f"lean count: {lean_count}")
    if total:
        print("counts by tier:")
        for tier, count in card["tier"].value_counts().sort_index().items():
            print(f"  {tier}: {int(count)}")
        print("counts by prop_type:")
        for prop_type, count in card["prop_type"].value_counts().sort_index().items():
            print(f"  {prop_type}: {int(count)}")


if __name__ == "__main__":
    main()
