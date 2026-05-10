#!/usr/bin/env python3
"""Export daily top-ranked MLB model selections with simple guardrails."""

from __future__ import annotations

import argparse
import csv
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_SLATE_CSV = Path("backend/mlb/data/processed/mlb_slate_output.csv")
DEFAULT_BOOK_UPLOAD_CSV = Path("backend/mlb/data/processed/mlb_book_upload.csv")
DEFAULT_ODDS_HISTORY_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_OUT_ROOT = Path("backend/mlb/exports/top_rank")
DEFAULT_BACKTEST_OUT_CSV = Path("backend/mlb/exports/model_performance/top_rank_results.csv")

OUTPUT_COLUMNS = [
    "date",
    "player_name",
    "prop_type",
    "side",
    "line",
    "price",
    "model_pick_prob",
    "rank",
]


def _norm_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _norm_name(value: Any) -> str:
    text = _norm_text(value)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _norm_line(value: Any) -> float:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return np.nan
    return float(number)


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _line_key(value: Any) -> str:
    number = _norm_line(value)
    if pd.isna(number):
        return ""
    return f"{number:.3f}".rstrip("0").rstrip(".")


def _market_key(value: Any) -> str:
    text = _norm_text(value).replace(" ", "_")
    aliases = {
        "outs_recorded": "pitcher_outs",
        "pitching_outs": "pitcher_outs",
        "pitcher_outs": "pitcher_outs",
        "strikeouts_pitching": "pitcher_strikeouts",
        "pitcher_strikeouts": "pitcher_strikeouts",
    }
    return aliases.get(text, text)


def _side(value: Any) -> str:
    text = _norm_text(value)
    if text.startswith("o"):
        return "over"
    if text.startswith("u"):
        return "under"
    return text


def _prop_market_key(row: pd.Series) -> str:
    if "market_key" in row and _norm_text(row.get("market_key")):
        return _market_key(row.get("market_key"))
    prop = _norm_text(row.get("prop_type"))
    aliases = {
        "hits": "batter_hits",
        "total_bases": "batter_total_bases",
        "hits_runs_rbis": "batter_hits_runs_rbis",
        "runs_scored": "batter_runs_scored",
        "rbis": "batter_rbis",
        "walks": "batter_walks",
        "doubles": "batter_doubles",
        "strikeouts_batting": "batter_strikeouts",
        "hits_allowed": "pitcher_hits_allowed",
        "earned_runs": "pitcher_earned_runs",
        "walks_allowed": "pitcher_walks",
        "strikeouts_pitching": "pitcher_strikeouts",
        "outs_recorded": "pitcher_outs",
    }
    return aliases.get(prop, prop)


def _price_for_side(slate: pd.DataFrame) -> pd.Series:
    side = slate["side"].astype(str).str.lower()
    over = pd.to_numeric(slate.get("fair_odds_over_american"), errors="coerce")
    under = pd.to_numeric(slate.get("fair_odds_under_american"), errors="coerce")
    return pd.Series(np.where(side.eq("over"), over, np.where(side.eq("under"), under, np.nan)), index=slate.index)


def _derive_date(slate: pd.DataFrame, requested: str | None) -> str:
    if requested:
        return str(pd.to_datetime(requested).date())
    for column in ("slate_date", "game_date", "date"):
        if column in slate.columns:
            dates = pd.to_datetime(slate[column], errors="coerce").dropna()
            if not dates.empty:
                return str(dates.dt.date.mode().iloc[0])
    raise SystemExit("could not derive slate date; pass --date YYYY-MM-DD")


def _load_slate(path: Path, requested_date: str | None) -> tuple[pd.DataFrame, str]:
    if not path.exists():
        raise SystemExit(f"missing slate CSV: {path}")
    slate = pd.read_csv(path, low_memory=False)
    required = {"player_name", "prop_type", "line", "model_pick_side", "model_pick_prob"}
    missing = required - set(slate.columns)
    if missing:
        raise SystemExit(f"{path} missing required columns: {sorted(missing)}")

    date_value = _derive_date(slate, requested_date)
    slate = slate.copy()
    for column in ("slate_date", "game_date", "date"):
        if column in slate.columns:
            mask = pd.to_datetime(slate[column], errors="coerce").dt.date.astype(str).eq(date_value)
            slate = slate.loc[mask].copy()
            break

    slate["date"] = date_value
    slate["prop_type"] = slate["prop_type"].map(_norm_text)
    slate["side"] = slate["model_pick_side"].map(_side)
    slate["line"] = slate["line"].map(_norm_line)
    slate["model_pick_prob"] = pd.to_numeric(slate["model_pick_prob"], errors="coerce")
    slate["market_key_norm"] = slate.apply(_prop_market_key, axis=1)
    slate["player_id_norm"] = pd.to_numeric(slate.get("player_id"), errors="coerce").astype("Int64").astype(str)
    slate["player_name_norm"] = slate["player_name"].map(_norm_name)
    slate["price"] = _price_for_side(slate)
    return slate, date_value


def _merge_upload_prices(slate: pd.DataFrame, upload_path: Path, date_value: str) -> pd.DataFrame:
    if not upload_path.exists():
        return slate
    upload = pd.read_csv(upload_path, low_memory=False)
    required = {"MARKET", "SELECTOR", "POINT", "SIDE", "WIN %"}
    if not required.issubset(upload.columns):
        return slate

    work = upload.copy()
    if "DATE" in work.columns:
        date_compact = date_value.replace("-", "")
        work = work[work["DATE"].astype(str).str.replace(r"\.0$", "", regex=True).eq(date_compact)].copy()
    if work.empty:
        return slate

    work["market_key_norm"] = work["MARKET"].map(_market_key)
    work["side"] = work["SIDE"].map(_side)
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
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
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
                market_key = _market_key(market.get("key"))
                if not market_key:
                    continue
                for outcome in market.get("outcomes") or []:
                    side = _side(outcome.get("name"))
                    if side not in {"over", "under"}:
                        continue
                    player_name = outcome.get("description") or outcome.get("name")
                    line = _norm_line(outcome.get("point"))
                    price = pd.to_numeric(pd.Series([outcome.get("price")]), errors="coerce").iloc[0]
                    if not _norm_name(player_name) or pd.isna(line) or pd.isna(price):
                        continue
                    rows.append(
                        {
                            "player_name_norm": _norm_name(player_name),
                            "market_key_norm": market_key,
                            "side": side,
                            "line": line,
                            "odds_price": float(price),
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
        odds[["player_name_norm", "market_key_norm", "side", "line", "odds_price"]],
        on=["player_name_norm", "market_key_norm", "side", "line"],
        how="left",
    )
    merged["price"] = merged["odds_price"].fillna(merged["price"])
    return merged.drop(columns=["odds_price"])


def _apply_selection(slate: pd.DataFrame, top_n: int) -> pd.DataFrame:
    rows = slate.copy()
    rows["price"] = pd.to_numeric(rows["price"], errors="coerce")
    rows["model_pick_prob"] = pd.to_numeric(rows["model_pick_prob"], errors="coerce")
    rows = rows[rows["price"].notna() & rows["model_pick_prob"].notna()].copy()
    rows = rows[rows["price"] > -200].copy()
    rows = rows[
        rows["side"].eq("under")
        | (rows["side"].eq("over") & rows["model_pick_prob"].le(0.60) & rows["price"].ge(-120))
    ].copy()
    rows = rows.sort_values(["model_pick_prob", "price", "prop_type", "player_name"], ascending=[False, False, True, True])
    rows = rows.head(int(top_n)).copy()
    rows["rank"] = range(1, len(rows) + 1)
    rows["price"] = rows["price"].round(0).astype("Int64")
    rows["model_pick_prob"] = rows["model_pick_prob"].round(6)
    return rows[OUTPUT_COLUMNS]


def _selection_key(row: pd.Series) -> tuple[str, str, str, str, str]:
    return (
        _date_key(row.get("date") or row.get("game_date")),
        _norm_name(row.get("player_name")),
        _market_key(row.get("market_key_norm") or row.get("market_key") or _prop_market_key(row)),
        _side(row.get("side") or row.get("model_pick_side")),
        _line_key(row.get("line")),
    )


def _resolved_sides(reconcile_csv: Path) -> pd.DataFrame:
    if not reconcile_csv.exists():
        raise SystemExit(f"missing reconcile CSV for backtest: {reconcile_csv}")
    rec = pd.read_csv(reconcile_csv, low_memory=False)
    required = {
        "game_date",
        "player_name",
        "market_key",
        "line",
        "actual_over_outcome",
        "actual_under_outcome",
        "pnl_over_1u",
        "pnl_under_1u",
    }
    missing = required - set(rec.columns)
    if missing:
        raise SystemExit(f"{reconcile_csv} missing required backtest columns: {sorted(missing)}")

    rows = []
    for side in ("over", "under"):
        side_df = rec.copy()
        side_df["date"] = side_df["game_date"]
        side_df["side"] = side
        side_df["result"] = side_df["actual_over_outcome" if side == "over" else "actual_under_outcome"].map(_norm_text)
        side_df["reconcile_pnl"] = pd.to_numeric(
            side_df["pnl_over_1u" if side == "over" else "pnl_under_1u"],
            errors="coerce",
        )
        side_df["bet_key"] = side_df.apply(_selection_key, axis=1)
        rows.append(side_df)
    out = pd.concat(rows, ignore_index=True)
    out = out[out["result"].isin({"win", "loss", "push"})].copy()
    return out.sort_values(["bet_key", "bookmaker_key"]).drop_duplicates("bet_key", keep="first")


def _backtest(selection: pd.DataFrame, slate: pd.DataFrame, reconcile_csv: Path) -> dict[str, Any]:
    resolved = _resolved_sides(reconcile_csv)
    key_market = slate[["date", "player_name", "prop_type", "side", "line", "market_key_norm"]].drop_duplicates()
    keyed = selection.merge(key_market, on=["date", "player_name", "prop_type", "side", "line"], how="left")
    keyed["bet_key"] = keyed.apply(_selection_key, axis=1)
    merged = keyed.merge(resolved[["bet_key", "result", "reconcile_pnl"]], on="bet_key", how="left")
    graded = merged[merged["result"].isin({"win", "loss", "push"})].copy()
    bets = int(len(graded))
    wins = int(graded["result"].eq("win").sum())
    losses = int(graded["result"].eq("loss").sum())
    pushes = int(graded["result"].eq("push").sum())
    profit = float(pd.to_numeric(graded["reconcile_pnl"], errors="coerce").sum()) if bets else 0.0
    return {
        "date": str(selection["date"].iloc[0]) if not selection.empty else "",
        "source": "top_rank",
        "top_n": int(len(selection)),
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": wins / bets if bets else math.nan,
        "profit_units": profit,
        "roi": profit / bets if bets else math.nan,
        "unmatched_or_unresolved": int(len(selection) - bets),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="Slate date YYYY-MM-DD. Defaults to slate file date.")
    parser.add_argument("--slate-csv", type=Path, default=DEFAULT_SLATE_CSV)
    parser.add_argument("--book-upload-csv", type=Path, default=DEFAULT_BOOK_UPLOAD_CSV)
    parser.add_argument(
        "--odds-snapshot-json",
        type=Path,
        help="Optional OddsAPI snapshot. Defaults to backend/mlb/exports/odds_history/<date>/odds_latest_compatible.json",
    )
    parser.add_argument("--bookmaker-key", default="betonlineag", help="Preferred bookmaker for bet prices. Blank uses best price.")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--out-csv", type=Path, help="Defaults to backend/mlb/exports/top_rank/<date>/top_rank.csv")
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument(
        "--reconcile-csv",
        type=Path,
        help="Defaults to artifacts/analysis/mlb/execution_vs_model/<date>/reconcile_rows.csv in --backtest mode.",
    )
    parser.add_argument("--backtest-out-csv", type=Path, default=DEFAULT_BACKTEST_OUT_CSV)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    slate, date_value = _load_slate(args.slate_csv, args.date)
    slate = _merge_upload_prices(slate, args.book_upload_csv, date_value)
    odds_path = args.odds_snapshot_json or DEFAULT_ODDS_HISTORY_ROOT / date_value / "odds_latest_compatible.json"
    slate = _merge_odds_prices(slate, odds_path, args.bookmaker_key)

    selection = _apply_selection(slate, args.top_n)
    out_csv = args.out_csv or DEFAULT_OUT_ROOT / date_value / "top_rank.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    selection.to_csv(out_csv, index=False, quoting=csv.QUOTE_NONNUMERIC)

    print(f"wrote {out_csv}")
    print(f"selected={len(selection)} top_n={args.top_n}")
    if len(selection):
        print("counts by prop_type:")
        for prop_type, count in selection["prop_type"].value_counts().sort_index().items():
            print(f"  {prop_type}: {int(count)}")
        print("counts by side:")
        for side, count in selection["side"].value_counts().sort_index().items():
            print(f"  {side}: {int(count)}")

    if args.backtest:
        reconcile_csv = args.reconcile_csv or Path(
            f"artifacts/analysis/mlb/execution_vs_model/{date_value}/reconcile_rows.csv"
        )
        result = _backtest(selection, slate, reconcile_csv)
        out = args.backtest_out_csv
        out.parent.mkdir(parents=True, exist_ok=True)
        row = pd.DataFrame([result])
        if out.exists():
            prev = pd.read_csv(out)
            prev = prev[
                ~(
                    prev.get("date", pd.Series(dtype=str)).astype(str).eq(str(result["date"]))
                    & prev.get("source", pd.Series(dtype=str)).astype(str).eq("top_rank")
                    & pd.to_numeric(prev.get("top_n", pd.Series(dtype=float)), errors="coerce").eq(result["top_n"])
                )
            ].copy()
            row = pd.concat([prev, row], ignore_index=True)
        row.to_csv(out, index=False)
        print(f"backtest_out={out}")
        print(
            "backtest "
            f"bets={result['bets']} wins={result['wins']} losses={result['losses']} "
            f"win_rate={result['win_rate']:.2%} roi={result['roi']:.2%}"
        )


if __name__ == "__main__":
    main()
